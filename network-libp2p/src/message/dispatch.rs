use std::{
    collections::HashMap,
    pin::Pin,
    task::Waker,
};

use futures::{
    channel::mpsc,
    io::{AsyncRead, AsyncWrite},
    stream::{Stream, StreamExt},
    sink::SinkExt,
    task::{Context, Poll},
};
use tokio_util::codec::Framed;
use bytes::{BytesMut, buf::BufExt};
use async_stream::stream;

use beserial::Deserialize;

use crate::codecs::{
    typed::{MessageCodec, Error, Message, MessageType},
    tokio_adapter::TokioAdapter,
};

/// Message dispatcher for a single socket.
/// 
/// This sends messages to the peer and receives messages from the peer.
/// 
/// Messages are received by calling `poll_incoming`, which reads messages from the underlying socket and
/// pushes them into a registered stream. Exactly one stream can be registered per message type. Receiver
/// streams can be registered by calling `receive`.
/// 
/// If no stream is registered for a message type, and a message of that type is received, it will be
/// buffered, and once a stream is registered it will read the buffered messages first (in order as they were
/// received).
/// 
/// # TODO
/// 
///  - Something requires the underlying stream `C` to be be pinned, but I'm not sure what. I think we can
///    just pin it to the heap - it'll be fine...
/// 
pub struct MessageDispatch<C>
where
    C: AsyncRead + AsyncWrite + Send + Sync,
{
    framed: Pin<Box<Framed<TokioAdapter<C>, MessageCodec>>>,

    /// Channels that receive raw messages for a specific message type.
    channels: HashMap<MessageType, mpsc::Sender<BytesMut>>,

    /// If we receive messages for which no receiver is registered, we buffer them here.
    buffers: HashMap<MessageType, Vec<BytesMut>>,

    /// Number of buffered messages. This is used to bound the amount of messages we buffer.
    num_buffered: usize,

    /// Maximum number of buffered messages.
    max_buffered: usize,

    /// Waker to wake-up the task once there is space in the buffer again.
    waker: Option<Waker>,
}

impl<C> MessageDispatch<C>
where
    C: AsyncRead + AsyncWrite + Send + Sync + 'static,
{
    ///
    /// # Arguments
    /// 
    ///  - `socket`: The underlying socket
    ///  - `max_buffered`: Maximum number of buffered messages. Must be at least 1.
    /// 
    pub fn new(socket: C, max_buffered: usize) -> Self {
        // This is to ensure that we can just start processing the message and buffer it the channel
        // is full.
        assert!(max_buffered > 0, "Needs at least a buffer size of 1");

        Self {
            framed: Box::pin(Framed::new(socket.into(), MessageCodec::default())),
            channels: HashMap::new(),
            buffers: HashMap::new(),
            num_buffered: 0,
            max_buffered,
            waker: None,
        }
    }

    /// Buffers the message.
    /// 
    /// # Panics
    /// 
    /// Panics if the buffer is full (i.e. `num_buffered == max_buffered`). This is called by `poll_inbound`, which makes
    /// sure that there is space in the buffer.
    /// 
    fn put_buffer(&mut self, type_id: MessageType, data: BytesMut) {
        assert!(self.num_buffered < self.max_buffered, "Buffer is full");
        
        self.buffers.entry(type_id)
            .or_default()
            .push(data);
    }

    /// Process the message, by either sending it (sucessfully) to the receiver, or buffering it.
    /// This also handles disconnected channels, by removing them.
    fn process_message(&mut self, type_id: MessageType, data: BytesMut, cx: &mut Context<'_>) {
        if let Some(tx) = self.channels.get(&type_id) {
            if let Err(e) = tx.try_send(data) {
                if e.is_full() {
                    self.put_buffer(type_id, e.into_inner());
                }
                else if e.is_disconnected() {
                    // The receiver disconnected, so we remove it and buffer the message for the next
                    // receiver.
                    self.channels.remove(&type_id);
                    self.put_buffer(type_id, e.into_inner());
                }
                else {
                    // Sending can only fail if channel is full or disconnected.
                    unreachable!();
                }
            }
        }
    }

    /// Polls the inbound socket and either pushes the message to the registered channel, or buffers it.
    pub fn poll_inbound(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<(), Error>>> {
        // Make sure we have space for buffering.
        if self.num_buffered == self.max_buffered {
            self.waker = Some(cx.waker().clone());
            return Poll::Pending;
        }

        // Poll the incoming stream and handle the message
        match self.framed.poll_next_unpin(cx) {
            // A message was received. The stream gives us tuples of message type and data (BytesMut)
            Poll::Ready(Some(Ok((type_id, data)))) => {
                self.process_message(type_id, data, cx);

                Poll::Ready(Some(Ok(())))
            }

            // Error while receiving a message. This could be an error from the underlying socket (i.e. an
            // IO error), or the message was malformed.
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),

            // End of stream. This will be propagated as error, so 
            Poll::Ready(None) => Poll::Ready(None),

            Poll::Pending => Poll::Pending,
        }
    }

    /// Send a message to the peer
    /// 
    /// # Type Arguments
    /// 
    ///  - `M`: The type of the message.
    /// 
    /// # Arguments
    /// 
    ///  - `message`: Sends the message (with header) to the other peer.
    /// 
    /// # Returns
    /// 
    /// Returns an error, if the message can't be written to the underlying socket.
    /// 
    pub async fn send<M: Message>(&mut self, message: &M) -> Result<(), Error> {
        self.framed.send(message).await?;
        Ok(())
    }

    /// Registers a message receiver for a specific message type (as defined by the implementation `M`).
    /// 
    /// # Panics
    /// 
    /// Panics if a message receiver for this message type is already registered.
    /// 
    /// # TODO
    /// 
    /// Why does `M` need to be `Unpin`?
    /// 
    pub fn receive<M: Message>(&mut self) -> impl Stream<Item = M> {
        let type_id = M::TYPE_ID.into();

        if self.channels.contains_key(&type_id) {
            panic!("Receiver for channel already registered: type_id = {}", type_id);
        }

        // We don't really need buffering here, since we already have that in the `MessageDispatch`.
        let (tx, mut rx) = mpsc::channel(0);

        // Get all buffered messages.
        let buffered = self.buffers.remove(&type_id).unwrap_or_default();

        // Wake up the task, if it was waiting for space in the buffer
        self.wake();

        // Insert sender into channels
        self.channels.insert(M::TYPE_ID.into(), tx);

        stream! {
            // First yield all buffered messages
            for data in buffered {
                match Deserialize::deserialize(&mut data.reader()) {
                    Ok(message) => yield message,
                    Err(e) => log::error!("Error while deserializing message for receiver: {}", e),
                }
            }

            // Then yield the messages that we receive from the dispatch.
            while let Some(data) = rx.next().await {
                match Deserialize::deserialize(&mut data.reader()) {
                    Ok(message) => yield message,
                    Err(e) => log::error!("Error while deserializing message for receiver: {}", e),
                }
            }
        }
    }

    fn wake(&mut self) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}
