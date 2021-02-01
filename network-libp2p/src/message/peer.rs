use std::{
    hash::{Hash, Hasher},
    pin::Pin,
    task::{Waker, Context, Poll},
    sync::Arc,
};

use async_trait::async_trait;
use futures::{channel::oneshot, Stream, StreamExt};
use libp2p::{swarm::NegotiatedSubstream, PeerId};
use parking_lot::Mutex;
use futures::lock::{Mutex as AsyncMutex};
use async_stream::stream;

use nimiq_network_interface::message::Message;
use nimiq_network_interface::peer::{CloseReason, Peer as PeerInterface, RequestResponse, SendError};

use super::dispatch::MessageDispatch;
use crate::{
    network::NetworkError,
    codecs::typed::Error,
};

pub struct Peer {
    pub id: PeerId,

    pub(crate) dispatch: Arc<AsyncMutex<MessageDispatch<NegotiatedSubstream>>>,

    /// Channel used to pass the close reason the the network handler.
    close_tx: Mutex<Option<oneshot::Sender<CloseReason>>>,

    /// Waker used to wake up the task, that's waiting for the dispatch mutex to `poll_inbound`.
    /// 
    /// Note: Is this a good approach?
    /// 
    waker_for_dispatch_lock: Mutex<Option<Waker>>,
}

impl Peer {
    pub fn new(id: PeerId, dispatch: MessageDispatch<NegotiatedSubstream>, close_tx: oneshot::Sender<CloseReason>) -> Self {
        Self {
            id,
            dispatch: Arc::new(AsyncMutex::new(dispatch)),
            close_tx: Mutex::new(Some(close_tx)),
            waker_for_dispatch_lock: Mutex::new(None),
        }
    }

    /// Polls the underlying dispatch's inbound stream by first trying to acquire the mutex. If it's not availble,
    /// this will return `Poll::Pending` and make sure that the task is woken up, once the mutex was released.
    pub(crate) fn poll_inbound(&self, cx: &mut Context<'_>) -> Poll<Option<Result<(), Error>>> {
        if let Some(dispatch) = self.dispatch.try_lock() {
            dispatch.poll_inbound(cx)
        }
        else {
            // Lock is currently held by someone else, so we need to come back later.
            *self.waker_for_dispatch_lock.lock() = Some(cx.waker().clone());
            Poll::Pending
        }
    }

    fn wake(&self) {
        if let Some(waker) = self.waker_for_dispatch_lock.lock().take() {
            waker.wake();
        }
    }
}

impl std::fmt::Debug for Peer {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut debug = f.debug_struct("Peer");

        debug.field("peer_id", &self.id());

        if self.close_tx.lock().is_none() {
            debug.field("closed", &true);
        }

        debug.finish()
    }
}

impl PartialEq for Peer {
    fn eq(&self, other: &Peer) -> bool {
        self.id == other.id
    }
}

impl Eq for Peer {}

impl Hash for Peer {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}

#[async_trait]
impl PeerInterface for Peer {
    type Id = PeerId;
    type Error = NetworkError;

    fn id(&self) -> Self::Id {
        self.id.clone()
    }

    async fn send<M: Message>(&self, message: &M) -> Result<(), SendError> {
        {
            // Acquire the mutex and send the message.
            let dispatch = self.dispatch.lock();
            dispatch.await.send(message).await?;
        }

        // We need to wake the polling task up, if it's waiting for the mutex.
        self.wake();

        Ok(())
    }

    // TODO: Make this a stream of Result<M, Error>
    //
    // NOTE: Move new message codec to network-interface
    //
    fn receive<M: Message>(&self) -> Pin<Box<dyn Stream<Item = M> + Send>> {
        // The lock needs to be an async lock for the send half. So we will register the
        // the receiver when we poll the first message from the stream.

        let dispatch = Arc::clone(&self.dispatch);

        let stream = stream! {
            let stream = dispatch.lock().await.receive();
            while let Some(message) = stream.next().await {
                yield message;
            }
        };

        stream.boxed()
    }

    fn close(&self, reason: CloseReason) {
        // TODO: I think we must poll_close on the underlying socket

        log::debug!("Peer::close: reason={:?}", reason);

        let close_tx_opt = self.close_tx.lock().take();

        if let Some(close_tx) = close_tx_opt {
            if close_tx.send(reason).is_err() {
                log::error!("The receiver for Peer::close was already dropped.");
            }
        } else {
            log::error!("Peer is already closed");
        }
    }

    async fn request<R: RequestResponse>(&self, _request: &<R as RequestResponse>::Request) -> Result<R::Response, Self::Error> {
        unimplemented!()
    }

    fn requests<R: RequestResponse>(&self) -> Box<dyn Stream<Item = R::Request>> {
        unimplemented!()
    }
}
