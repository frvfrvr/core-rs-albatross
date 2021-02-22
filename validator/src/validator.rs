use std::sync::Arc;
use std::time::Duration;
use std::pin::Pin;

use futures::{
    task::{Context, Poll, Waker, noop_waker_ref},
    future::FutureExt,
    stream::{StreamExt, Stream, BoxStream},
    ready,
};
use tokio::{sync::{broadcast, mpsc}, task::JoinHandle};
use pin_project::pin_project;

use block_albatross::{Block, BlockType, ViewChange, ViewChangeProof, MicroBlock, MacroBlock, MultiSignature, MacroHeader, SignedTendermintProposal};
use blockchain_albatross::{BlockchainEvent, ForkEvent, PushResult, AbstractBlockchain};
use bls::CompressedPublicKey;
use consensus_albatross::{
    sync::block_queue::BlockTopic, Consensus, ConsensusEvent, ConsensusProxy,
};
use database::{Database, Environment, ReadTransaction, WriteTransaction};
use hash::Blake2bHash;
use network_interface::network::{Network, Topic};
use nimiq_block_production_albatross::BlockProducer;
use nimiq_tendermint::TendermintReturn;
use nimiq_validator_network::ValidatorNetwork;

use crate::micro::{ProduceMicroBlock, ProduceMicroBlockEvent};
use crate::r#macro::{PersistedMacroState, ProduceMacroBlock};
use crate::slash::ForkProofPool;
use crate::error::Error;
use crate::tendermint::TendermintState;


/// Item produced by `ProduceBlocks`. This is either a block, a macro state update or view change.
#[derive(Debug)]
enum ProduceBlocksItem {
    MacroBlock(MacroBlock),
    MicroBlock(MicroBlock),
    MacroStateUpdate(TendermintState<MacroHeader, MultiSignature>),
    ViewChange(ViewChange, ViewChangeProof),
}

/// State of `ProduceBlocks`. Stores whether we're currently inactive, producing a macro block or
/// micro block.
enum ProduceBlocksState<TValidatorNetwork> {
    Inactive,
    Macro(ProduceMacroBlock),
    Micro(ProduceMicroBlock<TValidatorNetwork>),
}

/// Wrapper stream to produce blocks or related events. This will continiously produce macro blocks,
/// micro blocks, macro state updates, or view changes from the underlying `ProduceMacroBlock`s and
/// `ProduceMicroBlocks`.
/// 
/// To start producing a macro or micro block, `set_macro` or `set_micro` have to be called with the
/// underlying producer.
/// 
/// Once the inner producer stream terminates, this will return `Poll::Pending` until a new producer
/// is set.
#[pin_project]
struct ProduceBlocks<TValidatorNetwork>
{
    state: ProduceBlocksState<TValidatorNetwork>,
    waker: Option<Waker>,
}

impl<TValidatorNetwork> Default for ProduceBlocks<TValidatorNetwork> {
    fn default() -> Self {
        ProduceBlocks {
            state: ProduceBlocksState::Inactive,
            waker: None,
        }
    }
}

impl<TValidatorNetwork> ProduceBlocks<TValidatorNetwork> {
    /// Set block production to inactive. This will cause the stream to return Poll::Pending
    pub fn set_inactive(&mut self) {
        self.state = ProduceBlocksState::Inactive;
        self.wake();
    }

    /// Set block production to produce a macro block.
    pub fn set_macro(&mut self, macro_block_producer: ProduceMacroBlock) {
        self.state = ProduceBlocksState::Macro(macro_block_producer);
        self.wake();
    }

    /// Set block production to produce a micro block
    pub fn set_micro(&mut self, micro_block_producer: ProduceMicroBlock<TValidatorNetwork>) {
        self.state = ProduceBlocksState::Micro(micro_block_producer);
        self.wake();
    }

    fn wake(&mut self) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}

impl<TValidatorNetwork> Stream for ProduceBlocks<TValidatorNetwork>
where
    TValidatorNetwork: ValidatorNetwork + 'static,
{
    type Item = Result<ProduceBlocksItem, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let this = self.project();

        match this.state {
            ProduceBlocksState::Inactive => {
                // Do nothing here, set waker later and return Poll::Pending
            }
            ProduceBlocksState::Macro(macro_block_producer) => {
                while let Some(event) = ready!(macro_block_producer.poll_next_unpin(cx)) {
                    match event {
                        TendermintReturn::Error(e) => {
                            return Poll::Ready(Some(Err(e.into())));
                        }
                        TendermintReturn::Result(block) => {
                            return Poll::Ready(Some(Ok(ProduceBlocksItem::MacroBlock(block))));
                        }
                        TendermintReturn::StateUpdate(update) => {
                            return Poll::Ready(Some(Ok(ProduceBlocksItem::MacroStateUpdate(update))));
                        }
                    }
                }

                // Block production finished, so we can set ourself to `Inactive`.
            },
            ProduceBlocksState::Micro(micro_block_producer) => {
                while let Some(event) = ready!(micro_block_producer.poll_next_unpin(cx)) {
                    match event {
                        ProduceMicroBlockEvent::MicroBlock(block) => {
                            return Poll::Ready(Some(Ok(ProduceBlocksItem::MicroBlock(block))));
                        }
                        ProduceMicroBlockEvent::ViewChange(view_change, view_change_proof) => {
                            return Poll::Ready(Some(Ok(ProduceBlocksItem::ViewChange(view_change, view_change_proof))));
                        }
                    }
                }

                // Block production finished, so we can set ourself to `Inactive`.
                *this.state = ProduceBlocksState::Inactive;
            }
        }
        
        // We either are already `Inactive`, or we just finished the inner block production.
        // Thus we return `Poll::Pending` until we have a new block production.

        *this.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

pub struct ProposalTopic;
impl Topic for ProposalTopic {
    type Item = SignedTendermintProposal;

    fn topic(&self) -> String {
        "tendermint-proposal".to_owned()
    }

    fn validate(&self) -> bool {
        false
    }
}

enum ValidatorStakingState {
    Active,
    Parked,
    Inactive,
    NoStake,
}

struct ActiveEpochState {
    validator_id: u16,
}

struct BlockchainState {
    fork_proofs: ForkProofPool,
}

struct ProduceMicroBlockState {
    view_number: u32,
    view_change_proof: Option<ViewChangeProof>,
    view_change: Option<ViewChange>,
}

pub struct Validator<TNetwork: Network, TValidatorNetwork: ValidatorNetwork + 'static> {
    pub consensus: ConsensusProxy<TNetwork>,
    network: Arc<TValidatorNetwork>,
    // TODO: Also have the validator ID here.
    signing_key: bls::KeyPair,
    wallet_key: Option<keys::KeyPair>,
    database: Database,
    env: Environment,

    proposal_task: Option<
        JoinHandle<
            Result<
                BoxStream<'static, (<ProposalTopic as Topic>::Item, TValidatorNetwork::PubsubId)>,
                TValidatorNetwork::Error,
            >,
        >,
    >,

    consensus_event_rx: broadcast::Receiver<ConsensusEvent<TNetwork>>,
    blockchain_event_rx: mpsc::UnboundedReceiver<BlockchainEvent>,
    fork_event_rx: mpsc::UnboundedReceiver<ForkEvent>,

    epoch_state: Option<ActiveEpochState>,
    blockchain_state: BlockchainState,

    
    macro_state: Option<PersistedMacroState<TValidatorNetwork>>,
    micro_state: ProduceMicroBlockState,
    block_producer: ProduceBlocks<TValidatorNetwork>,
}

impl<TNetwork: Network, TValidatorNetwork: ValidatorNetwork + 'static>
    Validator<TNetwork, TValidatorNetwork>
{
    const MACRO_STATE_DB_NAME: &'static str = "ValidatorState";
    const MACRO_STATE_KEY: &'static str = "validatorState";
    const VIEW_CHANGE_DELAY: Duration = Duration::from_secs(10);
    const FORK_PROOFS_MAX_SIZE: usize = 1_000; // bytes

    pub async fn new(
        consensus: &Consensus<TNetwork>,
        network: Arc<TValidatorNetwork>,
        signing_key: bls::KeyPair,
        wallet_key: Option<keys::KeyPair>,
    ) -> Self {
        let consensus_event_rx = consensus.subscribe_events();
        let blockchain_event_rx = consensus.blockchain.notifier.write().as_stream();
        let fork_event_rx = consensus.blockchain.fork_notifier.write().as_stream();

        let blockchain_state = BlockchainState {
            fork_proofs: ForkProofPool::new(),
        };

        let micro_state = ProduceMicroBlockState {
            view_number: consensus.blockchain.view_number(),
            view_change_proof: None,
            view_change: None,
        };

        let env = consensus.env.clone();
        let database = env.open_database(Self::MACRO_STATE_DB_NAME.to_string());

        let macro_state: Option<PersistedMacroState<TValidatorNetwork>> = {
            let read_transaction = ReadTransaction::new(&env);
            read_transaction.get(&database, Self::MACRO_STATE_KEY)
        };

        // Spawn into task so the lifetime does not expire.
        // Also start executing immediately as we will need to wait for this on the first macro block.
        let nw = network.clone();
        let proposal_task = Some(tokio::spawn(
            async move { nw.subscribe(&ProposalTopic).await },
        ));

        let mut this = Self {
            consensus: consensus.proxy(),
            network,
            signing_key,
            wallet_key,
            database,
            env,

            proposal_task,

            consensus_event_rx,
            blockchain_event_rx,
            fork_event_rx,

            epoch_state: None,
            blockchain_state,

            macro_state,
            micro_state,
            block_producer: ProduceBlocks::default(),
        };
        this.init().await;
        this
    }

    async fn init(&mut self) {
        self.init_epoch();
        self.init_block_producer().await;
    }

    fn init_epoch(&mut self) {
        log::debug!("Initializing epoch");

        let validators = self.consensus.blockchain.current_validators().unwrap();

        // TODO: This code block gets this validators position in the validators struct by searching it
        //  with its public key. This is an insane way of doing this. Just start saving the validator
        //  id in the Validator struct (the one in this crate).
        self.epoch_state = None;
        for (i, validator) in validators.iter().enumerate() {
            if validator.public_key.compressed() == &self.signing_key.public_key.compress() {
                self.epoch_state = Some(ActiveEpochState {
                    validator_id: i as u16,
                });
                break;
            }
        }

        let validator_keys: Vec<CompressedPublicKey> = self
            .consensus
            .blockchain
            .current_validators()
            .unwrap()
            .iter()
            .map(|validator| validator.public_key.compressed().clone())
            .collect();
        let key = self.signing_key.clone();
        let nw = self.network.clone();

        // TODO might better be done without the task.
        // However we have an entire batch to execute the task so it should not be extremely bad.
        // Also the setting up of our own public key record should probably not be done here but in `init` instead.
        tokio::spawn(async move {
            if let Err(err) = nw
                .set_public_key(&key.public_key.compress(), &key.secret_key)
                .await
            {
                error!("could not set up DHT record: {:?}", err);
            }
            nw.set_validators(validator_keys).await;
        });
    }

    async fn init_block_producer(&mut self) {
        self.block_producer.set_inactive();

        log::debug!("Initializing block producer");

        if !self.is_active() {
            log::debug!("Validator not active");
            return;
        }

        let next_block_type;
        let block_number;
        let next_view_number;

        {
            // TODO: use state lock guard instead of acquiring push lock?
            let _lock = self.consensus.blockchain.lock();
            next_block_type = self.consensus.blockchain.get_next_block_type(None);
            block_number = self.consensus.blockchain.block_number();
            next_view_number = self.consensus.blockchain.head().next_view_number();
        }

        match next_block_type {
            BlockType::Macro => {
                let block_producer = BlockProducer::new(
                    self.consensus.blockchain.clone(),
                    self.consensus.mempool.clone(),
                    self.signing_key.clone(),
                );

                let (mut sender, receiver) = mpsc::channel::<(
                    <ProposalTopic as Topic>::Item,
                    TValidatorNetwork::PubsubId,
                )>(2);

                if let Some(task) = self.proposal_task.take() {
                    self.proposal_task = Some(tokio::spawn(async move {
                        if let Ok(stream) = task.await {
                            if let Ok(mut stream) = stream {
                                while let Some(item) = stream.next().await {
                                    let mut cx = Context::from_waker(noop_waker_ref());
                                    match sender.poll_ready(&mut cx) {
                                        Poll::Pending => {
                                            // Todo: buffer proposal if necessary.
                                            log::debug!("Proposal recipient not able to receive new Messages. Waiting to try with the next proposal!");
                                        }
                                        Poll::Ready(Ok(_)) => {
                                            if let Err(_err) = sender.send(item).await {
                                                log::debug!("failed to send message through sender, even though poll_ready returned Ok");
                                            }
                                        }
                                        Poll::Ready(Err(_err)) => {
                                            // recipient is no longer present, leave the loop and return the subscription stream.
                                            log::trace!("Sonder for proposals no longer has a recipient, Block was produced!");
                                            break;
                                        }
                                    }
                                }
                                let mut cx = Context::from_waker(noop_waker_ref());
                                while let Poll::Ready(Some(_)) = stream.poll_next_unpin(&mut cx) {
                                    // noop. Empty out the stream before returning it, dropping all accumulated messages.
                                }
                                Ok(stream)
                            } else {
                                // Todo: Recover from this?
                                panic!("subscription stream returned err");
                            }
                        } else {
                            // Todo: Recover from this?
                            panic!("failed to join subscription task");
                        }
                    }));
                } else {
                    panic!("There is no proposal task. Validator dysfunctional");
                }

                // Take the current state and see if it is applicable to the current height.
                // We do not need to keep it as it is persisted.
                // This will always result in None in case the validator works as intended.
                // Only in case of a crashed node this will result in a value from which Tendermint can resume its work.
                let state = self
                    .macro_state
                    .take()
                    .map(|state| {
                        if state.height == block_number + 1 {
                            Some(state)
                        } else {
                            None
                        }
                    })
                    .flatten();

                let blockchain = self.consensus.blockchain.clone();
                let network = self.network.clone();
                let signing_key = self.signing_key.clone();
                let validator_id = self.validator_id();

                let macro_producer = ProduceMacroBlock::new(
                    blockchain,
                    network,
                    block_producer,
                    signing_key,
                    validator_id,
                    state,
                    receiver.boxed(),
                ).await;

                self.block_producer.set_macro(macro_producer);
            }
            BlockType::Micro => {
                self.micro_state = ProduceMicroBlockState {
                    view_number: next_view_number,
                    view_change_proof: None,
                    view_change: None,
                };

                let fork_proofs = self
                    .blockchain_state
                    .fork_proofs
                    .get_fork_proofs_for_block(Self::FORK_PROOFS_MAX_SIZE);

                let micro_producer = ProduceMicroBlock::new(
                    Arc::clone(&self.consensus.blockchain),
                    Arc::clone(&self.consensus.mempool),
                    Arc::clone(&self.network),
                    self.signing_key.clone(),
                    self.validator_id(),
                    fork_proofs,
                    self.micro_state.view_number,
                    self.micro_state.view_change_proof.clone(),
                    self.micro_state.view_change.clone(),
                    Self::VIEW_CHANGE_DELAY,
                );

                self.block_producer.set_micro(micro_producer);
            }
        }
    }

    async fn on_blockchain_event(&mut self, event: BlockchainEvent) {
        match event {
            BlockchainEvent::Extended(ref hash) => self.on_blockchain_extended(hash),
            BlockchainEvent::Finalized(ref hash) => self.on_blockchain_extended(hash),
            BlockchainEvent::EpochFinalized(ref hash) => {
                self.on_blockchain_extended(hash);
                self.init_epoch()
            }
            BlockchainEvent::Rebranched(ref old_chain, ref new_chain) => {
                self.on_blockchain_rebranched(old_chain, new_chain)
            }
        }

        self.init_block_producer().await;
    }

    fn on_blockchain_extended(&mut self, hash: &Blake2bHash) {
        let block = self
            .consensus
            .blockchain
            .get_block(hash, true, None)
            .expect("Head block not found");
        self.blockchain_state.fork_proofs.apply_block(&block);
    }

    fn on_blockchain_rebranched(
        &mut self,
        old_chain: &[(Blake2bHash, Block)],
        new_chain: &[(Blake2bHash, Block)],
    ) {
        for (_hash, block) in old_chain.iter() {
            self.blockchain_state.fork_proofs.revert_block(block);
        }
        for (_hash, block) in new_chain.iter() {
            self.blockchain_state.fork_proofs.apply_block(&block);
        }
    }

    fn on_fork_event(&mut self, event: ForkEvent) {
        match event {
            ForkEvent::Detected(fork_proof) => self.blockchain_state.fork_proofs.insert(fork_proof),
        };
    }

    fn is_active(&self) -> bool {
        self.epoch_state.is_some()
    }

    pub fn validator_id(&self) -> u16 {
        self.epoch_state
            .as_ref()
            .expect("Validator not active")
            .validator_id
    }

    pub fn signing_key(&self) -> bls::KeyPair {
        self.signing_key.clone()
    }

    /// Runs the validator to completion.
    /// 
    /// This only terminates if an error occurs, e.g. an event stream was closed. Some errors are not
    /// propagates, e.g. when it fails to propagate a block over the network.
    /// 
    /// # TODO
    /// 
    /// - How do we gracefully shut down a validator?
    /// 
    pub async fn run(mut self) -> Result<(), Error> {
        loop {
            futures::select! {
                // When consensus is established we initialize the epoch and block production
                event = self.consensus_event_rx.recv().fuse() => {
                    match event {
                        Ok(ConsensusEvent::Established) => {
                            self.init().await;
                        }
                        Ok(_) => {},
                        Err(broadcast::RecvError::Closed) => return Err(Error::ConsensusEventsClosed),
                        Err(broadcast::RecvError::Lagged(_)) => return Err(Error::ConsensusEventsLagged),
                    }
                    
                },

                // If consensus is established forward blockchain events
                event = self.blockchain_event_rx.recv().fuse() => {
                    match event {
                        Some(event) => {
                            if self.consensus.is_established() {
                                self.on_blockchain_event(event).await;
                            }
                        }
                        None => return Err(Error::BlockchainEventsClosed),
                    }
                },

                // If consensus is established forward fork events
                event = self.fork_event_rx.recv().fuse() => {
                    match event {
                        Some(event) => {
                            if self.consensus.is_established() {
                                self.on_fork_event(event);
                            }
                        }
                        None => return Err(Error::ForkEventsClosed),
                    }
                },

                // Get events from block producer. This either gives us produced blocks, view changes,
                // or macro state updates. Blocks are pushed to the blockchain and propagated to the
                // network. View changes update the micro state.
                item = self.block_producer.next().fuse() => {
                    match item {
                        Some(Ok(ProduceBlocksItem::MacroBlock(macro_block))) => {
                            // If the event is a result meaning the next macro block was produced we push it onto our local chain
                            let result = self.consensus
                                .blockchain
                                .push(Block::Macro(macro_block.clone()));

                            match result {
                                Ok(PushResult::Extended) | Ok(PushResult::Rebranched) => {
                                    if macro_block.is_election_block() {
                                        info!("Publishing Election MacroBlock #{}", &macro_block.header.block_number);
                                    } else {
                                        info!("Publishing Checkpoint MacroBlock #{}", &macro_block.header.block_number);
                                    }
    
                                    if let Err(e) = self.network.publish(&BlockTopic, Block::Macro(macro_block)).await {
                                        log::warn!("Failed to publish block: {}", e);
                                    }
                                },
                                Err(e) => log::warn!("Failed to push macro block: {}", e),
                                _ => {},
                            }
                        },
                        Some(Ok(ProduceBlocksItem::MicroBlock(micro_block))) => {
                            let result = self.consensus
                                .blockchain
                                .push(Block::Micro(micro_block.clone()));

                            match result {
                                Ok(PushResult::Extended) | Ok(PushResult::Rebranched) => {
                                    info!("Publishing micro block: {:?}", micro_block);

                                    if let Err(e) = self.network.publish(&BlockTopic, Block::Micro(micro_block)).await {
                                        log::warn!("Failed to publish block: {}", e);
                                    }
                                },
                                Err(e) => log::warn!("Failed to push micro block: {}", e),
                                _ => {},
                            }
                        },
                        Some(Ok(ProduceBlocksItem::MacroStateUpdate(update))) => {
                            let mut write_transaction = WriteTransaction::new(&self.env);
                            let persistable_state = PersistedMacroState::<TValidatorNetwork> {
                                height: self.consensus.blockchain.block_number() + 1,
                                step: update.step.into(),
                                round: update.round,
                                locked_round: update.locked_round,
                                locked_value: update.locked_value,
                                valid_round: update.valid_round,
                                valid_value: update.valid_value,
                            };

                            write_transaction.put::<str, Vec<u8>>(
                                &self.database,
                                Self::MACRO_STATE_KEY,
                                &beserial::Serialize::serialize_to_vec(&persistable_state),
                            );

                            self.macro_state = Some(persistable_state);
                        },
                        Some(Ok(ProduceBlocksItem::ViewChange(view_change, view_change_proof))) => {
                            self.micro_state.view_number = view_change.new_view_number; // needed?
                            self.micro_state.view_change_proof = Some(view_change_proof);
                            self.micro_state.view_change = Some(view_change);
                        },
                        Some(Err(e)) => {
                            return Err(e.into());
                        }
                        None => unreachable!(),
                    }
                },
            }
        }
    }
}
