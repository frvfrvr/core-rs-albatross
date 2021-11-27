use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use futures::future::BoxFuture;
use futures::task::{Context, Poll};
use futures::{ready, FutureExt, Stream};
use parking_lot::RwLock;
use tokio::time;

use beserial::Serialize;
use block::{Block, ForkProof, MicroBlock, ViewChange, ViewChangeProof};
use block_production::BlockProducer;
use blockchain::{AbstractBlockchain, Blockchain, PushResult};
use mempool::mempool::Mempool;
use nimiq_account::{Account, AccountError, AccountTransactionInteraction, Accounts};
use nimiq_bls::KeyPair as BlsKeyPair;
use nimiq_database::WriteTransaction;
use nimiq_keys::KeyPair as SchnorrKeyPair;
use nimiq_primitives::policy;
use nimiq_primitives::slots::Validators;
use nimiq_transaction::Transaction;
use nimiq_validator_network::ValidatorNetwork;
use utils::time::systemtime_to_timestamp;
use vrf::VrfSeed;

use crate::aggregation::view_change::ViewChangeAggregation;

// Ignoring this clippy warning since size difference is not that much (320
// bytes) and we probably don't want the performance penalty of the allocation.
#[allow(clippy::large_enum_variant)]
pub(crate) enum ProduceMicroBlockEvent {
    MicroBlock(MicroBlock, PushResult),
    ViewChange(ViewChange, ViewChangeProof),
}

#[derive(Clone)]
struct NextProduceMicroBlockEvent<TValidatorNetwork> {
    blockchain: Arc<RwLock<Blockchain>>,
    mempool: Arc<Mempool>,
    network: Arc<TValidatorNetwork>,
    signing_key: SchnorrKeyPair,
    voting_key: BlsKeyPair,
    validator_id: u16,
    fork_proofs: Vec<ForkProof>,
    prev_seed: VrfSeed,
    block_number: u32,
    view_number: u32,
    view_change_proof: Option<ViewChangeProof>,
    view_change: Option<ViewChange>,
    view_change_delay: Duration,
}

impl<TValidatorNetwork: ValidatorNetwork + 'static> NextProduceMicroBlockEvent<TValidatorNetwork> {
    const MAX_TXN_CHECK_ITERATIONS: u32 = 3;
    // Ignoring clippy warning because there wouldn't be much to be gained by refactoring this,
    // except making clippy happy
    #[allow(clippy::too_many_arguments)]
    fn new(
        blockchain: Arc<RwLock<Blockchain>>,
        mempool: Arc<Mempool>,
        network: Arc<TValidatorNetwork>,
        signing_key: SchnorrKeyPair,
        voting_key: BlsKeyPair,
        validator_id: u16,
        fork_proofs: Vec<ForkProof>,
        prev_seed: VrfSeed,
        block_number: u32,
        view_number: u32,
        view_change_proof: Option<ViewChangeProof>,
        view_change: Option<ViewChange>,
        view_change_delay: Duration,
    ) -> Self {
        Self {
            blockchain,
            mempool,
            network,
            signing_key,
            voting_key,
            validator_id,
            fork_proofs,
            prev_seed,
            block_number,
            view_number,
            view_change_proof,
            view_change,
            view_change_delay,
        }
    }

    async fn next(
        mut self,
    ) -> (
        Option<ProduceMicroBlockEvent>,
        NextProduceMicroBlockEvent<TValidatorNetwork>,
    ) {
        let in_current_state = |block: &Block| {
            self.prev_seed == *block.seed()
                || self.block_number == block.block_number() + 1
                || self.view_number >= block.view_number()
        };

        // Acquire blockchain.upgradable_read() to prevent further changes to the blockchain while
        // we're constructing the block. Check if we're still in the correct state, abort otherwise.
        let return_value = {
            let blockchain = self.blockchain.upgradable_read();
            if !in_current_state(&blockchain.head()) {
                Some(None)
            } else if self.is_our_turn(&*blockchain) {
                info!(
                    "[{}] Our turn at #{}:{}, producing micro block",
                    self.validator_id, self.block_number, self.view_number
                );

                let block = self.produce_micro_block(&*blockchain);

                debug!(
                    "Produced micro block #{}.{} with {} transactions",
                    block.header.block_number,
                    block.header.view_number,
                    block
                        .body
                        .as_ref()
                        .map(|body| body.transactions.len())
                        .unwrap_or(0)
                );

                let block1 = block.clone();

                // Use a trusted push since these blocks were generated by this validator
                let result = if cfg!(feature = "trusted_push") {
                    Blockchain::trusted_push(blockchain, Block::Micro(block))
                } else {
                    Blockchain::push(blockchain, Block::Micro(block))
                };

                if let Err(e) = &result {
                    error!("Failed to push our own block onto the chain: {:?}", e);
                }

                let event = result
                    .map(move |result| ProduceMicroBlockEvent::MicroBlock(block1, result))
                    .ok();
                Some(event)
            } else {
                None
            }
        };
        if let Some(event) = return_value {
            return (event, self);
        }

        debug!(
            "[{}] Not our turn at #{}:{}, waiting for micro block",
            self.validator_id, self.block_number, self.view_number
        );
        time::sleep(self.view_change_delay).await;
        info!(
            "No micro block received within timeout at #{}:{}, starting view change",
            self.block_number, self.view_number
        );

        // Acquire a blockchain read lock and check if the state still matches to fetch active validators.
        let active_validators = {
            let blockchain = self.blockchain.read();
            if in_current_state(&blockchain.head()) {
                Some(blockchain.current_validators().unwrap())
            } else {
                None
            }
        };
        if active_validators.is_none() {
            return (None, self);
        }

        let (view_change, view_change_proof) = self.change_view(active_validators.unwrap()).await;
        info!(
            "View change completed for #{}:{}, new view is {}",
            self.block_number, self.view_number, view_change.new_view_number
        );
        let event = ProduceMicroBlockEvent::ViewChange(view_change, view_change_proof);
        (Some(event), self)
    }

    fn is_our_turn(&self, blockchain: &Blockchain) -> bool {
        // TODO: This match() used to be an expect(), I changed it because there is a case where the block
        // producer will continue running for a while in parallel to a rebranch operation that will
        // eventually drop it; when this happens, we want to keep running (while also not producing anything)
        // instead of panicking (it shouldn't matter since we will eventually drop this producer), but the
        // correct fix for this would be dropping the validator (or somehow stop its operation) as soon as we
        // know we're rebranching
        let slot = match blockchain.get_slot_owner_at(self.block_number, self.view_number, None) {
            Some((slot, _)) => slot,
            None => {
                warn!("Couldn't find who the next slot owner is, this should only happen if we rebranched while processing a view change");
                return false;
            }
        };

        self.signing_key.public == slot.signing_key
    }

    fn produce_micro_block(&self, blockchain: &Blockchain) -> MicroBlock {
        // TODO pass keys by reference
        let producer = BlockProducer::new(self.signing_key.clone(), self.voting_key.clone());

        let timestamp = u64::max(
            blockchain.timestamp(),
            systemtime_to_timestamp(SystemTime::now()),
        );

        let transactions = self.collect_transactions(blockchain, timestamp);

        producer.next_micro_block(
            blockchain,
            timestamp,
            self.view_number,
            self.view_change_proof.clone(),
            self.fork_proofs.clone(),
            transactions,
            vec![], // TODO
        )
    }

    async fn change_view(
        &mut self,
        active_validators: Validators,
    ) -> (ViewChange, ViewChangeProof) {
        let new_view_number = self.view_number + 1;
        let view_change = ViewChange {
            block_number: self.block_number,
            new_view_number,
            prev_seed: self.prev_seed.clone(),
        };

        // Include the previous_view_change_proof only if it has not yet been persisted on chain.
        let view_change_proof = self.view_change.as_ref().and_then(|vc| {
            if vc.block_number == self.block_number {
                Some(self.view_change_proof.as_ref().unwrap().sig.clone())
            } else {
                None
            }
        });

        let (view_change, view_change_proof) = ViewChangeAggregation::start(
            view_change.clone(),
            view_change_proof,
            self.voting_key.clone(),
            self.validator_id,
            active_validators,
            Arc::clone(&self.network),
        )
        .await;

        // Set the view change and view_change_proof properties so in case another view change happens they are available.
        self.view_number = view_change.new_view_number;
        self.view_change = Some(view_change.clone());
        self.view_change_proof = Some(view_change_proof.clone());

        (view_change, view_change_proof)
    }
}

impl<TValidatorNetwork: ValidatorNetwork + 'static> NextProduceMicroBlockEvent<TValidatorNetwork> {
    fn collect_transactions(&self, blockchain: &Blockchain, timestamp: u64) -> Vec<Transaction> {
        // Get transactions before acquiring blockchain.read as it will take mempool state
        // lock which we do not want to take with a blockchain lock held.
        // FIXME lock order
        let mut transactions = self
            .mempool
            .get_transactions_for_block(MicroBlock::get_available_bytes(self.fork_proofs.len()));

        let mut iterations = 0;
        let mut final_transactions: Vec<Transaction> = vec![];
        let env = blockchain.state().accounts.env.clone();
        let mut txn = WriteTransaction::new(&env);

        while iterations < Self::MAX_TXN_CHECK_ITERATIONS {
            // If the transaction isn't valid we need to remove it
            let removed_txns = Self::filter_invalid_staking_transactions(
                &mut transactions,
                &blockchain.state().accounts,
                self.block_number,
                timestamp,
                &mut txn,
            );

            // Add the surviving transactions to the original transactions
            final_transactions.extend(transactions);

            let removed_txns_size = removed_txns
                .iter()
                .fold(0, |acc, transaction| acc + transaction.serialized_size());

            if removed_txns_size != 0 {
                log::debug!(
                    "Dropped {} transactions doing staking verifications",
                    removed_txns.len()
                );

                // Since we dropped some transactions, try to collect more transactions from the mempool
                // FIXME Potential deadlock
                let new_transactions = self.mempool.get_transactions_for_block(removed_txns_size);
                if new_transactions.is_empty() {
                    // Mempool is empty, build the block with the surviving transactions
                    break;
                } else {
                    // There are transactions in the mempool to fill the block given that we just dropped sone
                    iterations += 1;

                    if iterations == Self::MAX_TXN_CHECK_ITERATIONS {
                        // If we are in the last iteration, add available transactions. Final checks are going
                        // to be done after this loop anyway.
                        final_transactions.extend(new_transactions);
                        break;
                    } else {
                        // Iterate again to filter the recently obtained transactions and see if we can use them
                        transactions = new_transactions;
                    }
                }
            } else {
                // Stop the iterations since no transactions were removed
                break;
            }
        }

        // Abort the write transaction
        txn.abort();

        // Do one more check with all the final transactions ordered
        final_transactions.sort_unstable();

        let mut txn = WriteTransaction::new(&env);
        Self::filter_invalid_staking_transactions(
            &mut final_transactions,
            &blockchain.state().accounts,
            self.block_number,
            timestamp,
            &mut txn,
        );
        txn.abort();

        final_transactions
    }

    fn check_staking_incoming_txn(
        accounts: &Accounts,
        transaction: &mut Transaction,
        block_number: u32,
        timestamp: u64,
        txn: &mut WriteTransaction,
    ) -> Result<(), AccountError> {
        if transaction.recipient == policy::STAKING_CONTRACT_ADDRESS {
            // Incoming staking transaction
            Account::commit_incoming_transaction(
                &accounts.tree,
                txn,
                transaction,
                block_number,
                timestamp,
            )?;
        }
        // Not a staking contract transaction or all checks passed, return ok
        Ok(())
    }

    fn check_staking_outgoing_txn(
        accounts: &Accounts,
        transaction: &mut Transaction,
        block_height: u32,
        timestamp: u64,
        txn: &mut WriteTransaction,
    ) -> Result<(), AccountError> {
        if transaction.sender == policy::STAKING_CONTRACT_ADDRESS {
            // Outgoing staking transaction
            Account::commit_outgoing_transaction(
                &accounts.tree,
                txn,
                transaction,
                block_height,
                timestamp,
            )?;
        }
        // Not a staking contract transaction or all checks passed, return ok
        Ok(())
    }

    fn filter_invalid_staking_transactions(
        transactions: &mut Vec<Transaction>,
        accounts: &Accounts,
        block_number: u32,
        timestamp: u64,
        txn: &mut WriteTransaction,
    ) -> Vec<Transaction> {
        // First check outgoing staking transactions then check incoming
        // staking transactions.
        // This is to be consistent with the way that the accounts are added to
        // the accounts tree: first commits outgoing transactions and then the
        // incoming ones.
        let mut removed_txns: Vec<Transaction> = transactions
            .drain_filter(|transaction| {
                Self::check_staking_outgoing_txn(
                    accounts,
                    transaction,
                    block_number,
                    timestamp,
                    txn,
                )
                .is_err()
            })
            .collect();

        let removed_incoming_txns: Vec<Transaction> = transactions
            .drain_filter(|transaction| {
                Self::check_staking_incoming_txn(
                    accounts,
                    transaction,
                    block_number,
                    timestamp,
                    txn,
                )
                .is_err()
            })
            .collect();

        removed_txns.extend(removed_incoming_txns);
        removed_txns
    }
}

pub(crate) struct ProduceMicroBlock<TValidatorNetwork> {
    next_event: Option<
        BoxFuture<
            'static,
            (
                Option<ProduceMicroBlockEvent>,
                NextProduceMicroBlockEvent<TValidatorNetwork>,
            ),
        >,
    >,
}

impl<TValidatorNetwork: ValidatorNetwork + 'static> ProduceMicroBlock<TValidatorNetwork> {
    // Ignoring clippy warning because there wouldn't be much to be gained by refactoring this,
    // except making clippy happy
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        blockchain: Arc<RwLock<Blockchain>>,
        mempool: Arc<Mempool>,
        network: Arc<TValidatorNetwork>,
        signing_key: SchnorrKeyPair,
        voting_key: BlsKeyPair,
        validator_id: u16,
        fork_proofs: Vec<ForkProof>,
        prev_seed: VrfSeed,
        block_number: u32,
        view_number: u32,
        view_change_proof: Option<ViewChangeProof>,
        view_change: Option<ViewChange>,
        view_change_delay: Duration,
    ) -> Self {
        let next_event = NextProduceMicroBlockEvent::new(
            blockchain,
            mempool,
            network,
            signing_key,
            voting_key,
            validator_id,
            fork_proofs,
            prev_seed,
            block_number,
            view_number,
            view_change_proof,
            view_change,
            view_change_delay,
        )
        .next()
        .boxed();
        Self {
            next_event: Some(next_event),
        }
    }
}

impl<TValidatorNetwork: ValidatorNetwork + 'static> Stream
    for ProduceMicroBlock<TValidatorNetwork>
{
    type Item = ProduceMicroBlockEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let next_event = match self.next_event.as_mut() {
            Some(next_event) => next_event,
            None => return Poll::Ready(None),
        };

        let (event, next_event) = ready!(next_event.poll_unpin(cx));
        let event = match event {
            Some(event) => event,
            None => {
                self.next_event.take();
                return Poll::Ready(None);
            }
        };

        self.next_event = match &event {
            ProduceMicroBlockEvent::MicroBlock(..) => None,
            ProduceMicroBlockEvent::ViewChange(..) => Some(next_event.next().boxed()),
        };
        Poll::Ready(Some(event))
    }
}
