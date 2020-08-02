use beserial::{Deserialize, Serialize};
use block_albatross::{Block, BlockType};
use keys::Address;
use primitives::coin::Coin;
use std::collections::HashSet;
use transaction::Transaction;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum SubscriptionType {
    None = 0,
    Hashes = 1,
    Objects = 2,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[repr(u8)]
pub enum TransactionFilter {
    #[beserial(discriminant = 0)]
    Addresses(#[beserial(len_type(u16))] HashSet<Address>),
    #[beserial(discriminant = 1)]
    MinFee(Coin), // Fee per byte
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[repr(u8)]
pub enum BlockFilter {
    #[beserial(discriminant = 0)]
    All,
    #[beserial(discriminant = 1)]
    MacroBlocks,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Subscription {
    pub tx_announcements: SubscriptionType,
    pub tx_filter: Option<TransactionFilter>,
    pub block_announcements: SubscriptionType,
    pub block_filter: BlockFilter,
}

impl Default for Subscription {
    fn default() -> Self {
        Subscription {
            tx_announcements: SubscriptionType::None,
            tx_filter: None,
            block_announcements: SubscriptionType::None,
            block_filter: BlockFilter::All,
        }
    }
}

impl Subscription {
    pub fn matches_block(&self, block: &Block) -> bool {
        match self.block_announcements {
            SubscriptionType::None => false,
            _ => match self.block_filter {
                BlockFilter::All => true,
                BlockFilter::MacroBlocks => block.ty() == BlockType::Macro,
            },
        }
    }

    pub fn matches_transaction(&self, transaction: &Transaction) -> bool {
        if self.tx_announcements == SubscriptionType::None {
            return false;
        }

        match self.tx_filter {
            None => true,
            Some(TransactionFilter::Addresses(ref addresses)) => {
                addresses.contains(&transaction.sender)
            }
            Some(TransactionFilter::MinFee(ref min_fee)) => {
                // TODO: Potential overflow for u64
                min_fee
                    .checked_mul(transaction.serialized_size() as u64)
                    .map(|block_fee| transaction.fee >= block_fee)
                    .unwrap_or(true)
            }
        }
    }
}
