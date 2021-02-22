use thiserror::Error;


#[derive(Debug, Error)]
pub enum Error {
    #[error("Consensus event stream closed")]
    ConsensusEventsClosed,
    #[error("Blockchain event stream closed")]
    BlockchainEventsClosed,
    #[error("Fork event stream closed")]
    ForkEventsClosed,

    #[error("Consensus event stream lagged")]
    ConsensusEventsLagged,

    #[error("Tendermint error")]
    Tendermint(#[from] nimiq_tendermint::TendermintError),

    #[error("Failed to push block to block chain")]
    Push(#[from] blockchain_albatross::PushError),

    #[error("Failed to publish block")]
    PublishError,
}