use thiserror;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, thiserror::Error)]
pub enum PBaseError {
    #[error("database does not exist")]
    DatabaseDoesNotExist,
}
