#![cfg_attr(coverage, coverage(off))]
pub mod implementation;
pub mod init;
pub mod list;

use pkg::mysql::DbPool;

#[cfg_attr(coverage, coverage(off))]
pub struct MySqlEvaluatorRepository {
    pub(crate) pool: DbPool,
}

#[cfg_attr(coverage, coverage(off))]
impl MySqlEvaluatorRepository {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }
}
