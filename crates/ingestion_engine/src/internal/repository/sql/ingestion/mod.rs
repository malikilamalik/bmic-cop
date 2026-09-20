#![cfg_attr(coverage, coverage(off))]
pub mod implementation;

use pkg::mysql::DbPool;

#[cfg_attr(coverage, coverage(off))]
pub struct MySqlIngestionRepository {
    pub(crate) pool: DbPool,
}

#[cfg_attr(coverage, coverage(off))]
impl MySqlIngestionRepository {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }
}
