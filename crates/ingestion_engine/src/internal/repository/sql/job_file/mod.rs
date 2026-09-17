#![cfg_attr(coverage, coverage(off))]
pub mod implementation;

use pkg::mysql::DbPool;

#[cfg_attr(coverage, coverage(off))]
pub struct MySqlJobFileRepository {
    pub(crate) pool: DbPool,
}

#[cfg_attr(coverage, coverage(off))]
impl MySqlJobFileRepository {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }
}
