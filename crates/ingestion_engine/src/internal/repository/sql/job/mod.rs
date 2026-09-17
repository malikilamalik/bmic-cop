#![cfg_attr(coverage, coverage(off))]
pub mod implementation;

use pkg::mysql::DbPool;

#[cfg_attr(coverage, coverage(off))]
pub struct MySqlJobRepository {
    pub(crate) pool: DbPool,
}

#[cfg_attr(coverage, coverage(off))]
impl MySqlJobRepository {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }
}
