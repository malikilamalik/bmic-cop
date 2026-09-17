#![cfg_attr(coverage, coverage(off))]
pub mod list;

use pkg::mysql::DbPool;

#[cfg_attr(coverage, coverage(off))]
pub struct MySqlJobDetailRepository {
    pub(crate) pool: DbPool,
}

#[cfg_attr(coverage, coverage(off))]
impl MySqlJobDetailRepository {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }
}
