/// A MapReduce worker
#[derive(Debug)]
pub struct Args {
    // The worker does not take any arguments
}
impl Args {
    pub fn parse() -> Self { Self {} }
}
