use async_trait::async_trait;
use serde_json::Value;
use std::error::Error;

#[async_trait]
pub trait Database {
    fn name(&self) -> &'static str;
    async fn connect(&mut self) -> Result<(), Box<dyn Error>>;
    async fn disconnect(&mut self) -> Result<(), Box<dyn Error>>;
    async fn clean_database(&self) -> Result<(), Box<dyn Error>>;
    async fn insert_test_data(
        &self,
        batch_size: usize,
        test_data: Vec<Value>,
    ) -> Result<(), Box<dyn Error>>;
    async fn run_queries(
        &self,
        depth: u8,
        num_reads: usize,
        num_writes: usize,
        use_index: bool,
        num_docs: usize,
        measurement_name: &str,
    ) -> Result<(), Box<dyn Error>>;
}
