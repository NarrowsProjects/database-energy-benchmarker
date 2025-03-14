use crate::data_generator::generate_fixed_size_word;
use crate::metrics::{start_power_monitoring, stop_power_monitoring};
use async_trait::async_trait;
use postgres_types::{Json, Type};
use serde_json::{json, Value};
use std::error::Error;
use std::pin::pin;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::{Client, NoTls};

use super::database::Database;

pub struct PostgreSQL {
    client: Option<Arc<Mutex<Client>>>,
    _connection: Option<tokio::task::JoinHandle<()>>,
}

#[async_trait]
impl Database for PostgreSQL {
    fn name(&self) -> &'static str {
        "PostgreSQL"
    }

    async fn connect(&mut self) -> Result<(), Box<dyn Error>> {
        let (client, connection) = tokio_postgres::connect(
            "host=localhost user=postgres password=postgres dbname=benchmark",
            NoTls,
        )
        .await?;

        let connection_task = tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("PostgreSQL connection error: {}", e);
            }
        });

        self.client = Some(Arc::new(Mutex::new(client)));
        self._connection = Some(connection_task);
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(handle) = self._connection.take() {
            handle.abort();
        }
        if let Some(client) = self.client.take() {
            let client = Arc::try_unwrap(client).unwrap();
            let _ = client.lock().await;
        }
        Ok(())
    }

    async fn clean_database(&self) -> Result<(), Box<dyn Error>> {
        if let Some(client) = &self.client {
            let client = client.lock().await;
            client
                .execute("DROP TABLE IF EXISTS hierarchical_data", &[])
                .await?;
        }
        Ok(())
    }

    async fn insert_test_data(
        &self,
        _: usize,
        test_data: Vec<Value>,
    ) -> Result<(), Box<dyn Error>> {
        self.create_table().await?;

        if let Some(client) = &self.client {
            let mut client = client.lock().await;
            let transaction = client.transaction().await?;
            let sink = transaction
                .copy_in("COPY hierarchical_data (data) FROM STDIN (FORMAT binary)")
                .await?;

            let mut encoder = pin!(BinaryCopyInWriter::new(sink, &[Type::JSONB]));

            for doc in &test_data {
                encoder.as_mut().write(&[&Json(doc)]).await?;
            }
            encoder.as_mut().finish().await?;

            transaction.commit().await?;
        }
        Ok(())
    }

    async fn run_queries(
        &self,
        depth: u8,
        num_reads: usize,
        num_writes: usize,
        use_index: bool,
        num_docs: usize,
        measurement_name: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(client) = &self.client {
            if use_index {
                self.create_index().await?;
            }

            let client = client.lock().await;

            let mut pcm_process = start_power_monitoring(measurement_name);
            if num_reads > num_writes {
                self.run_read_heavy(num_reads, num_writes, depth, num_docs, &client)
                    .await?;
            } else {
                self.run_write_heavy(num_reads, num_writes, depth, num_docs, &client)
                    .await?;
            }
            stop_power_monitoring(&mut pcm_process);
        }
        Ok(())
    }
}

impl PostgreSQL {
    async fn create_table(&self) -> Result<(), Box<dyn Error>> {
        if let Some(client) = &self.client {
            let client = client.lock().await;
            client
                .batch_execute(
                    "CREATE TABLE IF NOT EXISTS hierarchical_data (
                        id SERIAL PRIMARY KEY,
                        data JSONB NOT NULL
                    )",
                )
                .await?;
        }
        Ok(())
    }
    pub fn new() -> Self {
        PostgreSQL {
            client: None,
            _connection: None,
        }
    }

    fn create_read_query(&self, depth: u8) -> String {
        let target_json = self.create_containment_json(depth);
        let read_path = self.create_read_path(depth);
        format!(
            "SELECT data #> '{{{}}}'::text[] 
            FROM hierarchical_data WHERE data @> '{}'",
            read_path, target_json
        )
    }

    fn create_write_query(&self, depth: u8) -> String {
        let write_path = self.create_write_path(depth);
        let update_value = generate_fixed_size_word(16); // control for randomness in bytesize
        let target_json = self.create_containment_json(depth);

        format!(
            "UPDATE hierarchical_data SET data = jsonb_set(
                data,
                '{{{}}}'::text[],
                '\"{}\"'::jsonb
            )
            WHERE data @> '{}'",
            write_path, update_value, target_json
        )
    }

    async fn run_read_heavy(
        &self,
        num_reads: usize,
        num_writes: usize,
        depth: u8,
        _num_docs: usize,
        client: &Client,
    ) -> Result<(), Box<dyn Error>> {
        let ratio = num_reads / num_writes;
        let write_query = self.create_write_query(depth);
        let read_query = self.create_read_query(depth);

        for _ in 0..num_writes {
            for _ in 0..ratio {
                let _rows = client.query(&read_query, &[]).await?;
            }

            client.execute(&write_query, &[]).await?;
        }

        Ok(())
    }

    async fn run_write_heavy(
        &self,
        num_reads: usize,
        num_writes: usize,
        depth: u8,
        _num_docs: usize,
        client: &Client,
    ) -> Result<(), Box<dyn Error>> {
        let ratio = num_writes / num_reads;
        let write_query = self.create_write_query(depth);
        let read_query = self.create_read_query(depth);

        for _ in 0..num_reads {
            for _ in 0..ratio {
                client.execute(&write_query, &[]).await?;
            }

            let _rows = client.query(&read_query, &[]).await?;
        }
        Ok(())
    }

    async fn create_index(&self) -> Result<(), Box<dyn Error>> {
        if let Some(client) = &self.client {
            let client = client.lock().await;

            client.execute(
                "CREATE INDEX IF NOT EXISTS idx_gin_data ON hierarchical_data USING GIN (data jsonb_path_ops)",
                &[],
            ).await?;
        }

        Ok(())
    }

    fn create_containment_json(&self, depth: u8) -> Value {
        let mut value = json!({});
        let mut current = &mut value;

        for _ in 0..depth - 1 {
            current["children"] = json!([{}]);
            current = &mut current["children"][0];
        }

        current["read_target"] = json!("read_target");
        value
    }
    fn create_read_path(&self, depth: u8) -> String {
        format!("{}read_target", "children,0,".repeat(depth as usize - 1))
    }
    fn create_write_path(&self, depth: u8) -> String {
        format!("{}write_target", "children,0,".repeat(depth as usize - 1))
    }
}
