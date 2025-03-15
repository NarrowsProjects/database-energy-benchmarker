use super::database::Database;
use crate::metrics::{start_power_monitoring, stop_power_monitoring};
use crate::data_generator::generate_fixed_size_word;
use async_trait::async_trait;
use bson::{doc, to_document, Document};
use futures::stream::TryStreamExt;
use mongodb::{Client, Collection, IndexModel};
use mongodb::options::{AggregateOptions, ClientOptions};

use serde_json::Value;
use std::error::Error;

pub struct MongoDB {
    client: Option<Client>
}

#[async_trait]
impl Database for MongoDB {
    fn name(&self) -> &'static str {
        "MongoDB"
    }

    async fn connect(&mut self) -> Result<(), Box<dyn Error>> {
        let client_options = ClientOptions::parse("mongodb://localhost:27017").await?;
        let client: Client = Client::with_options(client_options)?;
        self.client = Some(client);
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(client) = self.client.take() {
            client.shutdown().await;
        }
        Ok(())
    }

    async fn clean_database(&self) -> Result<(), Box<dyn Error>> {
        if let Some(client) = &self.client {
            let db = client.database("benchmark");
            db.collection::<Document>("data").drop(None).await?;
        }
        Ok(())
    }

    async fn insert_test_data(
        &self,
        batch_size: usize,
        test_data: Vec<Value>,
    ) -> Result<(), Box<dyn Error>> {
        let mut all_ids = Vec::new();

        if let Some(client) = &self.client {
            let db = client.database("benchmark");
            let collection = db.collection::<Document>("data");

            let docs: Vec<Document> = test_data
                .into_iter()
                .map(|v| to_document(&v))
                .collect::<Result<_, _>>()?;

            for chunk in docs.chunks(batch_size) {
                let result = collection.insert_many(chunk.to_vec(), None).await?;

                all_ids.extend(
                    result
                        .inserted_ids
                        .values()
                        .map(|id| id.as_object_id().unwrap()),
                );
            }
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
    ) -> Result<(), Box<dyn Error>> {
        if use_index {
            self.create_index(depth).await?;
        }

        if let Some(client) = &self.client {
            let db = client.database("benchmark");
            let collection = db.collection::<Document>("data");

            let mut pcm_process = start_power_monitoring(measurement_name);
            if num_reads > num_writes {
                self.run_read_heavy(num_reads, num_writes, depth, num_docs, collection)
                    .await?;
            } else {
                self.run_write_heavy(num_reads, num_writes, depth, num_docs, collection)
                    .await?;
            }
            stop_power_monitoring(&mut pcm_process);
        }

        Ok(())
    }
}

impl MongoDB {
    pub fn new() -> Self {
        MongoDB {
            client: None
        }
    }

    async fn create_index(&self, depth: u8) -> Result<(), Box<dyn Error>> {
        let path = self.create_read_path(depth);
        let index_keys = doc! { &path: 1 };

        if let Some(client) = &self.client {
            let db = client.database("benchmark");
            let collection = db.collection::<Document>("data");

            let index_model = IndexModel::builder().keys(index_keys).build();
            collection.create_index(index_model, None).await?;
        }
        Ok(())
    }

    fn create_read_path(&self, depth: u8) -> String {
        format!("{}read_target", "children.".repeat(depth as usize - 1))
    }

    fn create_write_path(&self, depth: u8) -> String {
        format!("{}write_target", "children.0.".repeat(depth as usize - 1))
    }

    fn create_read_filter(&self, depth: u8) -> Document {
        let path = self.create_read_path(depth);
        doc! { &path: "read-target" }
    }
    
    fn create_update(&self, depth: u8) -> Document {
        let path = self.create_write_path(depth);
        let update_value = generate_fixed_size_word(16);
    
        doc! {
            "$set": {
                &path: update_value
            }
        }
    }

    fn create_read_options(&self, num_doc: u32) -> AggregateOptions {
        AggregateOptions::builder()
            .batch_size(Some(num_doc))
            .build()
    }

    fn create_read_pipeline(&self, depth: u8) -> Vec<Document> {
        let path = self.create_read_path(depth);
        vec![
            doc! {
                "$match": {
                    &path: "read_target"
                }
            },
            doc! {
                "$project": {
                    "_id": 0,
                    "value": format!("${}", path)
                }
            }        
        ]
    }

    async fn run_read_heavy(
        &self,
        num_reads: usize,
        num_writes: usize,
        depth: u8,
        num_doc: usize,
        collection: Collection<Document>,
    ) -> Result<(), Box<dyn Error>> {
        // Pipeline so, that it doesn't send an entire document over the network causing noice.
        let read_pipeline = self.create_read_pipeline(depth); 
        let options = self.create_read_options(num_doc as u32);
        let filter = self.create_read_filter(depth); 
        let update = self.create_update(depth);

        for _ in 0..num_writes {
            for _ in 0..(num_reads / num_writes) {
                let cursor = collection
                    .aggregate(read_pipeline.clone(), Some(options.clone()))
                    .await?;
                let _docs:Vec<Document> = cursor.try_collect().await?;
            }
            collection
                .update_many(filter.clone(), update.clone(), None)
                .await?;
        }
        Ok(())
    }

    async fn run_write_heavy(
        &self,
        num_reads: usize,
        num_writes: usize,
        depth: u8,
        num_doc: usize,
        collection: Collection<Document>,
    ) -> Result<(), Box<dyn Error>> {
        let read_filter = self.create_read_pipeline(depth);
        let update_filter = self.create_read_filter(depth);
        let update = self.create_update(depth);
        let read_options = self.create_read_options(num_doc as u32);

        for _ in 0..num_reads {
            for _ in 0..(num_writes / num_reads) {
                collection
                    .update_many(update_filter.clone(), update.clone(), None)
                    .await?;
            }

            let cursor = collection
                .aggregate(read_filter.clone(), Some(read_options.clone()))
                .await?;
            
            let _docs:Vec<Document> = cursor.try_collect().await?;
        }
        Ok(())
    }
}