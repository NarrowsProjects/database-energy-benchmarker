use crate::databases::database::Database;
use crate::metrics::{start_power_monitoring, stop_power_monitoring};
use serde_json::Value;
use std::error::Error;
use std::fs;
use std::time::Duration;
use tokio::time::{sleep, Instant};

pub async fn run_read_heavy(
    db: &dyn Database,
    depth: u8,
    use_index: bool,
    num_docs: usize,
    epoch: u8,
) -> Result<u128, Box<dyn Error>> {
    let measurement_name = format!(
        "{}_read_heavy_depth_{}_epoch_{}{}.csv",
        db.name(),
        depth,
        epoch,
        if use_index { "_with_index" } else { "" }
    );

    let read_ops =  1000;
    let write_ops = 200;

    let start_time = Instant::now();
    db.run_queries(depth, read_ops, write_ops, use_index, num_docs, &measurement_name)
        .await?;

    let duration = start_time.elapsed().as_millis();
    Ok(duration)
}

pub async fn run_write_heavy(
    db: &dyn Database,
    depth: u8,
    use_index: bool,
    num_docs: usize,
    epoch: u8,
) -> Result<u128, Box<dyn Error>> {
    let measurement_name: String = format!(
        "{}_write_heavy_depth_{}_epoch_{}{}.csv",
        db.name(),
        depth,
        epoch,
        if use_index { "_with_index" } else { "" }
    );
    
    let read_ops = 200;
    let write_ops = 1000;
    
    let start_time: Instant = Instant::now();
    db.run_queries(depth, read_ops, write_ops, use_index, num_docs, &measurement_name)
        .await?;

    let duration = start_time.elapsed().as_millis();
    Ok(duration)
}

pub async fn measure_idle_energy_consumption(epoch: u8) -> Result<u128, Box<dyn Error>> {
    let start_time: Instant = Instant::now();
    let name = format!("Control Test C1 epoch {}.csv", epoch);
    let mut pcm_process = start_power_monitoring(&name);
    sleep(Duration::from_secs(1800)).await;
    stop_power_monitoring(&mut pcm_process);

    let duration = start_time.elapsed().as_millis();
    Ok(duration)
}

pub async fn clean_and_insert_data(db: &mut dyn Database, depth: u8) -> Result<(), Box<dyn Error>> {
    let test_data = load_test_data(depth);
    db.connect().await?;
    db.clean_database().await?;
    db.insert_test_data(1000, test_data).await?;
    Ok(())
}

fn load_test_data(depth: u8) -> Vec<Value> {
    let data = fs::read_to_string(format!("src/test_data/data_depth_{}.json", depth)).unwrap();
    serde_json::from_str(&data).unwrap()
}
