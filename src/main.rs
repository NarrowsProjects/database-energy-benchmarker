use std::error::Error;

use energy_benchmark::databases::database::Database;
use energy_benchmark::harness::{
    clean_and_insert_data, measure_idle_energy_consumption, run_read_heavy, run_write_heavy,
};
use energy_benchmark::mongodb::MongoDB;
use energy_benchmark::postgres::PostgreSQL;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Main started");
    let mut databases: Vec<Box<dyn Database>> =
        vec![Box::new(MongoDB::new()), Box::new(PostgreSQL::new())];

    println!("Databases initialized");

    let depths = [3, 5, 10];
    let epochs = [1, 2, 3];
    let num_docs = 3000;

    for epoch in epochs {
        println!("Control Test C1");
        measure_idle_energy_consumption(epoch).await?;
        for db in databases.iter_mut() {
            for depth in &depths {
                clean_and_insert_data(db.as_mut(), *depth).await?;
                let duration = run_read_heavy(db.as_ref(), *depth, false, num_docs, epoch).await?;
                println!(
                    "Test 1A - {} Depth {}: Epoch: {} {}ms",
                    db.name(),
                    depth,
                    epoch,
                    duration
                );

                let duration: u128 =
                    run_write_heavy(db.as_ref(), *depth, false, num_docs, epoch).await?;
                println!(
                    "Test 1B - {} Depth {}: Epoch: {} {}ms",
                    db.name(),
                    depth,
                    epoch,
                    duration
                );

                clean_and_insert_data(db.as_mut(), *depth).await?;
                let duration = run_read_heavy(db.as_ref(), *depth, true, num_docs, epoch).await?;
                println!(
                    "Test 2A - {} Depth {}: Epoch: {} {}ms",
                    db.name(),
                    depth,
                    epoch,
                    duration
                );

                let duration = run_write_heavy(db.as_ref(), *depth, true, num_docs, epoch).await?;
                println!(
                    "Test 2B - {} Depth {}: Epoch: {} {}ms",
                    db.name(),
                    depth,
                    epoch,
                    duration
                );
            }
            db.disconnect().await?;
        }
    }
    Ok(())
}
