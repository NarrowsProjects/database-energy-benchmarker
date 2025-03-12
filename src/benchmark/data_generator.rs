use fake::faker::lorem::en::Word;
use fake::Fake;
use fs_extra::dir::create_all;
use serde_json::{json, to_string_pretty, Value};
use rand::distributions::{Alphanumeric, DistString};
use std::path::Path;
use std::error::Error;

const WRITE_TARGET_BYTE_SIZE: usize = 16;

pub fn generate_and_save_data(depth: u8, count: usize) -> Result<(), Box<dyn Error>> {
    let data = generate_nested_data(depth, count);
    let json = to_string_pretty(&data)?;

    let project_root = env!("CARGO_MANIFEST_DIR");
    let dir_path = format!("{}/src/test_data", project_root);

    if !Path::new(&dir_path).exists() {
        create_all(&dir_path, false)?;
    }

    let file_path = format!("{}/data_depth_{}.json", &dir_path, depth);
    std::fs::write(file_path, json)?;

    Ok(())
}

fn generate_nested_data(depth: u8, count: usize) -> Value {
    let documents: Vec<Value> = (0..count)
        .map(|i| {
            // Half of the data should be targetable
            let should_use_specific = i % 2 == 0;
            build_level(1, depth, should_use_specific)
        })
        .collect();
    
    json!(documents)
}

fn build_level(current_depth: u8, max_depth: u8, use_specific_value: bool) -> Value {
    if current_depth == max_depth {
        // Leaf node - create object with read_target and write_target
        if use_specific_value {
            json!({
                "read_target": "read_target",
                "write_target": generate_fixed_size_word(WRITE_TARGET_BYTE_SIZE)
            })
        } else {
            json!({
                "read_target": Word().fake::<String>(),
                "write_target": generate_fixed_size_word(WRITE_TARGET_BYTE_SIZE)
            })
        }
    } else {
        let mut object = json!({
            "field": Word().fake::<String>(),
        });

        if current_depth < max_depth {
            object["children"] = json!(vec![build_level(current_depth + 1, max_depth, use_specific_value)]);
        }
        object
    }
}

pub fn generate_fixed_size_word(byte_size: usize) -> String {
    let mut rng = rand::thread_rng();
    
    let mut result = String::new();
    while result.len() < byte_size {
        let remaining = byte_size - result.len();
        let chunk_size = std::cmp::min(remaining, 32); 
        let chunk = Alphanumeric.sample_string(&mut rng, chunk_size);
        result.push_str(&chunk);
    }
    
    if result.len() > byte_size {
        result.truncate(byte_size);
    }
    
    result
}