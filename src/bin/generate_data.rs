use energy_benchmark::data_generator;

fn main() {
    let depths = [3, 5, 10];
    let documents_per_depth = 10000;
    
    for depth in depths {
        let _ = data_generator::generate_and_save_data(depth, documents_per_depth);
        println!("Generated {} documents for depth {}", documents_per_depth, depth);
    }
}