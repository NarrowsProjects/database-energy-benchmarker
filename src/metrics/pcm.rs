use std::{path::Path, process::{Child, Command, Stdio}};
use fs_extra::dir::create_all;

pub fn stop_power_monitoring(pcm_process: &mut Child) {
    pcm_process.kill().expect("Failed to kill PCM");
    let _ = pcm_process.wait();
}

pub fn start_power_monitoring(file_name: &str) -> Child {
    let project_root = env!("CARGO_MANIFEST_DIR");
    let file_dir = format!("{}/src/energy_benchmarks/", project_root);
    
    if !Path::new(&file_dir).exists() {
        let _ = create_all(&file_dir, false);
    }
    
    let file_path = format!("{}{}", file_dir, file_name);
    
    let pcm_process = Command::new("pcm")
    .arg("0.1")
    .arg("-r")
    .arg("-silent")
    .arg(format!("-csv=\"{}\"", file_path))
    .stdout(Stdio::piped())
    .spawn()
    .expect("Failed to run Intel PCM command");

pcm_process
}
