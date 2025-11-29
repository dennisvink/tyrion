use std::fs;
use std::path::PathBuf;
use std::process::Command;

fn tyrion_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_tyrion"))
}

fn example(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join(name)
}

fn run_and_capture(args: &[&str]) -> String {
    let out = Command::new(tyrion_bin())
        .args(args)
        .output()
        .expect("failed to run command");
    assert!(
        out.status.success(),
        "command failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).to_string()
}

#[test]
fn interpreter_runs_examples() {
    let hello_out = run_and_capture(&[example("hello.ty").to_str().unwrap()]);
    assert!(
        hello_out.contains("hello world"),
        "unexpected output: {hello_out}"
    );

    let class_out = run_and_capture(&[example("class.ty").to_str().unwrap()]);
    assert!(
        class_out.contains("Hello Sansa I'm Arya"),
        "unexpected output: {class_out}"
    );
}

#[test]
fn build_and_interpret_outputs_match() {
    let interp_out = run_and_capture(&[example("hello.ty").to_str().unwrap()]);

    let out_bin = unique_temp_path("hello_bin");
    let build_status = Command::new(tyrion_bin())
        .arg("--build")
        .arg(example("hello.ty"))
        .arg("--out")
        .arg(&out_bin)
        .output()
        .expect("failed to build");
    assert!(
        build_status.status.success(),
        "build failed: {}",
        String::from_utf8_lossy(&build_status.stderr)
    );

    let run_out = Command::new(&out_bin)
        .output()
        .expect("failed to run built binary");
    assert!(
        run_out.status.success(),
        "built binary failed: {}",
        String::from_utf8_lossy(&run_out.stderr)
    );
    let built_out = String::from_utf8_lossy(&run_out.stdout).to_string();

    assert_eq!(interp_out, built_out);

    let _ = fs::remove_file(&out_bin);
}

#[test]
fn full_feature_parity() {
    let interp_out = run_and_capture(&[example("full_feature_test.ty").to_str().unwrap()]);

    let out_bin = unique_temp_path("full_feature_bin");
    let build_status = Command::new(tyrion_bin())
        .arg("--build")
        .arg(example("full_feature_test.ty"))
        .arg("--out")
        .arg(&out_bin)
        .output()
        .expect("failed to build");
    assert!(
        build_status.status.success(),
        "build failed: {}",
        String::from_utf8_lossy(&build_status.stderr)
    );

    let run_out = Command::new(&out_bin)
        .output()
        .expect("failed to run built binary");
    assert!(
        run_out.status.success(),
        "built binary failed: {}",
        String::from_utf8_lossy(&run_out.stderr)
    );
    let built_out = String::from_utf8_lossy(&run_out.stdout).to_string();

    assert_eq!(interp_out, built_out);
    let _ = fs::remove_file(out_bin);
}

fn unique_temp_path(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    p.push(format!("{}_{}", name, nanos));
    p
}
