fn main() {
    let mut config = prost_build::Config::new();

    // Add Serde support to all generated types
    config.type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");
    // Use camelCase for JSON fields (standard for JS)
    config.type_attribute(".", "#[serde(rename_all = \"camelCase\")]");

    config.compile_protos(&["proto/ego_proc.proto"], &["proto/"])
        .expect("Failed to compile ego_proc.proto");    
}