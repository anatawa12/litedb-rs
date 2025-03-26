use vrc_get_litedb::file_io::LiteDBFile;

#[test]
fn test_read() {
    let data = include_bytes!("issue_5.liteDb");
    let _ = LiteDBFile::parse(data).unwrap();
}
