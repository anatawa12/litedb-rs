use vrc_get_litedb::bson::Value;
use vrc_get_litedb::document;
use vrc_get_litedb::file_io::{BsonAutoId, LiteDBFile};

#[test]
fn issue_8() {
    let data = include_bytes!("issue_8.liteDb");
    let file = LiteDBFile::parse(data).unwrap();

    assert_eq!(
        file.get_by_index("test", "_id", &Value::Int32(0))
            .collect::<Vec<_>>()
            .len(),
        1
    );
    assert_eq!(
        file.get_by_index("test", "_id", &Value::Int32(1))
            .collect::<Vec<_>>()
            .len(),
        1
    );
    assert_eq!(
        file.get_by_index("test", "_id", &Value::Int32(2))
            .collect::<Vec<_>>()
            .len(),
        1
    );
    assert_eq!(
        file.get_by_index("test", "_id", &Value::Int32(3))
            .collect::<Vec<_>>()
            .len(),
        1
    );
}

#[test]
fn issue_8_insert() {
    let data = include_bytes!("issue_8.liteDb");
    let mut file = LiteDBFile::parse(data).unwrap();

    file.insert("test", vec![document! {"_id" => 1.5}], BsonAutoId::ObjectId)
        .unwrap();
}
