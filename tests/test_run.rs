mod memory_stream;

use crate::memory_stream::MemoryStreamFactory;
use litedb::bson;
use litedb::expression::BsonExpression;
use std::sync::{Arc, Mutex};
use litedb::engine::BsonAutoId;

fn new_database_buffer() -> Arc<Mutex<Vec<u8>>> {
    let data = include_bytes!("vcc.liteDb");
    Arc::new(Mutex::new(Vec::from(data)))
}

async fn open_database(data: &Arc<Mutex<Vec<u8>>>) -> litedb::engine::LiteEngine {
    let main = MemoryStreamFactory::with_data(data.clone());
    let log = MemoryStreamFactory::absent();
    let temp = MemoryStreamFactory::absent();

    let settings = litedb::engine::LiteSettings {
        data_stream: Box::new(main),
        log_stream: Box::new(log),
        temp_stream: Box::new(temp),
        auto_build: false,
        collation: None,
    };

    litedb::engine::LiteEngine::new(settings).await.unwrap()
}

#[tokio::test]
async fn run_test() {
    let buffer = new_database_buffer();
    let engine = open_database(&buffer).await;

    println!("collections: {:?}", engine.get_collection_names());

    engine.drop_collection("projects").await.unwrap();

    println!(
        "collections after drop: {:?}",
        engine.get_collection_names()
    );

    let deleted = engine
        .delete(
            "unityVersions",
            &[
                bson::ObjectId::from_bytes(*b"\x66\x33\xbc\x66\x8a\x6a\x1d\x23\x2a\xb0\x13\x71")
                    .into(),
            ],
        )
        .await
        .unwrap();
    println!("deleted: {deleted}");

    engine
        .ensure_index(
            "unityVersions",
            "path",
            BsonExpression::create("Path").unwrap(),
            false,
        )
        .await
        .unwrap();

    engine.drop_index("unityVersions", "Version").await.unwrap();

    engine.insert("unityVersions", {
        let mut doc = bson::Document::new();

        doc.insert("Path".into(), "/Applications/Unity/Hub/Editor/2022.3.49f1_arm64/Unity.app/Contents/MacOS/Unity");
        doc.insert("Version".into(), "2022.3.49f1");
        doc.insert("LoadedFromHub".into(), true);

        vec![doc]
    }, BsonAutoId::ObjectId).await.unwrap();

    engine.checkpoint().await.unwrap();

    engine.dispose().await.unwrap();

    if cfg!(not(miri)) {
        std::fs::write("./tests/vcc.test.liteDb", &*buffer.lock().unwrap()).unwrap();
    }
}
