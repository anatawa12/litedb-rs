mod memory_stream;

use futures::prelude::*;
use std::sync::{Arc, Mutex};
use vrc_get_litedb::bson;
use vrc_get_litedb::expression::BsonExpression;
use vrc_get_litedb::file_io::{BsonAutoId, LiteDBFile, Order};

#[test]
fn run_test() {
    let data = include_bytes!("vcc.liteDb");
    let mut engine = LiteDBFile::parse(data).unwrap();

    println!("collections: {:?}", engine.get_collection_names());

    engine.drop_collection("projects");

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
        );
    println!("deleted: {deleted}");

    engine
        .ensure_index(
            "unityVersions",
            "path",
            BsonExpression::create("Path").unwrap(),
            false,
        )
        .unwrap();

    engine
        .ensure_index(
            "unityVersions",
            "version",
            BsonExpression::create("Version").unwrap(),
            false,
        )
        .unwrap();

    engine.drop_index("unityVersions", "Version");

    engine.insert("unityVersions", {
        let mut doc = bson::Document::new();

        doc.insert("Path", "/Applications/Unity/Hub/Editor/2022.3.49f1_arm64/Unity.app/Contents/MacOS/Unity");
        doc.insert("Version", "2022.3.49f1");
        doc.insert("LoadedFromHub", false);

        vec![doc]
    }, BsonAutoId::ObjectId).unwrap();

    let updated = engine
        .update("unityVersions", {
            let mut doc = bson::Document::new();

            doc.insert(
                "_id",
                bson::ObjectId::from_bytes(
                    hex::decode("668e1f8a7a74cbd413470ad2")
                        .unwrap()
                        .try_into()
                        .unwrap(),
                ),
            );
            doc.insert(
                "Path",
                "/Applications/Unity/Hub/Editor/2022.3.6f1/Unity.app/Contents/MacOS/Unity",
            );
            doc.insert("Version", "2022.3.6f1");
            doc.insert("LoadedFromHub", false);

            vec![doc]
        })
        .unwrap();

    println!("updated {updated}");

    let inserted = engine.upsert("unityVersions", {
        let mut doc1 = bson::Document::new();

        doc1.insert("Path", "/Applications/Unity/Hub/Editor/6000.0.0b12-x86_64/Unity.app/Contents/MacOS/Unity");
        doc1.insert("Version", "6000.0.0b12");
        doc1.insert("LoadedFromHub", false);


        let mut doc2 = bson::Document::new();

        doc2.insert(
            "_id",
            bson::ObjectId::from_bytes(
                hex::decode("66475280c17fa4fe9f23dd15")
                    .unwrap()
                    .try_into()
                    .unwrap(),
            ),
        );
        doc2.insert(
            "Path",
            "/Applications/Unity/Hub/Editor/6000.0.0b12/Unity.app/Contents/MacOS/Unity",
        );
        doc2.insert("Version", "6000.0.0b12");
        doc2.insert("LoadedFromHub", false);

        vec![doc1, doc2]
    }, BsonAutoId::ObjectId).unwrap();

    println!("upsert  {inserted}");

    println!("get all unityVersions: ");

    engine
        .get_all("unityVersions")
        .for_each(|doc| {
            println!("version: {:?}", doc.get("version"));
            println!("LoadedFromHub: {:?}", doc.get("LoadedFromHub"));
            println!("Path: {:?}", doc.get("Path"));
            println!();
        });

    println!("find by version: ");

    engine
        .get_by_index(
            "unityVersions",
            "version",
            &"2022.3.49f1".to_string().into(),
        )
        .for_each(|doc| {
            println!("version: {:?}", doc.get("version"));
            println!("LoadedFromHub: {:?}", doc.get("LoadedFromHub"));
            println!("Path: {:?}", doc.get("Path"));
            println!();
        });

    println!("find by version range: ");

    engine
        .get_range_indexed(
            "unityVersions",
            "version",
            &"2022".to_string().into(),
            &"2023".to_string().into(),
            Order::Descending,
        )
        .for_each(|doc| {
            println!("version: {:?}", doc.get("version"));
            println!("LoadedFromHub: {:?}", doc.get("LoadedFromHub"));
            println!("Path: {:?}", doc.get("Path"));
            println!();
        });

    /*
    engine.checkpoint().await.unwrap();

    engine.dispose().await.unwrap();
     */

    if cfg!(not(miri)) {
        //std::fs::write("./tests/vcc.test.liteDb", &*buffer.lock().unwrap()).unwrap();
    }
}
