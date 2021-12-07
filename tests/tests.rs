use bytes::Bytes;
use minidb_rs::*;
use rand::Rng;

#[test]
fn test_open() {
    let db = MiniDB::new("testdata/minidb").unwrap();
    db.remove().unwrap();
}

fn put(db: &mut MiniDB) {
    let mut rng = rand::thread_rng();

    for i in 0..10000 {
        let key = Bytes::from(format!("test_key_{}", i % 5));
        let val = Bytes::from(format!("test_val_{}", rng.gen::<u64>()));
        db.put(key, val).unwrap();
    }
}

#[test]
fn test_put() {
    let mut db = MiniDB::new("testdata/minidb").unwrap();
    put(&mut db);
    db.remove().unwrap();
}

#[test]
fn test_get() {
    let mut db = MiniDB::new("testdata/minidb").unwrap();
    put(&mut db);
    for i in 0..5 {
        let key = Bytes::from(format!("test_key_{}", i));
        let _value = db.get(key).unwrap();
    }
    db.remove().unwrap();
}

#[test]
fn test_delete() {
    let mut db = MiniDB::new("testdata/minidb").unwrap();
    put(&mut db);

    db.delete(Bytes::from(format!("test_key_{}", 1))).unwrap();
    db.delete(Bytes::from(format!("test_key_{}", 2))).unwrap();
    db.delete(Bytes::from(format!("test_key_{}", 3))).unwrap();

    assert!(db.delete(Bytes::from(format!("test_key_{}", 7))).is_err());
    assert!(db.delete(Bytes::from(format!("test_key_{}", 8))).is_err());
    assert!(db.delete(Bytes::from(format!("test_key_{}", 9))).is_err());
}

#[test]
fn test_merge() {
    let mut db = MiniDB::new("testdata/minidb").unwrap();
    put(&mut db);

    db.merge().unwrap();
}
