use crate::bson::BsonWriter;

pub(super) fn dec_len(u: usize) -> usize {
    // noinspection RsAssertEqual
    static MAX_LENS: &[usize] = {
        if cfg!(target_pointer_width = "64") {
            assert!(usize::MAX as u128 == u64::MAX as u128);
            assert!(9999999999999999999u128 < usize::MAX as u128, "");
            assert!((usize::MAX as u128) < 99999999999999999999u128, "");
            &[
                9,
                99,
                999,
                9999,
                99999,
                999999,
                9999999,
                99999999,
                999999999,
                9999999999,
                99999999999,
                999999999999,
                9999999999999,
                99999999999999,
                999999999999999,
                9999999999999999,
                99999999999999999,
                999999999999999999,
                9999999999999999999,
                usize::MAX,
            ]
        } else if cfg!(target_pointer_width = "32") {
            assert!(usize::MAX as u128 == u32::MAX as u128);
            assert!(999999999u128 < usize::MAX as u128, "");
            assert!((usize::MAX as u128) < 9999999999u128, "");
            &[
                9,
                99,
                999,
                9999,
                99999,
                999999,
                9999999,
                99999999,
                999999999,
                usize::MAX,
            ]
        } else {
            panic!("unsupported pointer width");
        }
    };

    let mut i = 0;

    loop {
        let max = MAX_LENS[i];
        if u <= max {
            return i + 1;
        }
        i += 1;
    }
}

#[test]
fn dec_len_test() {
    assert_eq!(dec_len(0), 1);
    assert_eq!(dec_len(9), 1);
    assert_eq!(dec_len(10), 2);
    assert_eq!(dec_len(20), 2);
    assert_eq!(dec_len(4294967296), 10);
    if cfg!(target_pointer_width = "64") {
        assert_eq!(dec_len(18446744073709551615), 20);
    }
}

pub(super) fn write_c_string<W: BsonWriter>(w: &mut W, s: &str) -> Result<(), W::Error> {
    w.write_bytes(s.as_bytes())?;
    w.write_bytes(&[0])?;
    Ok(())
}
