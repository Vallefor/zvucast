#[tokio::test(flavor = "multi_thread")]
async fn slave_locks_to_master() {
    if std::env::var("ZVUCAST_HW_TS").is_err() {
        eprintln!("пропуск: нужен CAP_SYS_TIME и timestamping (запускать с ZVUCAST_HW_TS=1)");
        return;
    }
    let _m = zsync::start(100, "lo").unwrap();
    let s = zsync::start(128, "lo").unwrap();
    let off = s.offset_ns;
    let mut last = i64::MAX;
    for _ in 0..20 {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let v = *off.borrow();
        if v.abs() < 1_000_000 {
            last = v.abs();
            break;
        } // <1 мс
        last = v.abs();
    }
    assert!(last < 1_000_000, "offset не сошёлся: {last} ns");
}
