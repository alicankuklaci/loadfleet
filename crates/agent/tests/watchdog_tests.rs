use loadfleet_agent::Watchdog;
use std::time::Duration;

#[tokio::test(start_paused = true)]
async fn watchdog_cancels_after_15s_silence() {
    let mut w = Watchdog::new(Duration::from_secs(15));
    tokio::time::sleep(Duration::from_secs(14)).await;
    assert!(!w.expired());
    w.feed();
    tokio::time::sleep(Duration::from_secs(14)).await;
    assert!(!w.expired());
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(w.expired());
}
