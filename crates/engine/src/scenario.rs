use async_trait::async_trait;
use loadfleet_metrics::VuRecorder;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// One virtual user's context (closed model). The scenario loops until `cancel` fires.
pub struct VuCtx {
    pub vu: u64,
    pub vu_id: String,
    pub rec: VuRecorder,
    pub cancel: CancellationToken,
    pub seed: u64,
}

/// One planned operation (open model). Latency MUST be measured from `scheduled`
/// (coordinated-omission correction). `cancel` fires on Stop or once the end-of-run drain
/// expires; an op should give up (and drop any pooled connection it holds) when it does.
/// Ops that ignore it are aborted shortly after.
pub struct OpCtx {
    pub vu: u64,
    pub vu_id: String,
    pub iter: u64,
    pub scheduled: Instant,
    pub rec: VuRecorder,
    pub cancel: CancellationToken,
}

#[async_trait]
pub trait Scenario: Send + Sync + 'static {
    fn kind(&self) -> &'static str;
    async fn run_vu(&self, ctx: VuCtx);
    fn supports_open(&self) -> bool {
        false
    }
    async fn op(&self, ctx: OpCtx) {
        ctx.rec.error("open:unsupported");
    }
    /// Called once when the run is stopped (Stop / cancel) and once more when `execute` returns,
    /// before VUs have necessarily finished: stop background work (e.g. worker threads) here.
    fn stop(&self) {}
}
