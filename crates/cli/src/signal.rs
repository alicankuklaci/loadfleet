//! Graceful shutdown on Ctrl-C and on SIGTERM (`docker stop`, systemd, launchd), so a
//! container's PID 1 or a service stops the same way as an interactive Ctrl-C.
use std::future::Future;

/// Resolves on the first Ctrl-C or SIGTERM. The SIGTERM handler is installed when this is
/// called (not on first poll), so a signal sent right after cannot kill the process.
pub fn shutdown() -> impl Future<Output = ()> {
    #[cfg(unix)]
    let term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
    async move {
        let ctrl_c = async {
            // no handler could be installed: never fire rather than stop at once
            if tokio::signal::ctrl_c().await.is_err() {
                std::future::pending::<()>().await;
            }
        };
        #[cfg(unix)]
        {
            let terminate = async move {
                match term {
                    Some(mut t) => {
                        t.recv().await;
                    }
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::select! {
                _ = ctrl_c => {}
                _ = terminate => {}
            }
        }
        #[cfg(not(unix))]
        ctrl_c.await;
    }
}
