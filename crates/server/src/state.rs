use crate::{Coordinator, Hub, ServerConfig, Store};
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub cfg: Arc<ServerConfig>,
    pub store: Store,
    pub hub: Hub,
    pub coord: Coordinator,
    pub auth: Arc<crate::auth::Auth>,
}
