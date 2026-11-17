use super::StateRuntime;
use super::peer::PeerRoute;
use super::peer::PeerTask;
use codex_utils_absolute_path::test_support::PathExt;

async fn runtime() -> (tempfile::TempDir, std::sync::Arc<StateRuntime>) {
    let home = tempfile::tempdir().unwrap();
    let db = StateRuntime::init(
        crate::SqliteConfig::new_for_testing(home.path().abs()),
        "test".into(),
    )
    .await
    .unwrap();
    (home, db)
}

fn task() -> PeerTask {
    let route = PeerRoute {
        product: "codex".into(),
        session_id: "existing".into(),
        data_context: "context".into(),
        locator: "/local/socket".into(),
    };
    PeerTask {
        receipt: "short".into(),
        source: route.clone(),
        target: route,
        message_id: "input".into(),
        message: "task".into(),
        admission: "prepared".into(),
        state: "open".into(),
        replies: Vec::new(),
    }
}

#[tokio::test]
async fn peer_replies_require_admission_and_close_only_on_explicit_final() {
    let (_home, db) = runtime().await;
    db.peer_prepare(&task()).await.unwrap();
    assert!(db.peer_append("short", "too early", false).await.is_err());
    db.peer_admission("short", "accepted").await.unwrap();
    let (_, first) = db.peer_append("short", "progress", false).await.unwrap();
    let (_, replay) = db.peer_append("short", "progress", false).await.unwrap();
    assert_eq!(first.message_id, replay.message_id);
    db.peer_delivered("short", first.sequence).await.unwrap();
    let (_, another) = db.peer_append("short", "progress", false).await.unwrap();
    assert_eq!(another.sequence, 2);
    let (closed, final_reply) = db.peer_append("short", "done", true).await.unwrap();
    assert_eq!(closed.state, "closed");
    assert_eq!(final_reply.sequence, 3);
    assert!(db.peer_append("short", "late", false).await.is_err());
    let (_, replay_final) = db.peer_append("short", "done", true).await.unwrap();
    assert_eq!(replay_final.message_id, final_reply.message_id);
}

#[tokio::test]
async fn peer_final_and_records_survive_reopening_the_database() {
    let (_home, db) = runtime().await;
    let identity = db.peer_context().await.unwrap();
    db.peer_prepare(&task()).await.unwrap();
    db.peer_admission("short", "accepted").await.unwrap();
    let (_, final_reply) = db.peer_append("short", "done", true).await.unwrap();
    let reopened = StateRuntime::init(db.sqlite.clone(), "test".into())
        .await
        .unwrap();
    assert_eq!(reopened.peer_context().await.unwrap(), identity);
    let (_, replay) = reopened.peer_append("short", "done", true).await.unwrap();
    assert_eq!(replay.message_id, final_reply.message_id);
    reopened
        .peer_delivered("short", replay.sequence)
        .await
        .unwrap();
    assert!(reopened.peer_append("short", "done", true).await.is_err());
    assert_eq!(reopened.peer_task("short").await.unwrap().replies.len(), 1);
}

#[tokio::test]
async fn peer_concurrent_final_updates_save_one_reply() {
    let (_home, db) = runtime().await;
    db.peer_prepare(&task()).await.unwrap();
    db.peer_admission("short", "accepted").await.unwrap();
    let (first, second) = tokio::join!(
        db.peer_append("short", "first", true),
        db.peer_append("short", "second", true)
    );
    assert_ne!(first.is_ok(), second.is_ok());
    let stored = db.peer_task("short").await.unwrap();
    assert_eq!(stored.replies.len(), 1);
    assert_eq!(stored.state, "closed");
}
