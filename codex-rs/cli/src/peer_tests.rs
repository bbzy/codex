use super::Operation;
use super::PeerCommand;
use super::blockers;
use super::dispatch;
use super::outcome;
use super::run;
use anyhow::Result;
use clap::Parser;
use codex_app_server_client::RemoteAppServerClient;
use codex_app_server_client::RemoteAppServerConnectArgs;
use codex_app_server_client::RemoteAppServerEndpoint;
use codex_app_server_protocol::ThreadActiveFlag;
use codex_app_server_protocol::ThreadStatus;
use codex_app_server_protocol::TurnStatus;
use serde_json::Value;
use serde_json::json;

#[test]
fn blockers_are_explicit_and_permissions_take_precedence() {
    assert!(blockers(&ThreadStatus::Idle).is_empty());
    assert!(
        blockers(&ThreadStatus::Active {
            active_flags: vec![]
        })
        .is_empty()
    );
    let result = blockers(&ThreadStatus::Active {
        active_flags: vec![
            ThreadActiveFlag::WaitingOnUserInput,
            ThreadActiveFlag::WaitingOnApproval,
        ],
    });
    assert_eq!(result[0]["reason"], "permission");
    assert_eq!(result[1]["reason"], "input");
    assert_eq!(outcome(&TurnStatus::InProgress), None);
    assert_eq!(outcome(&TurnStatus::Interrupted), Some("interrupted"));
}

#[test]
fn list_directory_options_are_mutually_exclusive() {
    assert!(PeerCommand::try_parse_from(["peer", "list", "--cur-dir"]).is_ok());
    assert!(PeerCommand::try_parse_from(["peer", "list", "--dir", "../目录 空格/"]).is_ok());
    for arguments in [
        vec!["peer", "list", "--cur-dir", "--dir", "."],
        vec!["peer", "list", "--dir", ".", "--cur-dir"],
    ] {
        assert_eq!(
            PeerCommand::try_parse_from(arguments).unwrap_err().kind(),
            clap::error::ErrorKind::ArgumentConflict
        );
    }
    assert!(PeerCommand::try_parse_from(["peer", "list", "--dir"]).is_err());
}

#[tokio::test]
async fn list_sends_absolute_directory_filters_with_each_page() -> Result<()> {
    use futures::SinkExt;
    use futures::StreamExt;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use tokio_tungstenite::tungstenite::Message;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("ws://{}", listener.local_addr()?);
    let requests = Arc::new(Mutex::new(Vec::<Value>::new()));
    let recorded = requests.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let mut websocket = tokio_tungstenite::accept_async(stream).await?;
        while let Some(frame) = websocket.next().await {
            let Message::Text(text) = frame? else {
                continue;
            };
            let request: Value = serde_json::from_str(&text)?;
            let Some(id) = request.get("id") else {
                continue;
            };
            let result = match request["method"].as_str().unwrap() {
                "initialize" => json!({"userAgent":"test/1"}),
                "peer/context" => {
                    json!({"version":2,"product":"codex","dataContext":"test-context"})
                }
                "thread/list" => {
                    recorded.lock().await.push(request["params"].clone());
                    json!({"data":[], "nextCursor":"next"})
                }
                method => anyhow::bail!("unexpected method: {method}"),
            };
            websocket
                .send(Message::Text(
                    json!({"jsonrpc":"2.0","id":id,"result":result})
                        .to_string()
                        .into(),
                ))
                .await?;
        }
        Ok::<_, anyhow::Error>(())
    });
    let mut client = RemoteAppServerClient::connect(RemoteAppServerConnectArgs {
        endpoint: RemoteAppServerEndpoint::WebSocket {
            websocket_url: endpoint,
            auth_token: None,
        },
        client_name: "peer_list_test".into(),
        client_version: "1".into(),
        experimental_api: true,
        required_human_approval: false,
        mcp_server_openai_form_elicitation: false,
        opt_out_notification_methods: vec![],
        channel_capacity: 128,
    })
    .await?;
    for arguments in [
        vec!["peer", "list"],
        vec!["peer", "list", "--cur-dir"],
        vec!["peer", "list", "--dir", "目录 空格/"],
    ] {
        let command = PeerCommand::try_parse_from(&arguments)?;
        let result = dispatch(&mut client, "test", command.command).await?;
        assert!(result["nextCursor"].is_string());
        if arguments.len() == 4 && arguments[2] == "--dir" {
            let command = PeerCommand::try_parse_from([
                "peer",
                "list",
                "--dir",
                "目录 空格/",
                "--cursor",
                result["nextCursor"].as_str().unwrap(),
            ])?;
            dispatch(&mut client, "test", command.command).await?;
        }
    }
    let requests = requests.lock().await;
    assert!(requests[0]["cwd"].is_null());
    assert_eq!(requests[1]["cwd"], json!(std::env::current_dir()?));
    assert_eq!(
        requests[2]["cwd"],
        json!(std::env::current_dir()?.join("目录 空格"))
    );
    assert_eq!(requests[3]["cwd"], requests[2]["cwd"]);
    assert_eq!(requests[3]["cursor"], "next");
    assert!(requests.iter().all(|request| request["limit"] == 50));
    client.shutdown().await?;
    server.abort();
    Ok(())
}

#[test]
fn peer_targets_are_required_positionals() {
    for operation in ["status", "read", "queue"] {
        let options = if operation == "queue" {
            vec!["--message", "task", "--from", "opencode:source"]
        } else {
            vec![]
        };
        let mut args = vec!["peer", operation];
        args.extend(options.clone());
        assert!(PeerCommand::try_parse_from(args).is_err());

        let mut args = vec!["peer", operation, "--session", "id"];
        args.extend(options.clone());
        assert!(PeerCommand::try_parse_from(args).is_err());

        let mut args = vec!["peer", operation, "id"];
        args.extend(options);
        assert!(PeerCommand::try_parse_from(args).is_ok());
    }
}

#[test]
fn queue_and_reply_are_explicit_async_operations() {
    let queue = PeerCommand::try_parse_from([
        "peer",
        "queue",
        "target",
        "--message",
        "task",
        "--from",
        "opencode:source",
        "--resume",
    ])
    .unwrap();
    assert!(matches!(
        queue.command,
        Operation::Queue { resume: true, .. }
    ));
    let reply = PeerCommand::try_parse_from([
        "peer",
        "reply",
        "--receipt",
        "opaque",
        "--message",
        "done",
        "--final",
    ])
    .unwrap();
    assert!(matches!(
        reply.command,
        Operation::Reply { r#final: true, .. }
    ));
    assert!(PeerCommand::try_parse_from(["peer", "wait", "--receipt", "old"]).is_err());
    assert!(
        PeerCommand::try_parse_from([
            "peer",
            "queue",
            "target",
            "--message",
            "task",
            "--from",
            "codex:source",
            "--wait"
        ])
        .is_err()
    );
}

#[tokio::test]
async fn invalid_arguments_are_reported_before_service_discovery() {
    for arguments in [
        vec![
            "peer",
            "queue",
            "target",
            "--from",
            "other:source",
            "--message",
            "task",
        ],
        vec![
            "peer",
            "queue",
            "target",
            "--from",
            "codex:",
            "--message",
            "task",
        ],
        vec!["peer", "read", " "],
        vec!["peer", "reply", "--receipt", " ", "--message", "reply"],
    ] {
        let result = run(PeerCommand::try_parse_from(arguments).unwrap()).await;
        assert_eq!(result["version"], 2);
        assert_eq!(result["error"]["code"], "invalid_arguments");
    }
}

#[tokio::test]
async fn malformed_cursors_fail_before_service_discovery() {
    for arguments in [
        vec!["peer", "read", "existing", "--cursor", "damaged"],
        vec!["peer", "list", "--cursor", "damaged"],
    ] {
        let result = run(PeerCommand::try_parse_from(arguments).unwrap()).await;
        assert_eq!(result["error"]["code"], "invalid_cursor");
    }
}
