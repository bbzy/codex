use super::Operation;
use super::PeerCommand;
use super::Receipt;
use super::blockers;
use super::dispatch;
use super::outcome;
use super::rpc;
use anyhow::Context;
use anyhow::Result;
use clap::Parser;
use codex_app_server_client::RemoteAppServerClient;
use codex_app_server_client::RemoteAppServerConnectArgs;
use codex_app_server_client::RemoteAppServerEndpoint;
use codex_app_server_client::app_server_control_socket_path;
use codex_app_server_protocol::ThreadActiveFlag;
use codex_app_server_protocol::ThreadStatus;
use codex_app_server_protocol::TurnStatus;
use serde_json::Value;
use serde_json::json;
use std::time::Duration;

#[test]
fn receipt_is_versioned_and_bound_to_the_local_service() {
    let receipt = Receipt {
        version: 1,
        service: "unix:/tmp/service".into(),
        session_id: "existing".into(),
        message_id: "request".into(),
    };
    let encoded = receipt.encode().unwrap();
    assert_eq!(
        Receipt::decode(&encoded, &receipt.service)
            .unwrap()
            .message_id,
        "request"
    );
    assert!(Receipt::decode(&encoded, "unix:/tmp/another").is_err());
    assert!(Receipt::decode("broken", &receipt.service).is_err());
}

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
fn cli_requires_existing_session_ids_and_supports_queue_wait() {
    assert!(PeerCommand::try_parse_from(["peer", "queue", "--message", "task"]).is_err());
    assert!(PeerCommand::try_parse_from(["peer", "wait", "--session", "id"]).is_err());
    let command = PeerCommand::try_parse_from([
        "peer",
        "queue",
        "--session",
        "id",
        "--message",
        "task",
        "--wait",
        "--resume",
    ])
    .unwrap();
    assert!(matches!(
        command.command,
        Operation::Queue {
            wait: true,
            resume: true,
            timeout: 60,
            ..
        }
    ));
    assert_eq!(
        PeerCommand::try_parse_from(["peer", "status", "--session", "id"])
            .unwrap()
            .command
            .name(),
        "status"
    );
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
        vec!["peer", "list", "--dir", "目录 空格/", "--cursor", "next"],
    ] {
        let command = PeerCommand::try_parse_from(arguments)?;
        let result = dispatch(&mut client, "test", command.command).await?;
        assert_eq!(result["nextCursor"], "next");
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
fn shared_contract_covers_normalized_status_and_wait_results() {
    let contract: Value =
        serde_json::from_str(include_str!("../../../docs/fixtures/peer-v1.json")).unwrap();
    assert_eq!(contract["version"], 1);
    assert_eq!(
        contract["states"],
        json!(["idle", "running", "blocked", "not_loaded", "archived"])
    );
    assert_eq!(
        contract["blocked"]["blocked"]["reason"],
        blockers(&ThreadStatus::Active {
            active_flags: vec![ThreadActiveFlag::WaitingOnApproval]
        })[0]["reason"]
    );
    assert!(
        contract["waitResults"]
            .as_array()
            .unwrap()
            .contains(&json!("waiting_interrupted"))
    );
}

#[tokio::test]
async fn queue_wait_uses_the_admitted_client_id_and_returns_reply_without_a_read_call() -> Result<()>
{
    use futures::SinkExt;
    use futures::StreamExt;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use tokio_tungstenite::tungstenite::Message;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("ws://{}", listener.local_addr()?);
    let calls = Arc::new(Mutex::new(Vec::<String>::new()));
    let recorded = calls.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let mut websocket = tokio_tungstenite::accept_async(stream).await?;
        let mut message_id = String::new();
        while let Some(frame) = websocket.next().await {
            let Message::Text(text) = frame? else {
                continue;
            };
            let request: Value = serde_json::from_str(&text)?;
            let Some(id) = request.get("id") else {
                continue;
            };
            let method = request["method"].as_str().unwrap();
            recorded.lock().await.push(method.to_string());
            let thread = json!({"id":"existing","sessionId":"existing","preview":"","ephemeral":false,
                "modelProvider":"test","createdAt":0,"updatedAt":1,"status":{"type":"active","activeFlags":[]},
                "cwd":"/tmp","cliVersion":"test","source":"cli","turns":[]});
            let user = json!({"turnId":"own","item":{"type":"userMessage","id":"user","clientId":message_id,"content":[{"type":"text","text":"task","text_elements":[]}]},"startedAtMs":null,"completedAtMs":null});
            let assistant = json!({"turnId":"own","item":{"type":"agentMessage","id":"answer","text":"reply to task"},"startedAtMs":null,"completedAtMs":null});
            let result = match method {
                "initialize" => json!({"userAgent":"test/1"}),
                "thread/read" => json!({"thread":thread}),
                "thread/list" => json!({"data":[],"nextCursor":null}),
                "thread/queue/list" => json!({"data":[],"nextCursor":null}),
                "thread/turns/list" => {
                    json!({"data":[{"id":"own","items":[],"status":"completed"}],"nextCursor":null})
                }
                "thread/items/list" => {
                    let data = if request["params"]["sortDirection"] == "desc" {
                        vec![assistant, user]
                    } else {
                        vec![user, assistant]
                    };
                    json!({"data":data,"nextCursor":null})
                }
                "thread/queue/add" => {
                    message_id = request["params"]["clientUserMessageId"]
                        .as_str()
                        .unwrap()
                        .to_string();
                    json!({"queuedSubmission":{"id":"queued","clientUserMessageId":message_id,"input":request["params"]["input"]}})
                }
                _ => anyhow::bail!("unexpected operation: {method}"),
            };
            websocket
                .send(Message::Text(
                    json!({"jsonrpc":"2.0","id":id,"result":result})
                        .to_string()
                        .into(),
                ))
                .await?;
        }
        Ok::<(), anyhow::Error>(())
    });
    let mut client = RemoteAppServerClient::connect(RemoteAppServerConnectArgs {
        endpoint: RemoteAppServerEndpoint::WebSocket {
            websocket_url: endpoint,
            auth_token: None,
        },
        client_name: "peer_test".into(),
        client_version: "1".into(),
        experimental_api: true,
        required_human_approval: false,
        mcp_server_openai_form_elicitation: false,
        opt_out_notification_methods: vec![],
        channel_capacity: 128,
    })
    .await?;
    let output = dispatch(
        &mut client,
        "test-service",
        Operation::Queue {
            session: "existing".into(),
            message: "task".into(),
            resume: false,
            wait: true,
            timeout: 0,
        },
    )
    .await?;
    assert_eq!(output["result"], "completed");
    assert_eq!(output["messages"][0]["text"], "reply to task");
    assert_eq!(output["status"]["state"], "running");
    assert!(output["receipt"].as_str().is_some());
    assert!(
        !calls
            .lock()
            .await
            .iter()
            .any(|method| method == "thread/start"
                || method == "thread/resume"
                || method.contains("approval"))
    );
    client.shutdown().await?;
    server.abort();
    Ok(())
}

#[tokio::test]
#[ignore = "Requires built CLI; run by the peer-runtime Nushell driver"]
async fn peer_cli_roundtrip_against_an_isolated_local_service() -> Result<()> {
    use app_test_support::MockResponsesConfig;
    use app_test_support::create_escalated_command_execution_sse_response;
    use app_test_support::create_final_assistant_message_sse_response;
    use app_test_support::create_mock_responses_server_sequence;
    use codex_app_server_protocol::ThreadStartResponse;
    use std::process::Stdio;
    use tempfile::TempDir;
    use tokio::process::Command;

    let binary = std::env::var("PEER_TEST_BINARY")?;
    let home = TempDir::new()?;
    let model = create_mock_responses_server_sequence(vec![
        create_final_assistant_message_sse_response("second peer answer")?,
        create_escalated_command_execution_sse_response(
            vec!["echo".into(), "approval-test".into()],
            None,
            Some(10_000),
            "peer-approval",
        )?,
    ])
    .await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path_regex(".*/responses$"))
        .respond_with(
            wiremock::ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(create_final_assistant_message_sse_response(
                    "first peer answer",
                )?)
                .set_delay(Duration::from_secs(3)),
        )
        .with_priority(1)
        .up_to_n_times(1)
        .expect(1)
        .mount(&model)
        .await;
    MockResponsesConfig::new(&model.uri())
        .with_approval_policy("on-request")
        .with_root_config("approvals_reviewer = \"user\"")
        .write(home.path())?;
    let socket = app_server_control_socket_path(home.path())?;
    let logs = std::fs::File::create(home.path().join("server.log"))?;
    let mut server = Command::new(&binary)
        .env("CODEX_HOME", home.path())
        .env("HOME", home.path())
        .current_dir(home.path())
        .args([
            "app-server",
            "--listen",
            &format!("unix://{}", socket.display()),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(logs)
        .kill_on_drop(true)
        .spawn()?;
    let connect = || {
        RemoteAppServerClient::connect(RemoteAppServerConnectArgs {
            endpoint: RemoteAppServerEndpoint::UnixSocket {
                socket_path: socket.clone(),
            },
            client_name: "peer_smoke".into(),
            client_version: "1".into(),
            experimental_api: true,
            required_human_approval: false,
            mcp_server_openai_form_elicitation: false,
            opt_out_notification_methods: vec![],
            channel_capacity: 128,
        })
    };
    let controller = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Ok(client) = connect().await {
                break Ok::<_, anyhow::Error>(client);
            }
            if let Some(status) = server.try_wait()? {
                anyhow::bail!(
                    "test service exited {status}: {}",
                    std::fs::read_to_string(home.path().join("server.log"))?
                );
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    let thread: ThreadStartResponse = rpc(
        &controller,
        "thread/start",
        json!({"cwd":home.path(),"ephemeral":false}),
    )
    .await?;
    let session = thread.thread.id;
    let _: Value = rpc(
        &controller,
        "thread/name/set",
        json!({"threadId":session,"name":"named-empty-peer"}),
    )
    .await?;
    let invoke = |args: Vec<String>| {
        let mut command = Command::new(&binary);
        command
            .env("CODEX_HOME", home.path())
            .env("HOME", home.path())
            .current_dir(home.path());
        command.arg("peer").args(args);
        async move {
            let output = tokio::time::timeout(Duration::from_secs(20), command.output()).await??;
            let value: Value = serde_json::from_slice(&output.stdout).with_context(|| {
                format!(
                    "stdout={}, stderr={}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                )
            })?;
            assert!(output.status.success(), "{value}");
            Ok::<_, anyhow::Error>(value)
        }
    };
    let listed = invoke(vec!["list".into()]).await?;
    assert!(
        listed["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["sessionID"] == session && row["title"] == "named-empty-peer"),
        "{listed}"
    );
    let child_directory = home.path().join("目录 空格");
    std::fs::create_dir_all(&child_directory)?;
    let child: ThreadStartResponse = rpc(
        &controller,
        "thread/start",
        json!({"cwd":child_directory,"ephemeral":false}),
    )
    .await?;
    let _: Value = rpc(
        &controller,
        "thread/name/set",
        json!({"threadId":child.thread.id,"name":"child peer"}),
    )
    .await?;
    let current = invoke(vec!["list".into(), "--cur-dir".into()]).await?;
    assert_eq!(
        current["sessions"].as_array().unwrap().len(),
        1,
        "{current}"
    );
    assert_eq!(current["sessions"][0]["sessionID"], session);
    let child_list = invoke(vec!["list".into(), "--dir".into(), "./目录 空格/".into()]).await?;
    assert_eq!(
        child_list["sessions"].as_array().unwrap().len(),
        1,
        "{child_list}"
    );
    assert_eq!(child_list["sessions"][0]["sessionID"], child.thread.id);
    assert_eq!(
        invoke(vec!["list".into(), "--dir".into(), "missing".into()]).await?["sessions"],
        json!([])
    );
    let admitted = invoke(vec![
        "queue".into(),
        "--session".into(),
        session.clone(),
        "--message".into(),
        "first".into(),
    ])
    .await?;
    let waiting = Command::new(&binary)
        .env("CODEX_HOME", home.path())
        .env("HOME", home.path())
        .current_dir(home.path())
        .args([
            "peer",
            "wait",
            "--receipt",
            admitted["receipt"].as_str().unwrap(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let signal = Command::new("kill")
        .args([
            "-INT",
            &waiting.id().context("wait process exited")?.to_string(),
        ])
        .status()
        .await?;
    assert!(signal.success());
    let interrupted =
        tokio::time::timeout(Duration::from_secs(5), waiting.wait_with_output()).await??;
    let interrupted: Value = serde_json::from_slice(&interrupted.stdout)?;
    assert_eq!(
        interrupted["result"], "waiting_interrupted",
        "{interrupted}"
    );
    assert_eq!(interrupted["receipt"], admitted["receipt"]);
    let first = invoke(vec![
        "wait".into(),
        "--receipt".into(),
        admitted["receipt"].as_str().unwrap().into(),
        "--timeout".into(),
        "10".into(),
    ])
    .await?;
    assert_eq!(first["result"], "completed", "{first}");
    assert!(
        first["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|message| message["text"] == "first peer answer"),
        "{first}"
    );
    let second = invoke(vec![
        "queue".into(),
        "--session".into(),
        session.clone(),
        "--message".into(),
        "follow-up".into(),
        "--wait".into(),
    ])
    .await?;
    assert_eq!(second["result"], "completed", "{second}");
    assert!(
        second["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|message| message["text"] == "second peer answer")
    );
    let again = invoke(vec![
        "wait".into(),
        "--receipt".into(),
        first["receipt"].as_str().unwrap().into(),
        "--timeout".into(),
        "0".into(),
    ])
    .await?;
    assert!(
        again["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|message| message["text"] == "first peer answer")
    );
    let blocked = invoke(vec![
        "queue".into(),
        "--session".into(),
        session.clone(),
        "--message".into(),
        "ask approval".into(),
        "--wait".into(),
    ])
    .await?;
    assert_eq!(blocked["result"], "blocked", "{blocked}");
    let snapshot = invoke(vec!["status".into(), "--session".into(), session]).await?;
    assert_eq!(snapshot["status"]["state"], "blocked", "{snapshot}");
    assert_eq!(snapshot["status"]["blocked"]["reason"], "permission");
    controller.shutdown().await?;
    server.kill().await?;
    server.wait().await?;
    Ok(())
}
