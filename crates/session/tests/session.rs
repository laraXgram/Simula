//! A client session over a real WebSocket: calls, errors, and which session gets which
//! updates.

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use simula_core::services::accounts::{self, NewBot, NewUser};
use simula_core::{Account, Caller, Peer, Sandbox, SandboxConfig};
use simula_tl::functions;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn start() -> (Arc<Sandbox>, Account, Account, String) {
    let sandbox = Sandbox::open(SandboxConfig {
        id: "test".into(),
        path: None,
    })
    .unwrap();
    let user = sandbox
        .admin(|ctx| {
            accounts::create_user(
                ctx,
                NewUser {
                    first_name: "Alice".into(),
                    ..Default::default()
                },
            )
        })
        .await
        .unwrap();
    let bot = sandbox
        .admin(|ctx| {
            accounts::create_bot(
                ctx,
                NewBot {
                    first_name: "Echo".into(),
                    username: "echo_bot".into(),
                    ..Default::default()
                },
            )
        })
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = simula_session::router(sandbox.clone());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (sandbox, user, bot, format!("ws://{address}/session"))
}

async fn connect(url: &str, account: &Account) -> Socket {
    let (mut socket, _) =
        tokio_tungstenite::connect_async(format!("{url}?account={}", account.id.0))
            .await
            .unwrap();
    let hello = next(&mut socket).await;
    assert_eq!(hello["session"]["account"], account.id.0.to_string());
    socket
}

async fn next(socket: &mut Socket) -> Value {
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(2), socket.next())
            .await
            .expect("a frame in time")
            .unwrap()
            .unwrap();
        if let Message::Text(text) = frame {
            return serde_json::from_str(&text).unwrap();
        }
    }
}

async fn call(socket: &mut Socket, id: u32, function: Value) -> Value {
    socket
        .send(Message::Text(
            json!({ "id": id, "call": function }).to_string().into(),
        ))
        .await
        .unwrap();
    loop {
        let frame = next(socket).await;
        if frame["id"] == id {
            return frame;
        }
    }
}

#[tokio::test]
async fn calls_errors_and_updates_per_session() {
    let (sandbox, user, bot, url) = start().await;
    let mut phone = connect(&url, &user).await;
    let mut laptop = connect(&url, &user).await;

    let resolved = call(
        &mut phone,
        1,
        json!({ "_": "contacts.resolveUsername", "username": "echo_bot" }),
    )
    .await;
    let found = &resolved["result"]["users"][0];
    assert_eq!(found["username"], "echo_bot");

    let peer = json!({ "_": "inputPeerUser", "user_id": found["id"], "access_hash": found["access_hash"] });
    let sent = call(
        &mut phone,
        2,
        json!({ "_": "messages.sendMessage", "peer": peer, "message": "hi from the phone", "random_id": "7" }),
    )
    .await;
    assert_eq!(sent["result"]["updates"][0]["_"], "updateMessageID");

    // The other session of the same user sees the message as an update.
    let pushed = next(&mut laptop).await;
    assert_eq!(pushed["updates"]["updates"][0]["_"], "updateNewMessage");
    assert_eq!(
        pushed["updates"]["updates"][0]["message"]["message"],
        "hi from the phone"
    );

    // The bot answers; both sessions get it.
    sandbox
        .call(
            Caller {
                account: bot.id,
                session: None,
            },
            functions::messages::SendMessage {
                peer: sandbox.input_peer(bot.id, Peer::User(user.id)),
                message: "hello!".into(),
                random_id: 8,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    for socket in [&mut phone, &mut laptop] {
        let pushed = next(socket).await;
        assert_eq!(
            pushed["updates"]["updates"][0]["message"]["message"],
            "hello!"
        );
    }

    let wrong = call(
        &mut phone,
        3,
        json!({ "_": "messages.sendMessage", "peer": { "_": "inputPeerUser", "user_id": "1", "access_hash": "1" },
                "message": "x", "random_id": "9" }),
    )
    .await;
    assert_eq!(
        wrong["error"],
        json!({ "code": 400, "message": "PEER_ID_INVALID" })
    );
    let unknown = call(&mut phone, 4, json!({ "_": "no.suchFunction" })).await;
    assert_eq!(unknown["error"]["code"], 400);
}
