//! Supergroups and channels as a bot sees them: joining, messages with the shared ids,
//! admin rights, restricting, banning, permissions, and channel posts.

use serde_json::json;
use simula_core::Peer;
use simula_tl::{functions, types};

use crate::support::{
    as_user, bob, create_channel, fails, input_channel, input_user, next_updates, ok,
    says_in_channel, world,
};

#[tokio::test]
async fn a_supergroup_from_joining_to_banning() {
    let w = world().await;
    let bob = bob(&w).await;
    let mut offset = 0;
    let dev = create_channel(&w, false, "Dev").await;
    let chat_id = Peer::Channel(dev).bot_api_id();
    as_user(
        &w,
        w.user.id,
        functions::channels::InviteToChannel {
            channel: input_channel(&w, w.user.id, dev),
            users: vec![
                input_user(&w, w.user.id, w.bot_id),
                input_user(&w, w.user.id, bob.id),
            ],
        },
    )
    .await
    .unwrap();
    let updates = next_updates(&w, &mut offset).await;
    let joined = updates
        .iter()
        .find_map(|u| u.get("my_chat_member"))
        .expect("my_chat_member");
    assert_eq!(
        joined["chat"],
        json!({ "id": chat_id, "type": "supergroup", "title": "Dev" })
    );
    assert_eq!(
        (
            joined["old_chat_member"]["status"].as_str(),
            joined["new_chat_member"]["status"].as_str()
        ),
        (Some("left"), Some("member"))
    );
    let added = updates
        .iter()
        .find_map(|u| u.get("message"))
        .expect("service message");
    assert_eq!(added["new_chat_members"].as_array().map(Vec::len), Some(2));

    // Commands reach the bot, with the channel's own ids; replies work in the chat.
    let id = says_in_channel(&w, w.user.id, dev, "/start").await.unwrap();
    let updates = next_updates(&w, &mut offset).await;
    assert_eq!(updates[0]["message"]["message_id"], id);
    assert_eq!(updates[0]["message"]["chat"]["type"], "supergroup");
    let sent = ok(
        &w,
        "sendMessage",
        json!({ "chat_id": chat_id, "text": "hi", "reply_parameters": { "message_id": id } }),
    )
    .await;
    assert_eq!(sent["message_id"], id + 1);
    assert_eq!(sent["reply_to_message"]["text"], "/start");

    let chat = ok(&w, "getChat", json!({ "chat_id": chat_id })).await;
    assert_eq!(chat["type"], "supergroup");
    assert_eq!(chat["permissions"]["can_send_messages"], true);
    assert_eq!(chat["permissions"]["can_change_info"], false);
    assert_eq!(
        ok(&w, "getChatMemberCount", json!({ "chat_id": chat_id })).await,
        3
    );

    // Without rights the bot can't restrict anyone.
    assert_eq!(
        fails(
            &w,
            "banChatMember",
            json!({ "chat_id": chat_id, "user_id": bob.id.0 })
        )
        .await,
        "Bad Request: not enough rights to restrict/unrestrict chat member"
    );
    as_user(
        &w,
        w.user.id,
        functions::channels::EditAdmin {
            channel: input_channel(&w, w.user.id, dev),
            user_id: input_user(&w, w.user.id, w.bot_id),
            admin_rights: types::ChatAdminRights {
                ban_users: true,
                delete_messages: true,
                change_info: true,
                other: true,
                ..Default::default()
            }
            .into(),
            rank: None,
        },
    )
    .await
    .unwrap();
    let updates = next_updates(&w, &mut offset).await;
    let promoted = updates
        .iter()
        .find_map(|u| u.get("my_chat_member"))
        .expect("my_chat_member");
    let me = &promoted["new_chat_member"];
    assert_eq!(me["status"], "administrator");
    assert_eq!(me["can_restrict_members"], true);
    assert_eq!(
        me["can_post_messages"],
        serde_json::Value::Null,
        "channels only"
    );
    let admins = ok(&w, "getChatAdministrators", json!({ "chat_id": chat_id })).await;
    let statuses: Vec<_> = admins
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["status"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(statuses, ["creator", "administrator"]);

    // Restricting Bob.
    ok(
        &w,
        "restrictChatMember",
        json!({ "chat_id": chat_id, "user_id": bob.id.0, "permissions": { "can_send_messages": false } }),
    )
    .await;
    let member = ok(
        &w,
        "getChatMember",
        json!({ "chat_id": chat_id, "user_id": bob.id.0 }),
    )
    .await;
    assert_eq!(member["status"], "restricted");
    assert_eq!(member["can_send_messages"], false);
    let err = says_in_channel(&w, bob.id, dev, "let me in")
        .await
        .unwrap_err();
    assert_eq!(err.message, "CHAT_SEND_PLAIN_FORBIDDEN");
    // Every permission lifts the restriction.
    let all = json!({
        "can_send_messages": true, "can_send_audios": true, "can_send_documents": true,
        "can_send_photos": true, "can_send_videos": true, "can_send_video_notes": true,
        "can_send_voice_notes": true, "can_send_polls": true, "can_send_other_messages": true,
        "can_add_web_page_previews": true, "can_react_to_messages": true, "can_edit_tag": true,
        "can_change_info": true, "can_invite_users": true, "can_pin_messages": true,
        "can_manage_topics": true
    });
    ok(
        &w,
        "restrictChatMember",
        json!({ "chat_id": chat_id, "user_id": bob.id.0, "permissions": all }),
    )
    .await;
    let member = ok(
        &w,
        "getChatMember",
        json!({ "chat_id": chat_id, "user_id": bob.id.0 }),
    )
    .await;
    assert_eq!(member["status"], "member");

    // Banned, then unbanned: Bob is out but may come back.
    ok(
        &w,
        "banChatMember",
        json!({ "chat_id": chat_id, "user_id": bob.id.0 }),
    )
    .await;
    let member = ok(
        &w,
        "getChatMember",
        json!({ "chat_id": chat_id, "user_id": bob.id.0 }),
    )
    .await;
    assert_eq!(
        (member["status"].as_str(), member["until_date"].as_i64()),
        (Some("kicked"), Some(0))
    );
    ok(
        &w,
        "unbanChatMember",
        json!({ "chat_id": chat_id, "user_id": bob.id.0 }),
    )
    .await;
    let member = ok(
        &w,
        "getChatMember",
        json!({ "chat_id": chat_id, "user_id": bob.id.0 }),
    )
    .await;
    assert_eq!(member["status"], "left");

    // Default permissions and the description.
    ok(
        &w,
        "setChatPermissions",
        json!({ "chat_id": chat_id, "permissions": { "can_send_messages": true, "can_send_polls": false } }),
    )
    .await;
    let chat = ok(&w, "getChat", json!({ "chat_id": chat_id })).await;
    assert_eq!(chat["permissions"]["can_send_photos"], false);
    ok(
        &w,
        "setChatDescription",
        json!({ "chat_id": chat_id, "description": "the dev team" }),
    )
    .await;
    let chat = ok(&w, "getChat", json!({ "chat_id": chat_id })).await;
    assert_eq!(chat["description"], "the dev team");
    assert_eq!(
        fails(
            &w,
            "setChatDescription",
            json!({ "chat_id": chat_id, "description": "the dev team" })
        )
        .await,
        "Bad Request: chat description is not modified"
    );

    // Admins with delete_messages delete anyone's messages.
    ok(
        &w,
        "deleteMessage",
        json!({ "chat_id": chat_id, "message_id": id }),
    )
    .await;
}

#[tokio::test]
async fn a_channel_with_the_bot_as_admin() {
    let w = world().await;
    let mut offset = 0;
    let news = create_channel(&w, true, "News").await;
    let chat_id = Peer::Channel(news).bot_api_id();
    as_user(
        &w,
        w.user.id,
        functions::channels::UpdateUsername {
            channel: input_channel(&w, w.user.id, news),
            username: "simula_news".into(),
        },
    )
    .await
    .unwrap();
    as_user(
        &w,
        w.user.id,
        functions::channels::EditAdmin {
            channel: input_channel(&w, w.user.id, news),
            user_id: input_user(&w, w.user.id, w.bot_id),
            admin_rights: types::ChatAdminRights {
                post_messages: true,
                edit_messages: true,
                other: true,
                ..Default::default()
            }
            .into(),
            rank: None,
        },
    )
    .await
    .unwrap();
    let updates = next_updates(&w, &mut offset).await;
    let joined = updates
        .iter()
        .find_map(|u| u.get("my_chat_member"))
        .expect("my_chat_member");
    assert_eq!(joined["chat"]["type"], "channel");
    assert_eq!(joined["chat"]["username"], "simula_news");
    assert_eq!(joined["new_chat_member"]["can_post_messages"], true);

    // Posts arrive as channel_post, from the channel itself.
    let post = says_in_channel(&w, w.user.id, news, "first post")
        .await
        .unwrap();
    let updates = next_updates(&w, &mut offset).await;
    let channel_post = &updates[0]["channel_post"];
    assert_eq!(channel_post["message_id"], post);
    assert_eq!(channel_post["sender_chat"]["id"], chat_id);
    assert!(channel_post.get("from").is_none());

    // The bot posts by @username, and edits the owner's post.
    let sent = ok(
        &w,
        "sendMessage",
        json!({ "chat_id": "@simula_news", "text": "bot post" }),
    )
    .await;
    assert_eq!(sent["chat"]["type"], "channel");
    assert_eq!(sent["sender_chat"]["id"], chat_id);
    let edited = ok(
        &w,
        "editMessageText",
        json!({ "chat_id": chat_id, "message_id": post, "text": "first post (edited)" }),
    )
    .await;
    assert_eq!(edited["text"], "first post (edited)");

    // Without post_messages, tdlib refuses before asking the server.
    as_user(
        &w,
        w.user.id,
        functions::channels::EditAdmin {
            channel: input_channel(&w, w.user.id, news),
            user_id: input_user(&w, w.user.id, w.bot_id),
            admin_rights: types::ChatAdminRights {
                edit_messages: true,
                other: true,
                ..Default::default()
            }
            .into(),
            rank: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(
        fails(
            &w,
            "sendMessage",
            json!({ "chat_id": chat_id, "text": "may I?" })
        )
        .await,
        "Bad Request: need administrator rights in the channel chat"
    );

    // Restricting is for supergroups.
    assert_eq!(
        fails(
            &w,
            "setChatPermissions",
            json!({ "chat_id": chat_id, "permissions": { "can_send_messages": true } })
        )
        .await,
        "Bad Request: can't change channel chat permissions"
    );
    ok(&w, "leaveChat", json!({ "chat_id": chat_id })).await;
    assert_eq!(
        fails(
            &w,
            "sendMessage",
            json!({ "chat_id": chat_id, "text": "still here?" })
        )
        .await,
        "Forbidden: bot is not a member of the channel chat"
    );
}
