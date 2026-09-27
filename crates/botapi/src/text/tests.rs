//! Parser tests: tdlib's own cases (extracted into `tdlib_cases.json` from
//! td/test/message_entities.cpp), plus Markdown v1 and preparation behaviour.

use serde::Deserialize;
use simula_botapi_types::types as bot;

use super::entity::{DateFormat, Entity, Kind};
use super::{Formatted, html, markdown, markdown_v2, parse};

#[derive(Deserialize)]
struct Fixture {
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    parser: String,
    input: String,
    error: Option<String>,
    output: Option<String>,
    entities: Option<Vec<Expected>>,
}

#[derive(Deserialize)]
struct Expected {
    r#type: String,
    offset: i32,
    length: i32,
    arg: Option<serde_json::Value>,
}

fn expected(e: &Expected) -> Entity {
    let text = || {
        e.arg
            .as_ref()
            .and_then(|a| a.as_str())
            .unwrap_or_default()
            .to_owned()
    };
    let int = || {
        e.arg
            .as_ref()
            .and_then(serde_json::Value::as_i64)
            .unwrap_or_default()
    };
    let kind = match e.r#type.as_str() {
        "Bold" => Kind::Bold,
        "Italic" => Kind::Italic,
        "Underline" => Kind::Underline,
        "Strikethrough" => Kind::Strikethrough,
        "Spoiler" => Kind::Spoiler,
        "Code" => Kind::Code,
        "Pre" => Kind::Pre,
        "PreCode" => Kind::PreCode(text()),
        "TextUrl" => Kind::TextUrl(text()),
        "MentionName" => Kind::MentionName(int()),
        "CustomEmoji" => Kind::CustomEmoji(int()),
        "BlockQuote" => Kind::BlockQuote,
        "ExpandableBlockQuote" => Kind::ExpandableBlockQuote,
        "FormattedDate" => {
            let arg = e
                .arg
                .as_ref()
                .and_then(|a| a.as_array())
                .expect("[unix, flags]");
            let (unix, flags) = (arg[0].as_i64().unwrap() as i32, arg[1].as_i64().unwrap());
            Kind::FormattedDate {
                unix,
                format: DateFormat {
                    relative: flags & 1 != 0,
                    short_time: flags & 2 != 0,
                    long_time: flags & 4 != 0,
                    short_date: flags & 8 != 0,
                    long_date: flags & 16 != 0,
                    day_of_week: flags & 32 != 0,
                },
            }
        }
        other => panic!("unknown entity type {other}"),
    };
    Entity::new(kind, e.offset, e.length)
}

#[test]
fn all_tdlib_cases() {
    let fixture: Fixture = serde_json::from_str(include_str!("tdlib_cases.json")).unwrap();
    let mut failures = Vec::new();
    for case in &fixture.cases {
        let result = match case.parser.as_str() {
            "html" => html::parse(&case.input),
            _ => markdown_v2::parse(&case.input),
        };
        let want = match (&case.error, &case.output, &case.entities) {
            (Some(error), ..) => Err(error.clone()),
            (None, Some(output), Some(entities)) => {
                Ok((output.clone(), entities.iter().map(expected).collect()))
            }
            _ => unreachable!(),
        };
        if result != want {
            failures.push(format!(
                "{} {:?}\n    got  {result:?}\n    want {want:?}",
                case.parser, case.input
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} tdlib cases failed:\n{}",
        failures.len(),
        fixture.cases.len(),
        failures
            .iter()
            .take(15)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
}

/// tdlib's loop over every ASCII character.
#[test]
fn reserved_characters_markdown_v2() {
    for c in 1u8..126 {
        if b"_*[~`>".contains(&c) {
            continue;
        }
        let text = (c as char).to_string();
        if b"]()>#+-=|{}.!".contains(&c) {
            assert_eq!(
                markdown_v2::parse(&text),
                Err(format!(
                    "Character '{text}' is reserved and must be escaped with the preceding '\\'"
                ))
            );
            assert_eq!(
                markdown_v2::parse(&format!("\\{text}")),
                Ok((text.clone(), vec![]))
            );
        } else {
            assert_eq!(markdown_v2::parse(&text), Ok((text.clone(), vec![])));
        }
    }
}

#[test]
fn legacy_markdown() {
    let ok = |text: &str| markdown::parse(text).unwrap();
    assert_eq!(
        ok("*bold* _it_"),
        (
            "bold it".into(),
            vec![
                Entity::new(Kind::Bold, 0, 4),
                Entity::new(Kind::Italic, 5, 2)
            ]
        )
    );
    assert_eq!(
        ok("[x](https://a.b)").1,
        vec![Entity::new(Kind::TextUrl("https://a.b/".into()), 0, 1)]
    );
    assert_eq!(
        ok("```rust\nfn\n```").1,
        vec![Entity::new(Kind::PreCode("rust".into()), 0, 3)]
    );
    assert_eq!(ok("\\*not bold\\*").0, "*not bold*");
    assert_eq!(
        markdown::parse("*open"),
        Err("Can't find end of the entity starting at byte offset 0".into())
    );
}

fn mention(offset: i64, length: i64, kind: &str) -> bot::MessageEntity {
    bot::MessageEntity {
        r#type: kind.into(),
        offset,
        length,
        ..Default::default()
    }
}

/// Both steps, as a send does them.
fn prepare(
    text: &str,
    parse_mode: Option<&str>,
    entities: Option<&[bot::MessageEntity]>,
) -> Result<Formatted, crate::error::BotError> {
    parse(text, parse_mode, entities).and_then(Formatted::fix)
}

#[test]
fn quotes_keep_only_quote_entities_and_shift_their_position() {
    let (quote, position) = parse("  <b>bold</b> <code>x</code>", Some("HTML"), None)
        .unwrap()
        .into_quote(3)
        .unwrap();
    assert_eq!(quote.text, "bold x");
    assert_eq!(quote.entities, vec![Entity::new(Kind::Bold, 0, 4)]);
    assert_eq!(position, 5);
    assert!(parse("   ", None, None).unwrap().into_quote(0).is_none());
}

#[test]
fn prepare_matches_bot_api_errors_and_trimming() {
    let err = |text: &str, mode: &str| prepare(text, Some(mode), None).unwrap_err().description;
    assert_eq!(
        err("*x", "MarkdownV2"),
        "Bad Request: can't parse entities: Can't find end of Bold entity at byte offset 0"
    );
    assert_eq!(
        err("<b>x", "HTML"),
        "Bad Request: can't parse entities: Can't find end tag corresponding to start tag \"b\""
    );
    assert_eq!(err("x", "BBCode"), "Bad Request: unsupported parse_mode");
    assert_eq!(err("   ", "HTML"), "Bad Request: text must be non-empty");

    let prepared = prepare("  \n<b>hi</b> there  \n", Some("html"), None).unwrap();
    assert_eq!(prepared.text, "hi there");
    assert_eq!(prepared.entities, vec![Entity::new(Kind::Bold, 0, 2)]);

    // Explicit entities: automatic kinds are dropped (the server re-detects them).
    let explicit = [mention(0, 2, "bold"), mention(3, 4, "mention")];
    let prepared = prepare("hi @you", None, Some(&explicit)).unwrap();
    assert_eq!(prepared.entities, vec![Entity::new(Kind::Bold, 0, 2)]);
    let bad = [mention(0, 1, "sparkle")];
    assert_eq!(
        prepare("x", None, Some(&bad)).unwrap_err().description,
        "Bad Request: can't parse MessageEntity: Unsupported type specified"
    );
    assert_eq!(
        prepare(&"x".repeat(40_000), None, None)
            .unwrap_err()
            .description,
        "Bad Request: text is too long"
    );
}
