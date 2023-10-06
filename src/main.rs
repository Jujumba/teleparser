use chrono::NaiveDateTime;
use clap::Parser;
use serde::{Deserialize, Serialize};

use std::borrow::Cow;
use std::collections::HashMap;
use std::path::PathBuf;
use std::{fs, io};

fn main() -> io::Result<()> {
    let cli = Cli::parse();

    let content = fs::read_to_string(cli.file)?;

    let chat: Chat = serde_json::from_str(&content).unwrap();
    let stat: ChatStatistics = ChatStatistics::gather(&chat, cli.jobs);

    if let Some(output) = cli.output {
        let file = fs::File::create(output)?;
        serde_json::to_writer_pretty(file, &stat)?;
    }

    Ok(())
}

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long, short)]
    file: PathBuf,
    #[arg(long, short, default_value = "out.json")]
    output: Option<PathBuf>,
    #[arg(long, short, default_value_t = 12)]
    jobs: usize
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Hash, Clone)]
struct Person<'a>(Cow<'a, str>);

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Hash, Clone)]
struct Token<'a>(Cow<'a, str>);

impl<'a> From<&'a str> for Token<'a> {
    fn from(value: &'a str) -> Self {
        Token(value.into())
    }
}
impl<'a> From<String> for Token<'a> {
    fn from(value: String) -> Self {
        Token(value.into())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Chat<'a> {
    name: String,
    #[serde(rename = "type")]
    _type: ChatType,
    id: u128,
    messages: Vec<Message<'a>>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ChatType {
    PublicChannel,
    PrivateChannel,
    PublicSupergroup,
    PrivateSupergroup,
    PersonalChat,
    ChatForbidden,
}

#[derive(Debug, Serialize, Deserialize)]
struct Message<'a> {
    id: u64,
    #[serde(rename = "type")]
    _type: MessageType,
    date: NaiveDateTime,
    from: Option<Person<'a>>,
    text_entities: Vec<TextEntity>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum MessageType {
    Service,
    Message,
}

#[derive(Debug, Serialize, Deserialize)]
struct TextEntity {
    #[serde(rename = "type")]
    _type: TextEntityType,
    text: String,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum TextEntityType {
    Pre,
    Bold,
    Link,
    Code,
    Email,
    Plain,
    Phone,
    Italic,
    Cashtag,
    Spoiler,
    Mention,
    Hashtag,
    TextLink,
    Underline,
    BotCommand,
    CustomEmoji,
    MentionName,
    Strikethrough,
}

#[derive(Debug, Serialize, Deserialize)]
struct ChatStatistics<'a> {
    num_tokens: usize,
    #[serde(borrow)]
    members_tokens_map: HashMap<Person<'a>, HashMap<Token<'a>, usize>>,
    #[serde(borrow)]
    tokens_map: HashMap<Token<'a>, usize>,
}
impl<'a> ChatStatistics<'a> {
    fn gather(chat: &'a Chat, jobs: usize) -> Self {
        let num_messages = chat.messages.len();
        let messages_per_thread = num_messages / jobs;
        let (sender, receiver) = std::sync::mpsc::sync_channel(jobs);

        let mut tokens_map = HashMap::new();
        let mut members_tokens_map = HashMap::new();

        let iter = chat.messages.iter();
        std::thread::scope(|s| {
            for i in 0..jobs {
                let iter = iter.clone();
                let sender = sender.clone();
                s.spawn(move || {
                    let mut chunk_tokens_map = HashMap::new();
                    let mut chunk_members_tokens_map = HashMap::new();

                    for message in iter.skip(i * messages_per_thread).take(messages_per_thread).filter(|message| message._type != MessageType::Service) {
                        let from = message.from.clone().unwrap(); // okay since we are not copying the actual data
                        for entity in &message.text_entities {
                            for token in entity.text.split([' ', ',', '.','(', ')', '\'', '\"', '\n', '\t']).filter(|s| !s.is_empty()) {
                                let token = Token::from(remove_emojis(token)); // <-- such a performance hit!
                                update_occurences(token.clone(), &mut chunk_tokens_map);
                                match chunk_members_tokens_map.get_mut(&from) {
                                    Some(map) => update_occurences(token, map),
                                    None => {
                                        let member_occurences_map = HashMap::from([(token, 1)]);
                                        chunk_members_tokens_map.insert(from.clone(), member_occurences_map);
                                    }
                                }
                            } 
                        }
                    }

                    sender.send((chunk_tokens_map, chunk_members_tokens_map)).unwrap();
                });
            }
            for _ in 0..jobs {
                let (chunk_tokens_map, chunk_members_tokens_map) =  receiver.recv().unwrap();
                for (recv_elem, recv_occurences) in chunk_tokens_map {
                    let occurences = *tokens_map.get(&recv_elem).unwrap_or(&0);
                    tokens_map.insert(recv_elem, occurences + recv_occurences);
                }
                for (member, map) in chunk_members_tokens_map {
                    match members_tokens_map.get_mut(&member) {
                        None => {
                            members_tokens_map.insert(member, map);
                        }
                        Some(outer) => {
                            for (token, occurences) in map {
                                let _occurences = *outer.get(&token).unwrap_or(&0);
                                outer.insert(token, occurences + _occurences);
                            }
                        }
                    }
                }
            }
        });
        Self {
            num_tokens: tokens_map.len(),
            members_tokens_map,
            tokens_map,
        }
    }
}

fn remove_emojis(string: &str) -> String {
    use unicode_segmentation::UnicodeSegmentation;
    let graphemes = string.graphemes(true);

    let is_not_emoji = |x: &&str| emojis::get(x).is_none();

    graphemes.filter(is_not_emoji).collect()
}

#[inline]
fn update_occurences<'a>(token: Token<'a>, map: &mut HashMap<Token<'a>, usize>) {
    let occurences = *map.get(&token).unwrap_or(&0);
    map.insert(token, occurences + 1);
}