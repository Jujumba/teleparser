use chrono::NaiveDateTime;
use clap::Parser;
use serde::{Deserialize, Serialize};

use rayon::prelude::*;

use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::Hash;
use std::path::PathBuf;
use std::{fs, io};

const SEPARATORS: [char; 12] = [' ', ',', '.','(', ')', '-', '!', '?', '\'', '\"', '\n', '\t'];

fn main() -> io::Result<()> {
    let cli = Cli::parse();

    if let Some(jobs) = cli.jobs {
        rayon::ThreadPoolBuilder::new().num_threads(jobs).build_global().unwrap();
    }

    let content = fs::read_to_string(cli.file)?;

    let chat: Chat = serde_json::from_str(&content)?;
    let stat: ChatStatistics = ChatStatistics::gather(&chat);

    let file = fs::File::create(cli.output)?;
    serde_json::to_writer_pretty(file, &stat)?;

    Ok(())
}

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long, short)]
    file: PathBuf,
    #[arg(long, short, default_value = "out.json")]
    output: PathBuf,
    #[arg(long, short)]
    jobs: Option<usize>
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

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
struct Id(u128);

#[derive(Debug, Serialize, Deserialize)]
struct Chat<'a> {
    name: String,
    #[serde(rename = "type")]
    chat_type: ChatType,
    id: Id,
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
    message_type: MessageType,
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
    text_type: TextEntityType,
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

impl TextEntityType {
    pub fn is_meta(&self) -> bool {
        use TextEntityType::*;
        matches!(self, Phone | BotCommand | Email | CustomEmoji | Mention)
    }
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
    pub fn gather(chat: &'a Chat) -> Self {
        let jobs = rayon::current_num_threads();

        let num_messages = chat.messages.len();
        let messages_per_thread = num_messages / jobs;

        let (sender, receiver) = crossbeam::channel::bounded(jobs + 1);
        let mut tokens_map = HashMap::new();
        let mut members_tokens_map = HashMap::new();

        chat.messages.par_iter().chunks(messages_per_thread).for_each_with(sender, move |sender, messages: Vec<&Message>| {
            let mut chunk_tokens_map: HashMap<Token<'_>, usize> = HashMap::new();
            let mut chunk_members_tokens_map: HashMap<Person<'_>, HashMap<Token<'_>, usize>> = HashMap::new();
            for message in messages.iter().filter(|message| message.message_type != MessageType::Service) {
                let from = message.from.clone().unwrap();
                for entity in message.text_entities.iter().filter(|entity| !entity.text_type.is_meta()) {
                    for token in entity.text.split(SEPARATORS).filter(|s| !s.is_empty()) {
                        let token = Token::from(remove_emojis(token)); // <-- such a performance hit!
                        *chunk_tokens_map.entry(token.clone()).or_insert(0) += 1;
                        if let Some(map) =  chunk_members_tokens_map.get_mut(&from) {
                            *map.entry(token).or_insert(0) += 1;
                        } else  {
                            let member_occurences_map = HashMap::from([(token, 1)]);
                            chunk_members_tokens_map.insert(from.clone(), member_occurences_map);
                        }
                    } 
                }
            }
            sender.send((chunk_tokens_map, chunk_members_tokens_map)).unwrap();
        });
        for _ in 0..jobs {
            let (chunk_tokens_map, chunk_members_tokens_map): (HashMap<Token<'_>, usize>, _) = receiver.recv().unwrap();
            merge_maps_with(&mut tokens_map, chunk_tokens_map, |tokens_map, token, occurences| *tokens_map.entry(token).or_insert(0) += occurences);
            for (member, map) in chunk_members_tokens_map {
                if let Some(mergee) = members_tokens_map.get_mut(&member) {
                    merge_maps_with(mergee, map, |mergee, member, occurences| *mergee.entry(member).or_insert(0) += occurences);
                } else {
                    members_tokens_map.insert(member, map);
                }
            }
        }
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

fn merge_maps_with<K, F>(dst: &mut HashMap<K, usize>, src: HashMap<K, usize>, f: F) 
where K: Eq + PartialEq + Hash,
F: Fn(&mut HashMap<K, usize>, K, usize) {
    for (key, occurences) in src {
        f(dst, key, occurences)
    }
}