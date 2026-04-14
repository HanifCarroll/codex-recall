use crate::parser::EventKind;
use clap::ValueEnum;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum KindArg {
    User,
    Assistant,
    Command,
}

impl KindArg {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Assistant => "assistant",
            Self::Command => "command",
        }
    }

    pub fn event_kind(self) -> EventKind {
        match self {
            Self::User => EventKind::UserMessage,
            Self::Assistant => EventKind::AssistantMessage,
            Self::Command => EventKind::Command,
        }
    }
}

pub fn event_kinds(kinds: &[KindArg]) -> Vec<EventKind> {
    kinds.iter().map(|kind| kind.event_kind()).collect()
}
