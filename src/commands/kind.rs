use crate::parser::EventKind;
use clap::ValueEnum;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum KindArg {
    #[value(name = "user_message", alias = "user")]
    UserMessage,
    #[value(name = "assistant_message", alias = "assistant")]
    AssistantMessage,
    Command,
}

impl KindArg {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UserMessage => "user_message",
            Self::AssistantMessage => "assistant_message",
            Self::Command => "command",
        }
    }

    pub fn event_kind(self) -> EventKind {
        match self {
            Self::UserMessage => EventKind::UserMessage,
            Self::AssistantMessage => EventKind::AssistantMessage,
            Self::Command => EventKind::Command,
        }
    }
}

pub fn event_kinds(kinds: &[KindArg]) -> Vec<EventKind> {
    kinds.iter().map(|kind| kind.event_kind()).collect()
}
