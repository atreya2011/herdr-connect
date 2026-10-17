use std::fmt::{self, Debug, Display};
use std::future::Future;
use std::hash::Hash;

use crate::{PermissionVendor, Question, TopologyRoute};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatError(String);

impl Display for ChatError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for ChatError {}

impl From<String> for ChatError {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<ChatError> for String {
    fn from(value: ChatError) -> Self {
        value.0
    }
}

pub enum Ack<'a> {
    Ephemeral(&'a str),
    Defer,
    EditDeferred(&'a str),
}

pub struct Component<C: Chat> {
    pub interaction: C::Interaction,
    pub route: C::Route,
    pub custom_id: String,
    pub values: Vec<String>,
    pub authorized: bool,
}

pub trait Chat: Send + Sync + Sized + 'static {
    type Route: Clone + Eq + Hash + Debug + Send + Sync + 'static;
    type Message: Clone + Eq + Hash + Debug + Send + Sync + 'static;
    type Interaction: Send + Sync + 'static;

    fn owner_id(&self) -> &str;

    fn sync_route_fresh(
        &self,
        route: &TopologyRoute,
    ) -> impl Future<Output = Result<Self::Route, ChatError>> + Send;
    fn expire_card(
        &self,
        route: &Self::Route,
        message: &Self::Message,
        text: &str,
    ) -> impl Future<Output = Result<(), ChatError>> + Send;
    fn post_question_card(
        &self,
        route: &Self::Route,
        question: &Question,
        token: &str,
    ) -> impl Future<Output = Result<Self::Message, ChatError>> + Send;
    fn edit_question_card(
        &self,
        route: &Self::Route,
        message: &Self::Message,
        question: &Question,
        token: &str,
    ) -> impl Future<Output = Result<(), ChatError>> + Send;
    fn post_permission_card(
        &self,
        route: &Self::Route,
        vendor: PermissionVendor,
        tool: &str,
        command: &str,
        token: &str,
    ) -> impl Future<Output = Result<Self::Message, ChatError>> + Send;
    fn expire_permission_card(
        &self,
        route: &Self::Route,
        message: &Self::Message,
        token: &str,
        text: &str,
    ) -> impl Future<Output = Result<(), ChatError>> + Send;
    fn acknowledge(
        &self,
        interaction: &Self::Interaction,
        ack: Ack<'_>,
    ) -> impl Future<Output = Result<(), ChatError>> + Send;
}
