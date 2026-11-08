use std::collections::HashSet;
use std::fmt::{self, Debug, Display};
use std::future::Future;
use std::hash::Hash;

use crate::{OwnerIdentity, PermissionVendor, Question, TopologyRoute, TransitionMessage};

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChatScope {
    TestRuns,
    Service,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActivityTask {
    pub id: String,
    pub title: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ActivityPlan {
    pub count: u32,
    pub prompts: u32,
    pub tasks: Vec<ActivityTask>,
}

impl ActivityPlan {
    #[must_use]
    pub fn next_tool_call(&self, tool: &str, summary: &str) -> Self {
        let count = self.count + 1;
        Self {
            count,
            prompts: self.prompts,
            tasks: self.appended(ActivityTask {
                id: format!("call_{count}"),
                title: crate::activity_task_title(tool, summary),
            }),
        }
    }

    #[must_use]
    pub fn next_prompt(&self) -> Self {
        let prompts = self.prompts + 1;
        Self {
            count: self.count,
            prompts,
            tasks: self.appended(ActivityTask {
                id: format!("prompt_{prompts}"),
                title: crate::PROMPT_TASK_TITLE.to_owned(),
            }),
        }
    }

    fn appended(&self, task: ActivityTask) -> Vec<ActivityTask> {
        let mut tasks = self
            .tasks
            .iter()
            .rev()
            .take(49)
            .cloned()
            .collect::<Vec<_>>();
        tasks.reverse();
        tasks.push(task);
        tasks
    }
}

#[cfg(test)]
mod tests {
    use super::{ActivityPlan, ActivityTask};

    #[test]
    fn activity_plan_keeps_the_newest_fifty_tool_calls() {
        let cases = [
            (0, Vec::new(), 1, 1, 1),
            (
                49,
                (1..=49)
                    .map(|id| ActivityTask {
                        id: format!("call_{id}"),
                        title: format!("call {id}"),
                    })
                    .collect(),
                50,
                50,
                1,
            ),
            (
                50,
                (1..=50)
                    .map(|id| ActivityTask {
                        id: format!("call_{id}"),
                        title: format!("call {id}"),
                    })
                    .collect(),
                51,
                50,
                2,
            ),
        ];
        for (count, tasks, expected_count, expected_len, expected_first_id) in cases {
            let plan = ActivityPlan {
                count,
                prompts: 0,
                tasks,
            }
            .next_tool_call("Bash", "echo next");
            assert_eq!(plan.count, expected_count);
            assert_eq!(plan.tasks.len(), expected_len);
            assert_eq!(
                plan.tasks.first().map(|task| task.id.clone()),
                Some(format!("call_{expected_first_id}"))
            );
            assert_eq!(
                plan.tasks.last().map(|task| task.id.clone()),
                Some(format!("call_{expected_count}"))
            );
            assert_eq!(
                plan.tasks.last().map(|task| task.title.as_str()),
                Some("Bash: echo next")
            );
        }
    }
}

pub enum Ack<'a> {
    Ephemeral(&'a str),
    Defer,
    EditDeferred(&'a str),
}

#[derive(Debug, PartialEq, Eq)]
pub enum Closed {
    Tab(String),
    Workspace(String),
}

#[derive(Debug, PartialEq, Eq)]
pub enum Renamed {
    Tab { tab_id: String, label: String },
    Workspace { workspace_id: String, label: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TopologyClosure {
    Tab {
        workspace_id: String,
        tab_id: String,
    },
    Workspace {
        workspace_id: String,
    },
}

pub struct Component<C: Chat> {
    pub interaction: C::Interaction,
    pub route: C::Route,
    pub custom_id: String,
    pub values: Vec<String>,
    pub authorized: bool,
}

pub struct OwnerMessage<C: Chat> {
    pub route: C::Route,
    pub message: C::Message,
    pub content: String,
    pub tab_id: String,
    pub workspace_id: String,
}

pub enum Inbound<C: Chat> {
    Posted(C::Posted),
    Component(Component<C>),
    Deleted(C::Deletion),
    Renamed(C::Rename),
}

pub trait Chat: Send + Sync + Sized + 'static {
    type Route: Clone + Eq + Hash + Debug + Send + Sync + 'static;
    type Message: Clone + Eq + Hash + Debug + Send + Sync + 'static;
    type Interaction: Clone + Send + Sync + 'static;
    type Posted: Send + 'static;
    type Deletion: Display + Send + 'static;
    type Rename: Send + 'static;
    const DELETES_ACTIVITY: bool = false;
    const PROMPT_ACTIVITY: bool = false;

    fn owner_id(&self) -> &str;
    fn scope(&self) -> ChatScope;
    fn accepts_herdr_route(
        &self,
        workspace: &str,
        tab: &str,
    ) -> impl Future<Output = Result<bool, ChatError>> + Send {
        async move {
            if self.scope() == ChatScope::Service {
                return Ok(true);
            }
            let (workspace, tab) = (workspace.to_owned(), tab.to_owned());
            tokio::task::spawn_blocking(move || crate::route_is_test_run(&workspace, &tab))
                .await
                .map_err(|error| ChatError(error.to_string()))?
                .map_err(ChatError)
        }
    }
    fn accepts_route(
        &self,
        route: &Self::Route,
    ) -> impl Future<Output = Result<bool, ChatError>> + Send;
    fn owner_message_accepted(
        &self,
        _route: &Self::Route,
        _message: &Self::Message,
    ) -> impl Future<Output = Result<(), ChatError>> + Send {
        async { Ok(()) }
    }
    fn lifecycle_subscriptions(&self) -> Vec<serde_json::Value> {
        crate::lifecycle_subscriptions()
    }
    fn topology_changed(&self) -> impl Future<Output = Result<(), ChatError>> + Send {
        async { Ok(()) }
    }
    fn sync_route(
        &self,
        route: &TopologyRoute,
    ) -> impl Future<Output = Result<Self::Route, ChatError>> + Send;
    fn sync_route_fresh(
        &self,
        route: &TopologyRoute,
    ) -> impl Future<Output = Result<Self::Route, ChatError>> + Send;
    fn sync_route_with_parent(
        &self,
        route: &TopologyRoute,
    ) -> impl Future<Output = Result<(Self::Route, Self::Route), ChatError>> + Send;
    fn cached_route(
        &self,
        route: &TopologyRoute,
    ) -> impl Future<Output = Result<Option<Self::Route>, ChatError>> + Send;
    fn register_archived(&self) -> impl Future<Output = Result<(), ChatError>> + Send;
    fn startup_sweep<F>(
        &self,
        routes: Vec<Result<TopologyRoute, String>>,
        live: F,
    ) -> impl Future<Output = ()> + Send
    where
        F: FnOnce() -> Result<(HashSet<String>, HashSet<String>), String> + Send;
    fn close_topology(
        &self,
        closure: &TopologyClosure,
    ) -> impl Future<Output = Result<(), ChatError>> + Send;
    fn post_live(
        &self,
        route: &Self::Route,
        text: &str,
        nonce: &str,
    ) -> impl Future<Output = Result<(), ChatError>> + Send;
    fn post_transition_card(
        &self,
        route: &Self::Route,
        card: &TransitionMessage,
        nonce: &str,
    ) -> impl Future<Output = Result<Self::Message, ChatError>> + Send;
    fn post_activity(
        &self,
        route: &Self::Route,
        text: &str,
        plan: &ActivityPlan,
    ) -> impl Future<Output = Result<Self::Message, ChatError>> + Send;
    fn edit_activity(
        &self,
        route: &Self::Route,
        message: &Self::Message,
        text: &str,
        plan: &ActivityPlan,
    ) -> impl Future<Output = Result<(), ChatError>> + Send;
    fn delete_activity(
        &self,
        _route: &Self::Route,
        _message: &Self::Message,
    ) -> impl Future<Output = Result<(), ChatError>> + Send {
        async { Ok(()) }
    }
    fn post_as_owner(
        &self,
        parent: &Self::Route,
        thread: &Self::Route,
        owner: &OwnerIdentity,
        text: &str,
    ) -> impl Future<Output = Result<(), ChatError>> + Send;
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
    fn typing(&self, route: &Self::Route) -> impl Future<Output = Result<(), ChatError>> + Send;
    fn agent_status(
        &self,
        _route: &TopologyRoute,
        _working: bool,
    ) -> impl Future<Output = Result<(), ChatError>> + Send {
        async { Ok(()) }
    }
    fn reply(
        &self,
        route: &Self::Route,
        message: &Self::Message,
        text: &str,
    ) -> impl Future<Output = Result<(), ChatError>> + Send;
    fn owner_message(
        &self,
        posted: Self::Posted,
    ) -> impl Future<Output = Result<Option<OwnerMessage<Self>>, ChatError>> + Send;
    fn next_event(&self) -> impl Future<Output = Option<Result<Inbound<Self>, ChatError>>> + Send;
    fn handle_deletion<F>(
        &self,
        deletion: Self::Deletion,
        close: F,
    ) -> impl Future<Output = Result<(), ChatError>> + Send
    where
        F: FnOnce(Closed) -> Result<(), String> + Send + 'static;
    fn handle_rename<F>(
        &self,
        rename: Self::Rename,
        apply: F,
    ) -> impl Future<Output = Result<(), ChatError>> + Send
    where
        F: FnOnce(Renamed) -> Result<(), String> + Send + 'static;
}
