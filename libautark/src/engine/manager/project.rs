use anyhow::Result;

use crate::{
    engine::{
        constants::{MAX_BUFFER_SLOTS, MAX_NODES},
        manager::{Actor, Command, ManagerActorReceiver},
        state::GraphUpdate,
    },
    model::{flow::NodeID, project::ProjectData},
};

use std::sync::Arc;

pub struct ProjectStacks {
    pub(crate) current: ProjectData,
    pub(crate) undo_stack: Vec<ProjectData>,
    pub(crate) redo_stack: Vec<ProjectData>,
}

pub trait ProjectCommand: Command<Object = ProjectData> + Send {}

impl ProjectStacks {
    pub fn project(&self) -> &ProjectData {
        &self.current
    }

    pub fn undo(&mut self) {
        if let Some(prev) = self.undo_stack.pop() {
            self.redo_stack
                .push(std::mem::replace(&mut self.current, prev));
            self.publish_current();
        }
    }

    pub fn redo(&mut self) {
        if let Some(next) = self.redo_stack.pop() {
            self.undo_stack
                .push(std::mem::replace(&mut self.current, next));
            self.publish_current();
        }
    }

    pub(crate) fn commit(&mut self, next: ProjectData) {
        let previous_commit = std::mem::replace(&mut self.current, next);
        self.undo_stack.push(previous_commit);
        self.redo_stack.clear();
        self.publish_current();
    }

    /// Builds the next `GraphUpdate`
    fn publish_current(&mut self) -> Result<GraphUpdate> {
        let schedule = self
            .project()
            .compile_graph()
            .expect("command validation prevents cycles");

        if schedule.buffer_count > MAX_BUFFER_SLOTS || self.project().graph.nodes.len() > MAX_NODES
        {
            // In a real UI this would surface as a rejected edit before
            // getting here (validate in Command::execute); this is the
            // last-resort backstop.
            anyhow::bail!("graph exceeds preallocated real-time budget; edit ignored")
        }

        let old_ids: std::collections::HashSet<NodeID> = self
            .undo_stack
            .last()
            .map(|proj| proj.graph.nodes.keys().collect())
            .unwrap_or_default();
        let new_ids: std::collections::HashSet<NodeID> =
            self.project().graph.nodes.keys().collect();

        let state_additions: Vec<_> = new_ids
            .difference(&old_ids)
            .map(|&id| (id, self.project().graph.nodes[id].spawn_state()))
            .collect();
        let state_removals: Vec<_> = old_ids.difference(&new_ids).copied().collect();

        Ok(GraphUpdate {
            project: self.project().clone().into(),
            schedule: Arc::new(schedule),
            state_additions,
            state_removals,
        })
    }
}

pub struct ProjectActor {
    rx: ManagerActorReceiver,
    stacks: ProjectStacks,
}

impl<C: ProjectCommand> Actor<C> for ProjectActor {
    type State = ProjectStacks;

    fn new(rx: ManagerActorReceiver, stacks: Self::State) -> Self {
        Self { rx, stacks }
    }

    fn rx(&self) -> ManagerActorReceiver {
        self.rx.clone()
    }

    fn pre_command(&mut self) {
        let next = self.stacks.current.clone();
        self.stacks.commit(next);
    }

    fn obj(&mut self) -> &mut <C as Command>::Object {
        &mut self.stacks.current
    }
}
