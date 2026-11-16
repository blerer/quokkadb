use quokkadb::{QuokkaDocument, QuokkaType};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, QuokkaType)]
struct Task {
    completed: bool,
}

#[derive(Serialize, Deserialize, QuokkaDocument)]
struct Project {
    #[serde(rename = "_id")]
    id: u64,
    tasks: Vec<Task>,
}

fn main() {
    let fields = Project::root_fields();
    fields.tasks.update_where(
        |task| task.completed.eq("wrong"),
        |task| task.completed.set(42),
    );
}
