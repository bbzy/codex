fn main() {
    for directory in [
        "migrations",
        "logs_migrations",
        "goals_migrations",
        "memory_migrations",
        "queue_migrations",
        "thread_history_migrations",
    ] {
        println!("cargo:rerun-if-changed={directory}");
    }
}
