#[must_use]
pub fn read_activity_fixture(path: &str) -> String {
    match std::fs::read_to_string(path) {
        Ok(value) => value,
        Err(error) => format!("activity read error: {error}"),
    }
}
