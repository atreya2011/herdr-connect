use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Follower {
    Claude(u64),
    Codex(u64),
    Cursor(i64),
}

pub struct LogPositions {
    path: PathBuf,
    positions: HashMap<PathBuf, (Follower, Follower)>,
}

impl LogPositions {
    pub fn open(path: PathBuf) -> Result<Self, String> {
        let positions = match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|error| error.to_string())?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => HashMap::new(),
            Err(error) => return Err(error.to_string()),
        };
        Ok(Self { path, positions })
    }

    #[must_use]
    pub fn get(&self, path: &Path) -> Option<(Follower, Follower)> {
        self.positions.get(path).copied()
    }

    pub fn save(
        &mut self,
        positions: impl IntoIterator<Item = (PathBuf, Follower, Follower)>,
    ) -> Result<(), String> {
        let mut changed = false;
        for (path, prompt, live) in positions {
            changed |= self.positions.insert(path, (prompt, live)) != Some((prompt, live));
        }
        if !changed {
            return Ok(());
        }
        let parent = self.path.parent().ok_or("positions file has no parent")?;
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        let bytes = serde_json::to_vec(&self.positions).map_err(|error| error.to_string())?;
        let temporary = self.path.with_extension("tmp");
        fs::write(&temporary, bytes).map_err(|error| error.to_string())?;
        fs::rename(temporary, &self.path).map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{Follower, LogPositions};

    #[test]
    fn saved_log_positions_resume_independently_for_each_destination() -> Result<(), String> {
        let directory =
            std::env::temp_dir().join(format!("testrun-log-positions-{}", std::process::id()));
        fs::create_dir(&directory).map_err(|error| error.to_string())?;
        let outcome = (|| {
            for (vendor, prompt, live) in [
                (
                    "claude",
                    Follower::Claude(101_513),
                    Follower::Claude(102_000),
                ),
                ("codex", Follower::Codex(101_513), Follower::Codex(102_000)),
                ("cursor", Follower::Cursor(10), Follower::Cursor(12)),
            ] {
                let log = directory.join(format!("{vendor}.log"));
                let slack = directory.join(format!("slack-{vendor}.json"));
                let discord = directory.join(format!("discord-{vendor}.json"));
                let mut saved = LogPositions::open(slack.clone())?;
                assert_eq!(saved.get(&log), None, "new log has no saved position");
                saved.save([(log.clone(), prompt, live)])?;
                let resumed = LogPositions::open(slack)?;
                assert_eq!(
                    resumed.get(&log),
                    Some((prompt, live)),
                    "restart resumes both followers"
                );
                assert_eq!(
                    LogPositions::open(discord)?.get(&log),
                    None,
                    "destinations keep separate positions"
                );
            }
            let invalid = directory.join("invalid.json");
            fs::write(&invalid, b"invalid JSON").map_err(|error| error.to_string())?;
            assert!(
                LogPositions::open(invalid).is_err(),
                "unreadable saved positions fail"
            );
            Ok(())
        })();
        fs::remove_dir_all(directory).map_err(|error| error.to_string())?;
        outcome
    }
}
