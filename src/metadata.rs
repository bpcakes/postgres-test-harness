use std::{collections::HashMap, time::SystemTime};

use crate::{ProjectName, TemplateFingerprint};

const PREFIX: &str = "postgres-test-harness:v1";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TemplateState {
    Initializing,
    Ready,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ResourceMetadata {
    Test {
        project: ProjectName,
        owner_key: i64,
        created_at: u64,
    },
    Template {
        project: ProjectName,
        lock_key: i64,
        fingerprint: TemplateFingerprint,
        state: TemplateState,
        created_at: u64,
    },
}

impl ResourceMetadata {
    pub(crate) fn test(project: ProjectName, owner_key: i64) -> Self {
        Self::Test {
            project,
            owner_key,
            created_at: unix_now(),
        }
    }

    pub(crate) fn template(
        project: ProjectName,
        lock_key: i64,
        fingerprint: TemplateFingerprint,
        state: TemplateState,
    ) -> Self {
        Self::Template {
            project,
            lock_key,
            fingerprint,
            state,
            created_at: unix_now(),
        }
    }

    pub(crate) fn encode(&self) -> String {
        match self {
            Self::Test {
                project,
                owner_key,
                created_at,
            } => format!(
                "{PREFIX};kind=test;project={};owner={owner_key};created={created_at}",
                project.as_str()
            ),
            Self::Template {
                project,
                lock_key,
                fingerprint,
                state,
                created_at,
            } => {
                let state = match state {
                    TemplateState::Initializing => "initializing",
                    TemplateState::Ready => "ready",
                };
                format!(
                    "{PREFIX};kind=template;project={};lock={lock_key};fingerprint={};state={state};created={created_at}",
                    project.as_str(),
                    fingerprint.to_hex()
                )
            }
        }
    }

    pub(crate) fn parse(encoded: &str) -> Option<Self> {
        let mut parts = encoded.split(';');
        if parts.next()? != PREFIX {
            return None;
        }
        let mut values = HashMap::new();
        for part in parts {
            let (key, value) = part.split_once('=')?;
            if values.insert(key, value).is_some() {
                return None;
            }
        }
        let kind = values.remove("kind")?;
        let project = ProjectName::new(values.remove("project")?).ok()?;
        let created_at = values.remove("created")?.parse().ok()?;
        let metadata = match kind {
            "test" if values.len() == 1 => Self::Test {
                project,
                owner_key: values.remove("owner")?.parse().ok()?,
                created_at,
            },
            "template" if values.len() == 3 => Self::Template {
                project,
                lock_key: values.remove("lock")?.parse().ok()?,
                fingerprint: TemplateFingerprint::from_hex(values.remove("fingerprint")?).ok()?,
                state: match values.remove("state")? {
                    "initializing" => TemplateState::Initializing,
                    "ready" => TemplateState::Ready,
                    _ => return None,
                },
                created_at,
            },
            _ => return None,
        };
        values.is_empty().then_some(metadata)
    }

    pub(crate) fn project(&self) -> &ProjectName {
        match self {
            Self::Test { project, .. } | Self::Template { project, .. } => project,
        }
    }

    pub(crate) fn created_at(&self) -> u64 {
        match self {
            Self::Test { created_at, .. } | Self::Template { created_at, .. } => *created_at,
        }
    }

    pub(crate) fn lock_key(&self) -> i64 {
        match self {
            Self::Test { owner_key, .. } => *owner_key,
            Self::Template { lock_key, .. } => *lock_key,
        }
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use crate::{FingerprintBuilder, ProjectName};

    use super::{ResourceMetadata, TemplateState};

    #[test]
    fn test_metadata_round_trips() {
        let metadata = ResourceMetadata::test(ProjectName::new("creditkit").unwrap(), -42);
        assert_eq!(ResourceMetadata::parse(&metadata.encode()), Some(metadata));
    }

    #[test]
    fn template_metadata_round_trips() {
        let metadata = ResourceMetadata::template(
            ProjectName::new("creditkit").unwrap(),
            42,
            FingerprintBuilder::new("schema").finish(),
            TemplateState::Ready,
        );
        assert_eq!(ResourceMetadata::parse(&metadata.encode()), Some(metadata));
    }

    #[test]
    fn metadata_rejects_unknown_or_duplicate_fields() {
        assert!(ResourceMetadata::parse("not-the-harness").is_none());
        assert!(
            ResourceMetadata::parse(
                "postgres-test-harness:v1;kind=test;kind=test;project=x;owner=1;created=1"
            )
            .is_none()
        );
    }
}
