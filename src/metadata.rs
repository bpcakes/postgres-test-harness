use std::{collections::HashMap, time::SystemTime};

use crate::{ProjectName, TemplateFingerprint, fingerprint::TemplateIdentity};

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
        // Absent for roots, so their encoding predates derived templates.
        parent: Option<TemplateFingerprint>,
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
        identity: TemplateIdentity,
        state: TemplateState,
    ) -> Self {
        Self::Template {
            project,
            lock_key,
            fingerprint: identity.fingerprint(),
            parent: identity.parent(),
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
                parent,
                state,
                created_at,
            } => {
                let parent = parent
                    .map(|parent| format!(";parent={}", parent.to_hex()))
                    .unwrap_or_default();
                let state = match state {
                    TemplateState::Initializing => "initializing",
                    TemplateState::Ready => "ready",
                };
                format!(
                    "{PREFIX};kind=template;project={};lock={lock_key};fingerprint={}{parent};state={state};created={created_at}",
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
            "template" => Self::Template {
                project,
                lock_key: values.remove("lock")?.parse().ok()?,
                fingerprint: TemplateFingerprint::from_hex(values.remove("fingerprint")?)?,
                parent: match values.remove("parent") {
                    Some(parent) => Some(TemplateFingerprint::from_hex(parent)?),
                    None => None,
                },
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
    use crate::{FingerprintBuilder, ProjectName, fingerprint::TemplateIdentity};

    use super::{ResourceMetadata, TemplateState};

    #[test]
    fn test_metadata_round_trips() {
        let metadata = ResourceMetadata::test(ProjectName::new("creditkit").unwrap(), -42);
        assert_eq!(ResourceMetadata::parse(&metadata.encode()), Some(metadata));
    }

    #[test]
    fn root_template_metadata_keeps_its_original_encoding() {
        let encoded = "postgres-test-harness:v1;kind=template;project=creditkit;lock=42;\
            fingerprint=fc8eb7daf3f83a0c92ccf0b5f4122bc72e6a84dba4eb38896c2c9d0ef95afe7f;\
            state=ready;created=7";
        let root = FingerprintBuilder::new("schema")
            .add("migration", "select 1")
            .finish_root();
        let metadata = ResourceMetadata::parse(encoded).unwrap();

        assert_eq!(
            metadata,
            ResourceMetadata::Template {
                project: ProjectName::new("creditkit").unwrap(),
                lock_key: 42,
                fingerprint: root.fingerprint(),
                parent: None,
                state: TemplateState::Ready,
                created_at: 7,
            }
        );
        assert_eq!(metadata.encode(), encoded);
    }

    #[test]
    fn derived_template_metadata_records_its_parent() {
        let encoded = "postgres-test-harness:v1;kind=template;project=creditkit;lock=42;\
            fingerprint=46d3709876f0cd53ee201cbda27d8c0322ccd48fe4fdfd95de2f14ae669b2123;\
            parent=4c618086f44e0de75968300e7a2db86f507fc5688a461831f3f679a80d0c60ed;\
            state=ready;created=7";
        let parent = FingerprintBuilder::new("schema")
            .finish_root()
            .fingerprint();
        let identity =
            TemplateIdentity::derived(parent, FingerprintBuilder::new("fixture").finish_step());
        let written = ResourceMetadata::template(
            ProjectName::new("creditkit").unwrap(),
            42,
            identity,
            TemplateState::Ready,
        );
        assert!(matches!(
            written,
            ResourceMetadata::Template {
                fingerprint,
                parent: Some(recorded_parent),
                ..
            } if fingerprint == identity.fingerprint() && recorded_parent == parent
        ));
        let metadata = ResourceMetadata::Template {
            project: ProjectName::new("creditkit").unwrap(),
            lock_key: 42,
            fingerprint: identity.fingerprint(),
            parent: identity.parent(),
            state: TemplateState::Ready,
            created_at: 7,
        };

        assert_eq!(metadata.encode(), encoded);
        assert!(encoded.contains(&format!(";parent={};", parent.to_hex())));
        assert_eq!(ResourceMetadata::parse(encoded), Some(metadata));
        assert!(
            ResourceMetadata::parse(&encoded.replace(&parent.to_hex(), "not-a-fingerprint"))
                .is_none()
        );
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
        let template = ResourceMetadata::template(
            ProjectName::new("x").unwrap(),
            1,
            TemplateIdentity::root(FingerprintBuilder::new("schema").finish_root()),
            TemplateState::Ready,
        )
        .encode();
        assert!(ResourceMetadata::parse(&template).is_some());
        assert!(ResourceMetadata::parse(&format!("{template};extra=1")).is_none());
    }
}
