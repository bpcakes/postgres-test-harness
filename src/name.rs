use uuid::Uuid;

use crate::{Error, ProjectName, Result, TemplateFingerprint};

const IDENTIFIER_MAX_LEN: usize = 63;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DatabaseKind {
    Test,
    Template,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DatabaseName {
    value: String,
    kind: DatabaseKind,
}

impl DatabaseName {
    pub(crate) fn test(project: &ProjectName) -> Self {
        Self::new(
            format!("pgh_{}_test_{}", project.as_str(), Uuid::now_v7().simple()),
            DatabaseKind::Test,
        )
        .expect("validated project names produce valid test database names")
    }

    pub(crate) fn template(project: &ProjectName, fingerprint: TemplateFingerprint) -> Self {
        Self::new(
            format!(
                "pgh_{}_template_{}",
                project.as_str(),
                fingerprint.short_hex()
            ),
            DatabaseKind::Template,
        )
        .expect("validated project names produce valid template database names")
    }

    pub(crate) fn from_existing(project: &ProjectName, value: String) -> Option<Self> {
        let test_prefix = format!("pgh_{}_test_", project.as_str());
        let template_prefix = format!("pgh_{}_template_", project.as_str());
        let kind = if let Some(suffix) = value.strip_prefix(&test_prefix) {
            valid_hex_suffix(suffix, 32).then_some(DatabaseKind::Test)?
        } else {
            let suffix = value.strip_prefix(&template_prefix)?;
            valid_hex_suffix(suffix, 24).then_some(DatabaseKind::Template)?
        };
        Self::new(value, kind).ok()
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.value
    }

    pub(crate) fn quoted(&self) -> String {
        format!(r#""{}""#, self.value)
    }

    pub(crate) fn kind(&self) -> DatabaseKind {
        self.kind
    }

    fn new(value: String, kind: DatabaseKind) -> Result<Self> {
        let valid = !value.is_empty()
            && value.len() <= IDENTIFIER_MAX_LEN
            && value
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_');
        if !valid {
            return Err(Error::InvalidDatabaseName { name: value });
        }
        Ok(Self { value, kind })
    }
}

fn valid_hex_suffix(suffix: &str, expected_len: usize) -> bool {
    suffix.len() == expected_len
        && suffix
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use crate::{FingerprintBuilder, ProjectName};

    use super::{DatabaseKind, DatabaseName};

    #[test]
    fn generated_names_fit_postgres_identifiers() {
        let project = ProjectName::new("sixteen_chars_ok").unwrap();
        let test = DatabaseName::test(&project);
        let template = DatabaseName::template(
            &project,
            FingerprintBuilder::new("schema")
                .finish_root()
                .fingerprint(),
        );
        assert!(test.as_str().len() <= 63);
        assert!(template.as_str().len() <= 63);
        assert_eq!(test.kind(), DatabaseKind::Test);
        assert_eq!(template.kind(), DatabaseKind::Template);
    }

    #[test]
    fn existing_names_require_the_exact_managed_shape() {
        let project = ProjectName::new("creditkit").unwrap();
        assert!(
            DatabaseName::from_existing(
                &project,
                "pgh_creditkit_test_00000000000000000000000000000000".to_owned()
            )
            .is_some()
        );
        assert!(
            DatabaseName::from_existing(&project, "pgh_creditkit_test_backup".to_owned()).is_none()
        );
        assert!(
            DatabaseName::from_existing(
                &project,
                "pgh_creditkit_template_000000000000000000000000".to_owned()
            )
            .is_some()
        );
        assert!(
            DatabaseName::from_existing(
                &project,
                "pgh_creditkit_template_00000000000000000000000g".to_owned()
            )
            .is_none()
        );
    }
}
