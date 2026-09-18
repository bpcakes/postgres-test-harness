use std::fmt;

use sha2::{Digest, Sha256};

const FINGERPRINT_BYTES: usize = 32;
const SHORT_FINGERPRINT_HEX_LEN: usize = 24;

/// SHA-256 identity of one initialized template.
///
/// Fingerprints are output-only: read one from
/// [`crate::DatabaseTemplate::fingerprint`] or [`RootSpec::fingerprint`] for
/// logs and comparisons. No API turns one back into a spec, because a derived
/// identity is valid only with the parent that produced it. To reopen a
/// derived scenario, repeat its root and `derive` calls; cache hits skip each
/// initializer.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct TemplateFingerprint([u8; FINGERPRINT_BYTES]);

impl TemplateFingerprint {
    /// Parses the catalog metadata encoding written by [`Self::to_hex`].
    pub(crate) fn from_hex(hex: &str) -> Option<Self> {
        if hex.len() != FINGERPRINT_BYTES * 2 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return None;
        }
        let mut bytes = [0_u8; FINGERPRINT_BYTES];
        for (index, chunk) in hex.as_bytes().as_chunks::<2>().0.iter().enumerate() {
            let encoded = std::str::from_utf8(chunk).ok()?;
            bytes[index] = u8::from_str_radix(encoded, 16).ok()?;
        }
        Some(Self(bytes))
    }

    pub fn to_hex(self) -> String {
        encode_hex(&self.0)
    }

    pub(crate) fn short_hex(self) -> String {
        self.to_hex()[..SHORT_FINGERPRINT_HEX_LEN].to_owned()
    }
}

impl fmt::Debug for TemplateFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("TemplateFingerprint")
            .field(&self.to_hex())
            .finish()
    }
}

fn encode_hex(bytes: &[u8; FINGERPRINT_BYTES]) -> String {
    let mut output = String::with_capacity(FINGERPRINT_BYTES * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to a String cannot fail");
    }
    output
}

/// Composes a persistent child identity without changing root fingerprints.
///
/// SHA-256 hashes three frames in order: the ASCII derived-v1 domain, the
/// parent's full 32 raw bytes, and the local step's full 32 raw bytes. Each
/// frame starts with its byte length as an unsigned 64-bit big-endian integer.
/// Changing this recipe requires a new domain version for cache compatibility.
fn derived_fingerprint(parent: TemplateFingerprint, step: StepSpec) -> TemplateFingerprint {
    let mut digest = Sha256::new();
    for frame in [
        b"postgres-test-harness-derived-template-v1".as_slice(),
        parent.0.as_slice(),
        step.0.as_slice(),
    ] {
        digest.update((frame.len() as u64).to_be_bytes());
        digest.update(frame);
    }
    TemplateFingerprint(digest.finalize().into())
}

/// Length-framed fingerprint builder that avoids concatenation ambiguity.
///
/// Finish with [`Self::finish_root`] for the complete identity passed to
/// [`crate::PostgresHarness::template`], or with [`Self::finish_step`] for one
/// local setup step passed to [`crate::DatabaseTemplate::derive`].
pub struct FingerprintBuilder(Sha256);

impl FingerprintBuilder {
    pub fn new(domain: impl AsRef<[u8]>) -> Self {
        let mut builder = Self(Sha256::new());
        builder.add_frame(b"postgres-test-harness-template-v1");
        builder.add_frame(domain.as_ref());
        builder
    }

    pub fn add(mut self, label: impl AsRef<[u8]>, content: impl AsRef<[u8]>) -> Self {
        self.add_frame(label.as_ref());
        self.add_frame(content.as_ref());
        self
    }

    /// Finishes the complete identity of a root template built from
    /// PostgreSQL's `template0`.
    pub fn finish_root(self) -> RootSpec {
        RootSpec(TemplateFingerprint(self.digest()))
    }

    /// Finishes the identity of one local setup step. `derive` composes it
    /// with the parent's complete identity.
    pub fn finish_step(self) -> StepSpec {
        StepSpec(self.digest())
    }

    fn add_frame(&mut self, bytes: &[u8]) {
        self.0.update((bytes.len() as u64).to_be_bytes());
        self.0.update(bytes);
    }

    fn digest(self) -> [u8; FINGERPRINT_BYTES] {
        self.0.finalize().into()
    }
}

/// Complete identity of a root template for [`crate::PostgresHarness::template`].
///
/// Only [`FingerprintBuilder::finish_root`] creates one, so a derived
/// template's fingerprint cannot be claimed as a root.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RootSpec(TemplateFingerprint);

impl RootSpec {
    /// Fingerprint of the template this spec gets or creates.
    pub fn fingerprint(self) -> TemplateFingerprint {
        self.0
    }
}

/// One local setup step for [`crate::DatabaseTemplate::derive`].
///
/// Only [`FingerprintBuilder::finish_step`] creates one. A step is not a
/// template identity: the child's fingerprint also covers every ancestor.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct StepSpec([u8; FINGERPRINT_BYTES]);

impl fmt::Debug for StepSpec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("StepSpec")
            .field(&encode_hex(&self.0))
            .finish()
    }
}

/// A template's fingerprint and the parent it was copied from, if any.
///
/// Catalog metadata records both, so a database built for one lineage is never
/// accepted as another lineage's template.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TemplateIdentity {
    fingerprint: TemplateFingerprint,
    parent: Option<TemplateFingerprint>,
}

impl TemplateIdentity {
    pub(crate) fn root(spec: RootSpec) -> Self {
        Self {
            fingerprint: spec.0,
            parent: None,
        }
    }

    pub(crate) fn derived(parent: TemplateFingerprint, step: StepSpec) -> Self {
        Self {
            fingerprint: derived_fingerprint(parent, step),
            parent: Some(parent),
        }
    }

    pub(crate) fn fingerprint(self) -> TemplateFingerprint {
        self.fingerprint
    }

    pub(crate) fn parent(self) -> Option<TemplateFingerprint> {
        self.parent
    }
}

#[cfg(test)]
mod tests {
    use super::{FingerprintBuilder, StepSpec, TemplateFingerprint, derived_fingerprint};

    #[test]
    fn derived_fingerprint_matches_independent_framed_vector() {
        let parent = TemplateFingerprint::from_hex(
            "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
        )
        .unwrap();
        let step = TemplateFingerprint::from_hex(
            "202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
        )
        .unwrap();
        // Independently calculated with Python hashlib.sha256 over frames
        // prefixed by struct.pack('>Q', len(frame)): lengths 41, 32, 32;
        // 129 bytes total. This deliberately does not use FingerprintBuilder.
        assert_eq!(
            derived_fingerprint(parent, StepSpec(step.0)).to_hex(),
            "678f8b338e81a48db8625fd8ab6c949a2852e6aec6261d710976de68381b7cd5"
        );
    }

    #[test]
    fn derived_fingerprint_tracks_ordered_parent_and_step_inputs() {
        let parent = FingerprintBuilder::new("schema")
            .add("migration", "v1")
            .finish_root()
            .fingerprint();
        let changed_parent = FingerprintBuilder::new("schema")
            .add("migration", "v2")
            .finish_root()
            .fingerprint();
        let step = FingerprintBuilder::new("fixture")
            .add("rows", "v1")
            .finish_step();
        let changed_step = FingerprintBuilder::new("fixture")
            .add("rows", "v2")
            .finish_step();
        let child = derived_fingerprint(parent, step);

        assert_eq!(child, derived_fingerprint(parent, step));
        assert_ne!(child, parent);
        assert_ne!(child.0, step.0);
        assert_ne!(child, derived_fingerprint(changed_parent, step));
        assert_ne!(child, derived_fingerprint(parent, changed_step));
        assert_ne!(
            child,
            derived_fingerprint(TemplateFingerprint(step.0), StepSpec(parent.0))
        );

        let descendant_step = FingerprintBuilder::new("descendant").finish_step();
        assert_ne!(
            derived_fingerprint(child, descendant_step),
            derived_fingerprint(derived_fingerprint(changed_parent, step), descendant_step)
        );
        assert_ne!(
            derived_fingerprint(child, descendant_step),
            derived_fingerprint(derived_fingerprint(parent, changed_step), descendant_step)
        );
    }

    #[test]
    fn fingerprint_frames_labels_and_content() {
        let left = FingerprintBuilder::new("schema")
            .add("ab", "c")
            .finish_root();
        let right = FingerprintBuilder::new("schema")
            .add("a", "bc")
            .finish_root();
        assert_ne!(left, right);
    }

    #[test]
    fn fingerprint_matches_known_vector_and_hex_round_trips() {
        let builder = || FingerprintBuilder::new("schema").add("migration", "select 1");
        let fingerprint = builder().finish_root().fingerprint();
        let expected = "fc8eb7daf3f83a0c92ccf0b5f4122bc72e6a84dba4eb38896c2c9d0ef95afe7f";

        assert_eq!(fingerprint.to_hex(), expected);
        // Steps hash the same frames, so existing derived identities persist.
        assert_eq!(builder().finish_step().0, fingerprint.0);
        assert_eq!(
            TemplateFingerprint::from_hex(expected).unwrap(),
            fingerprint
        );
    }
}
