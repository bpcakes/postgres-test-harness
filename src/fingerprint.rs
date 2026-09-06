use std::fmt;

use sha2::{Digest, Sha256};

use crate::{Error, Result};

const FINGERPRINT_BYTES: usize = 32;
const SHORT_FINGERPRINT_HEX_LEN: usize = 24;

/// SHA-256 identity for every input that shapes an initialized template.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct TemplateFingerprint([u8; FINGERPRINT_BYTES]);

impl TemplateFingerprint {
    pub fn from_hex(hex: &str) -> Result<Self> {
        if hex.len() != FINGERPRINT_BYTES * 2 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(Error::InvalidTemplateFingerprint);
        }
        let mut bytes = [0_u8; FINGERPRINT_BYTES];
        for (index, chunk) in hex.as_bytes().as_chunks::<2>().0.iter().enumerate() {
            let encoded =
                std::str::from_utf8(chunk).map_err(|_| Error::InvalidTemplateFingerprint)?;
            bytes[index] =
                u8::from_str_radix(encoded, 16).map_err(|_| Error::InvalidTemplateFingerprint)?;
        }
        Ok(Self(bytes))
    }

    pub fn to_hex(self) -> String {
        let mut output = String::with_capacity(FINGERPRINT_BYTES * 2);
        for byte in self.0 {
            use std::fmt::Write as _;
            write!(&mut output, "{byte:02x}").expect("writing to a String cannot fail");
        }
        output
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

/// Composes a persistent child identity without changing root fingerprints.
///
/// SHA-256 hashes three frames in order: the ASCII derived-v1 domain, the
/// parent's full 32 raw bytes, and the local step's full 32 raw bytes. Each
/// frame starts with its byte length as an unsigned 64-bit big-endian integer.
/// Changing this recipe requires a new domain version for cache compatibility.
pub(crate) fn derived_fingerprint(
    parent: TemplateFingerprint,
    step: TemplateFingerprint,
) -> TemplateFingerprint {
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

    pub fn finish(self) -> TemplateFingerprint {
        TemplateFingerprint(self.0.finalize().into())
    }

    fn add_frame(&mut self, bytes: &[u8]) {
        self.0.update((bytes.len() as u64).to_be_bytes());
        self.0.update(bytes);
    }
}

/// Inputs required to locate or create one immutable database template.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TemplateSpec {
    fingerprint: TemplateFingerprint,
}

impl TemplateSpec {
    pub fn new(fingerprint: TemplateFingerprint) -> Self {
        Self { fingerprint }
    }

    pub fn fingerprint(self) -> TemplateFingerprint {
        self.fingerprint
    }
}

#[cfg(test)]
mod tests {
    use super::{FingerprintBuilder, TemplateFingerprint, derived_fingerprint};

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
            derived_fingerprint(parent, step).to_hex(),
            "678f8b338e81a48db8625fd8ab6c949a2852e6aec6261d710976de68381b7cd5"
        );
    }

    #[test]
    fn derived_fingerprint_tracks_ordered_parent_and_step_inputs() {
        let parent = FingerprintBuilder::new("schema")
            .add("migration", "v1")
            .finish();
        let changed_parent = FingerprintBuilder::new("schema")
            .add("migration", "v2")
            .finish();
        let step = FingerprintBuilder::new("fixture")
            .add("rows", "v1")
            .finish();
        let changed_step = FingerprintBuilder::new("fixture")
            .add("rows", "v2")
            .finish();
        let child = derived_fingerprint(parent, step);

        assert_eq!(child, derived_fingerprint(parent, step));
        assert_ne!(child, parent);
        assert_ne!(child, step);
        assert_ne!(child, derived_fingerprint(changed_parent, step));
        assert_ne!(child, derived_fingerprint(parent, changed_step));
        assert_ne!(child, derived_fingerprint(step, parent));

        let descendant_step = FingerprintBuilder::new("descendant").finish();
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
        let left = FingerprintBuilder::new("schema").add("ab", "c").finish();
        let right = FingerprintBuilder::new("schema").add("a", "bc").finish();
        assert_ne!(left, right);
    }

    #[test]
    fn fingerprint_matches_known_vector_and_hex_round_trips() {
        let fingerprint = FingerprintBuilder::new("schema")
            .add("migration", "select 1")
            .finish();
        let expected = "fc8eb7daf3f83a0c92ccf0b5f4122bc72e6a84dba4eb38896c2c9d0ef95afe7f";

        assert_eq!(fingerprint.to_hex(), expected);
        assert_eq!(
            TemplateFingerprint::from_hex(expected).unwrap(),
            fingerprint
        );
    }
}
