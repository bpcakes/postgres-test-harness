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
    use super::{FingerprintBuilder, TemplateFingerprint};

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
