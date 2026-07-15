//! Size-capped opaque v3 keyset cursors.
//!
//! Cursor state is bound to an immutable snapshot and canonical request shape.  It carries the
//! last emitted candidate rather than an offset, so a continuation never depends on a retained
//! collection signature or a corpus-sized reference list.

use signal_aggregator::{FragilePageCursor, ListingOrder, PageLimit};

use super::{ListingOrderName, PageCollectionKind};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V3CursorBinding {
    pub collection: PageCollectionKind,
    pub order: ListingOrder,
    pub limit: PageLimit,
    pub snapshot_identity: String,
    pub query_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V3PageCursor {
    pub collection: PageCollectionKind,
    pub order: ListingOrder,
    pub limit: PageLimit,
    pub snapshot_identity: String,
    pub query_digest: String,
    pub last_reference: String,
    pub sort_tuple_digest: String,
    pub last_candidate_reference: String,
}

impl V3PageCursor {
    pub fn new(
        binding: V3CursorBinding,
        last_reference: String,
        sort_tuple_digest: String,
        last_candidate_reference: String,
    ) -> Self {
        Self {
            collection: binding.collection,
            order: binding.order,
            limit: binding.limit,
            snapshot_identity: binding.snapshot_identity,
            query_digest: binding.query_digest,
            last_reference,
            sort_tuple_digest,
            last_candidate_reference,
        }
    }

    pub fn to_reference(&self, maximum_bytes: u64) -> Option<FragilePageCursor> {
        let value = format!(
            "cursor:v3:{}:{}:{}:{}:{}:{}:{}:{}",
            self.collection.as_str(),
            ListingOrderName::new(self.order).as_str(),
            self.limit.into_u64(),
            self.snapshot_identity,
            self.query_digest,
            HexText::encode(&self.last_reference),
            self.sort_tuple_digest,
            HexText::encode(&self.last_candidate_reference),
        );
        (value.len() as u64 <= maximum_bytes).then(|| FragilePageCursor::new(value))
    }

    pub fn parse(reference: &FragilePageCursor, maximum_bytes: u64) -> Option<Self> {
        if reference.as_str().len() as u64 > maximum_bytes {
            return None;
        }
        let mut fields = reference.as_str().split(':');
        if fields.next()? != "cursor" || fields.next()? != "v3" {
            return None;
        }
        let cursor = Self {
            collection: PageCollectionKind::parse(fields.next()?)?,
            order: ListingOrderName::parse(fields.next()?)?,
            limit: PageLimit::new(fields.next()?.parse().ok()?),
            snapshot_identity: DigestText::parse(fields.next()?)?,
            query_digest: DigestText::parse(fields.next()?)?,
            last_reference: HexText::decode(fields.next()?)?,
            sort_tuple_digest: DigestText::parse(fields.next()?)?,
            last_candidate_reference: HexText::decode(fields.next()?)?,
        };
        fields.next().is_none().then_some(cursor)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DigestText;

impl DigestText {
    fn parse(value: &str) -> Option<String> {
        (value.len() == 16 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
            .then(|| value.to_owned())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HexText;

impl HexText {
    fn encode(value: &str) -> String {
        value
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    fn decode(value: &str) -> Option<String> {
        if !value.len().is_multiple_of(2) || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return None;
        }
        let bytes = (0..value.len())
            .step_by(2)
            .map(|position| u8::from_str_radix(&value[position..position + 2], 16).ok())
            .collect::<Option<Vec<_>>>()?;
        String::from_utf8(bytes).ok()
    }
}
