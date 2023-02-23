mod_use::mod_use![attr, compaction, log];

/// Several invariants that must be true for the format to work.
mod format_invariants {
    use crate::{
        consts::{HEADER_SIZE, INDEX_SIZE},
        formats::log::UuidIndex,
    };

    const _: () = {
        use std::mem::size_of;

        use crate::formats::log::Header;

        const fn assert_is_copy<T: Copy>() {}

        // The header must be `HEADER_SIZE` bytes.
        assert!(size_of::<Header>() == HEADER_SIZE);
        // The index must be `INDEX_SIZE` bytes.
        assert!(size_of::<UuidIndex>() == INDEX_SIZE);

        // The header must not hold other resources (vec etc.).
        assert_is_copy::<Header>();
        // The index must not hold other resources (vec etc.).
        assert_is_copy::<UuidIndex>();
    };
}
