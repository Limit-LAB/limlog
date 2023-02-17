pub mod log;

/// Several invariants that must be true for the format to work.
mod format_invariants {
    const _: () = {
        use std::mem::size_of;

        use crate::formats::log::Header;

        const fn assert_is_copy<T: Copy>() {}

        assert!(size_of::<Header>() == 16);
        assert_is_copy::<Header>();
    };
}
