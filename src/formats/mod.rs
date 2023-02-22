mod attr;
pub mod log;

/// Several invariants that must be true for the format to work.
mod format_invariants {
    use crate::consts::HEADER_SIZE;

    const _: () = {
        use std::mem::size_of;

        use crate::formats::log::Header;

        const fn assert_is_copy<T: Copy>() {}

        // The header must be `HEADER_SIZE` bytes.
        assert!(size_of::<Header>() == HEADER_SIZE);

        // The header must not hold other resources (vec etc.).
        assert_is_copy::<Header>();
    };
}
