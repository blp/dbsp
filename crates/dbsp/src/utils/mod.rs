mod advance_retreat;
mod sample;
//pub(crate) mod tests;
mod consolidation;
mod sort;
mod tuple;

#[cfg(test)]
mod vec_ext;

use std::{fmt::Display, hint::unreachable_unchecked, ptr};

pub use advance_retreat::{
    advance, advance_erased, dyn_advance, dyn_retreat, retreat, retreat_erased,
};
pub use consolidation::{
    consolidate, consolidate_from, consolidate_paired_slices, consolidate_payload_from,
    consolidate_slice, ConsolidatePairedSlices,
};

#[cfg(test)]
pub use consolidation::consolidate_pairs;

pub use sample::sample_slice;
pub use sort::{stable_sort, stable_sort_by, unstable_sort, unstable_sort_by};
pub use tuple::{
    ArchivedTup0, ArchivedTup1, ArchivedTup10, ArchivedTup2, ArchivedTup3, ArchivedTup4,
    ArchivedTup5, ArchivedTup6, ArchivedTup7, ArchivedTup8, ArchivedTup9, Tup0, Tup1, Tup10, Tup2,
    Tup3, Tup4, Tup5, Tup6, Tup7, Tup8, Tup9,
};

#[cfg(test)]
pub(crate) use vec_ext::VecExt;

/// Tells the optimizer that a condition is always true
///
/// # Safety
///
/// It's UB to call this function with `false` as the condition
#[inline(always)]
#[deny(unsafe_op_in_unsafe_fn)]
#[cfg_attr(debug_assertions, track_caller)]
pub(crate) unsafe fn assume(cond: bool) {
    debug_assert!(cond, "called `assume()` on a false condition");

    if !cond {
        // Safety: It's UB for `cond` to be false
        unsafe { unreachable_unchecked() };
    }
}

#[cold]
#[inline(never)]
pub(crate) fn cursor_position_oob<P: Display>(position: P, length: usize) -> ! {
    panic!("the cursor was at the invalid position {position} while the leaf was only {length} elements long")
}

// /// Casts a `Vec<T>` into a `Vec<MaybeUninit<T>>`
// #[inline]
// pub(crate) fn cast_uninit_vec<T>(vec: Vec<T>) -> Vec<MaybeUninit<T>> {
//     // Make sure we don't drop the old vec
//     let mut vec = ManuallyDrop::new(vec);

//     // Get the length, capacity and pointer of the vec (we get the pointer
// last as a     // rather nitpicky thing irt stacked borrows since the `.len()`
// and     // `.capacity()` calls technically reborrow the vec). Ideally we'd
// use     // `Vec::into_raw_parts()` but it's currently unstable via
// rust/#65816     let (len, cap, ptr) = (vec.len(), vec.capacity(),
// vec.as_mut_ptr());

//     // Create a new vec with the different type
//     unsafe { Vec::from_raw_parts(ptr.cast::<MaybeUninit<T>>(), len, cap) }
// }

// /// Creates a `Vec<MaybeUninit<T>>` with the given length
// /*#[inline]
// pub(crate) fn uninit_vec<T>(length: usize) -> Vec<MaybeUninit<T>> {
//     let mut buf: Vec<MaybeUninit<T>> = Vec::with_capacity(length);
//     // Safety: `buf` is initialized with uninitalized elements up to `length`
//     unsafe { buf.set_len(length) };
//     buf
// }*/
// // TODO: Replace with `MaybeUninit::write_slice_cloned()` via rust/#79995
// #[inline]
// pub(crate) fn write_uninit_slice_cloned<'a, T>(
//     this: &'a mut [MaybeUninit<T>],
//     src: &[T],
// ) -> &'a mut [T]
// where
//     T: Clone,
// {
//     // unlike copy_from_slice this does not call clone_from_slice on the
// slice     // this is because `MaybeUninit<T: Clone>` does not implement
// Clone.

//     struct Guard<'a, T> {
//         slice: &'a mut [MaybeUninit<T>],
//         initialized: usize,
//     }

//     impl<'a, T> Drop for Guard<'a, T> {
//         fn drop(&mut self) {
//             let initialized_part = &mut self.slice[..self.initialized];
//             // SAFETY: this raw slice will contain only initialized objects
//             // that's why, it is allowed to drop it.
//             unsafe {
//                 ptr::drop_in_place(&mut *(initialized_part as *mut
// [MaybeUninit<T>] as *mut [T]));             }
//         }
//     }

//     assert_eq!(
//         this.len(),
//         src.len(),
//         "destination and source slices have different lengths"
//     );
//     // NOTE: We need to explicitly slice them to the same length
//     // for bounds checking to be elided, and the optimizer will
//     // generate memcpy for simple cases (for example T = u8).
//     let len = this.len();
//     let src = &src[..len];

//     // guard is needed b/c panic might happen during a clone
//     let mut guard = Guard {
//         slice: this,
//         initialized: 0,
//     };

//     #[allow(clippy::needless_range_loop)]
//     for i in 0..len {
//         guard.slice[i].write(src[i].clone());
//         guard.initialized += 1;
//     }

//     forget(guard);

//     // SAFETY: Valid elements have just been written into `this` so it is
//     // initialized
//     unsafe { &mut *(this as *mut [MaybeUninit<T>] as *mut [T]) }
// }

#[inline]
pub(crate) fn bytes_of<T>(slice: &[T]) -> &[std::mem::MaybeUninit<u8>] {
    // Safety: It's always sound to interpret possibly uninitialized bytes as
    // `MaybeUninit<u8>`
    unsafe { std::slice::from_raw_parts(slice.as_ptr().cast(), std::mem::size_of_val(slice)) }
}

pub fn retain_in_columns<K, V>(
    keys: &mut Vec<K>,
    vals: &mut Vec<V>,
    lower_bound: usize,
    retain: &dyn Fn(&K, &V) -> bool,
) {
    debug_assert_eq!(keys.len(), vals.len());

    let original_len = keys.len();

    // SAFETY: We initialize BackshiftOnDrop with `lower_bound` offset below,
    // so these elements will never get touched again.
    unsafe {
        // Avoid double drop if the drop guard is not executed,
        // or if dropping elements up to `lower_bound` panics
        // since we may make some holes during the process.
        keys.set_len(0);
        vals.set_len(0);

        // Drop all keys up to `lower_bound`
        let keys = ptr::slice_from_raw_parts_mut(keys.as_mut_ptr(), lower_bound);
        ptr::drop_in_place(keys);

        // Drop all diffs up to `lower_bound`
        let vals = ptr::slice_from_raw_parts_mut(vals.as_mut_ptr(), lower_bound);
        ptr::drop_in_place(vals);
    }

    // Vec: [Kept, Kept, Hole, Hole, Hole, Hole, Unchecked, Unchecked]
    //      |<-              processed len   ->| ^- next to check
    //                  |<-  deleted cnt     ->|
    //      |<-              original_len                          ->|
    // Kept: Elements which predicate returns true on.
    // Hole: Moved or dropped element slot.
    // Unchecked: Unchecked valid elements.
    //
    // This drop guard will be invoked when predicate or `drop` of element panicked.
    // It shifts unchecked elements to cover holes and `set_len` to the correct
    // length. In cases when predicate and `drop` never panick, it will be
    // optimized out.
    struct BackshiftOnDrop<'a, K, R> {
        keys: &'a mut Vec<K>,
        vals: &'a mut Vec<R>,
        processed_len: usize,
        deleted_cnt: usize,
        original_len: usize,
    }

    impl<K, V> Drop for BackshiftOnDrop<'_, K, V> {
        fn drop(&mut self) {
            if self.deleted_cnt > 0 {
                let trailing = self.original_len - self.processed_len;
                let processed = self.processed_len - self.deleted_cnt;

                // SAFETY: Trailing unchecked items must be valid since we never touch them.
                unsafe {
                    ptr::copy(
                        self.keys.as_ptr().add(self.processed_len),
                        self.keys.as_mut_ptr().add(processed),
                        trailing,
                    );

                    ptr::copy(
                        self.vals.as_ptr().add(self.processed_len),
                        self.vals.as_mut_ptr().add(processed),
                        trailing,
                    );
                }
            }

            // SAFETY: After filling holes, all items are in contiguous memory.
            unsafe {
                let final_len = self.original_len - self.deleted_cnt;
                self.keys.set_len(final_len);
                self.vals.set_len(final_len);
            }
        }
    }

    let mut shifter = BackshiftOnDrop {
        keys,
        vals,
        processed_len: lower_bound,
        deleted_cnt: lower_bound,
        original_len,
    };

    fn process_loop<K, V, const DELETED: bool>(
        original_len: usize,
        retain: &dyn Fn(&K, &V) -> bool,
        shifter: &mut BackshiftOnDrop<'_, K, V>,
    ) {
        while shifter.processed_len != original_len {
            // SAFETY: Unchecked element must be valid.
            let current_key = unsafe { &mut *shifter.keys.as_mut_ptr().add(shifter.processed_len) };
            let current_val = unsafe { &mut *shifter.vals.as_mut_ptr().add(shifter.processed_len) };

            if !retain(current_key, current_val) {
                // Advance early to avoid double drop if `drop_in_place` panicked.
                shifter.processed_len += 1;
                shifter.deleted_cnt += 1;

                // SAFETY: We never touch these elements again after they're dropped.
                unsafe {
                    ptr::drop_in_place(current_key);
                    ptr::drop_in_place(current_val);
                }

                // We already advanced the counter.
                if DELETED {
                    continue;
                } else {
                    break;
                }
            }

            if DELETED {
                // SAFETY: `deleted_cnt` > 0, so the hole slot must not overlap with current
                // element. We use copy for move, and never touch this
                // element again.
                unsafe {
                    let hole_offset = shifter.processed_len - shifter.deleted_cnt;

                    let key_hole = shifter.keys.as_mut_ptr().add(hole_offset);
                    ptr::copy_nonoverlapping(current_key, key_hole, 1);

                    let val_hole = shifter.vals.as_mut_ptr().add(hole_offset);
                    ptr::copy_nonoverlapping(current_val, val_hole, 1);
                }
            }

            shifter.processed_len += 1;
        }
    }

    // Stage 1: Nothing was deleted.
    if lower_bound == 0 {
        process_loop::<K, V, false>(original_len, retain, &mut shifter);
    }

    // Stage 2: Some elements were deleted.
    process_loop::<K, V, true>(original_len, retain, &mut shifter);

    // All item are processed. This can be optimized to `set_len` by LLVM.
    drop(shifter);
}
