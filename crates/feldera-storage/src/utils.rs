use dbsp::trace::Deserializable;
use rand::{seq::index::sample, Rng};
use rkyv::{Archive, Deserialize, Infallible};
use std::fmt::Display;
use std::hint::unreachable_unchecked;

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

/// Compute a random sample of size `sample_size` of values in `slice`.
///
/// Pushes the random sample of values to the `output` vector in the order
/// in which values occur in `slice`, i.e., if `slice` is sorted, the output
/// will be sorted too.
pub fn sample_slice<T, RG>(
    slice: &[T::Archived],
    rng: &mut RG,
    sample_size: usize,
    output: &mut Vec<T>,
) where
    T: Archive + Deserializable + Clone,
    RG: Rng,
{
    let size = slice.len();

    if sample_size >= size {
        output.reserve(size);

        //let v = v
        for v in slice.iter() {
            let real_v = v.deserialize(&mut Infallible).unwrap();
            output.push(real_v);
        }
    } else {
        output.reserve(sample_size);

        let mut indexes = sample(rng, size, sample_size).into_vec();
        indexes.sort_unstable();
        for index in indexes.into_iter() {
            let real_v = slice[index].deserialize(&mut Infallible).unwrap();
            output.push(real_v);
        }
    }
}
