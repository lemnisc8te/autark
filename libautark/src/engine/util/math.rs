//! Math utilities for use in DSP

/// The infamous fast invert square root function from Quake.
///
/// For the uninitiated, this computes f(x) = 1/√x using bitwise hacks instead of
/// mathematical operations. This has an error margin of around 0.175.
///
/// This MIGHT result in a speedup, especially on older hardware, but on newer hardware it is probably easier to just use an intrinsic.
pub const fn fast_inv_sqrt(number: f32) -> f32 {
    const THREEHALFS: f32 = 1.5;

    // Reinterpret the f32 as its bits
    let mut i: i32 = number.to_bits() as i32;

    // Because the bit representation of an IEEE-754 32-bit float is ~ roughly
    // the numbers logbase(2) representation, we can do some black magic to get the approximate square root really fast.
    //
    // In other words, (number.to_bits() as i32) ~= logbase(2, number)
    //
    // See https://github.com/francisrstokes/githublog/blob/main/2024%2F5%2F29%2Ffast-inverse-sqrt.md for more information
    i = 0x5F37_5A86_i32.wrapping_sub(i >> 1);

    // Turn the transformed bits back into an f32
    let y = f32::from_bits(i as u32);

    // Error correction via Newton's method
    y * (THREEHALFS - (number * 0.5 * y * y))
}

pub const F32_EQ_ERR_MARGIN: f32 = 0.00001;
