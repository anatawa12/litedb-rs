use super::utils::ToHex;
use std::cmp::Ordering;
use std::fmt::{Debug, Display, Formatter};
use std::ops::{Add, Div, Mul, Neg, Sub};

/// Microsoft's decimal128
///
/// Please note that this is NOT BSON compliant data.
///
/// This struct is only for storing / passing data,
/// so most mathematical operations are not implemented (yet)
#[derive(Clone, Copy)]
pub struct Decimal128 {
    repr: u128,
}

// the exponent is at most 28, so the scale is at most 28
const POWERS_10: &[u128; 29] = &[
    1,
    10,
    100,
    1000,
    10000,
    100000,
    1000000,
    10000000,
    100000000,
    1000000000,
    10000000000,
    100000000000,
    1000000000000,
    10000000000000,
    100000000000000,
    1000000000000000,
    10000000000000000,
    100000000000000000,
    1000000000000000000,
    10000000000000000000,
    100000000000000000000,
    1000000000000000000000,
    10000000000000000000000,
    100000000000000000000000,
    1000000000000000000000000,
    10000000000000000000000000,
    100000000000000000000000000,
    1000000000000000000000000000,
    10000000000000000000000000000,
];

const MAX_POWERS_10: u128 = 10000000000000000000000000000;

const DEC_SCALE_MAX: u32 = 28;

impl Decimal128 {
    pub const ZERO: Decimal128 = Decimal128 { repr: 0u128 };
    pub const MAX: Decimal128 = Decimal128::new(79228162514264337593543950335, 0, false);
    pub const MIN: Decimal128 = Decimal128::new(79228162514264337593543950335, 0, true);

    const SIGN_MASK: u128 = 0x80000000_00000000_00000000_00000000;
    const EXPONENT_MASK: u128 = 0x00FF0000_00000000_00000000_00000000;
    const EXPONENT_SHIFT: u128 = 112;
    const MANTISSA_MASK: u128 = 0x00000000_FFFFFFFF_FFFFFFFF_FFFFFFFF;
    const UNUSED_BITS: u128 = !(Self::SIGN_MASK | Self::EXPONENT_MASK | Self::MANTISSA_MASK);

    /// Construct a new decimal128 from raw representation
    ///
    /// Returns None if the representation is not valid
    #[inline]
    pub const fn from_bytes(bytes: [u8; 16]) -> Option<Decimal128> {
        let repr = u128::from_le_bytes(bytes);

        // check unused bits are not used
        if (repr & Self::UNUSED_BITS) != 0 {
            return None;
        }

        let result = Self { repr };

        if result.exponent() > 28 {
            return None;
        }

        Some(result)
    }

    /// Construct a new decimal128 from raw parts
    ///
    /// The result value would be
    /// `mantissa` * 10 ^ -`exponent` \* (if `is_negative` { -1 } else { 1 }) where ^ is power
    ///
    /// ### Panics
    /// This function panics if:
    /// - mantissa is greater than 1 << 96
    /// - exponent is greater than 28
    #[inline]
    pub const fn new(mantissa: u128, exponent: u32, is_negative: bool) -> Decimal128 {
        assert!(mantissa <= Self::MANTISSA_MASK, "Mantissa is too big");
        assert!(exponent <= DEC_SCALE_MAX, "Exponent is too big");

        let repr = mantissa
            | ((exponent as u128) << Self::EXPONENT_SHIFT)
            | (if is_negative { Self::SIGN_MASK } else { 0 });

        Decimal128 { repr }
    }

    /// Construct a new decimal128 from raw parts with mantissa and exponent
    ///
    /// The result value would be
    /// `mantissa` * 10 ^ -`exponent` \* (if `is_negative` { -1 } else { 1 }) where ^ is power
    ///
    /// ### Panics
    /// This function panics if:
    /// - mantissa is greater than 1 << 96
    /// - exponent is greater than 28
    #[inline]
    pub const fn new_signed(mantissa: i128, exponent: u32) -> Decimal128 {
        let is_negative = mantissa < 0;
        let mantissa = mantissa.unsigned_abs();
        Self::new(mantissa, exponent, is_negative)
    }

    /// Parses the string and converts to Decimal128.
    ///
    /// Decimal is only supported.
    pub const fn parse(mut s: &str) -> Option<Decimal128> {
        let mut is_negative = false;
        if s.is_empty() {
            return None; // too short
        }
        if s.as_bytes()[0] == b'-' {
            is_negative = true;
            s = split_at(s, 1).1;
        } else if s.as_bytes()[0] == b'+' {
            s = split_at(s, 1).1;
        }
        if s.is_empty() {
            return None; // too short without sign char
        }

        macro_rules! parse_u128 {
            ($expr: expr) => {
                if let Ok(v) = u128::from_str_radix($expr, 10) {
                    v
                } else {
                    return None;
                }
            };
        }

        if let Some(dot) = find_dot(s.as_bytes()) {
            // XXX.YYY, .YYY, XXX.
            let before_dot = split_at(s, dot).0;
            let after_dot = split_at(s, dot + 1).1;
            if after_dot.len() > DEC_SCALE_MAX as usize {
                return None; // too precise
            }
            let before_dot_u128 = if !before_dot.is_empty() {
                parse_u128!(before_dot)
            } else {
                0
            };
            let after_dot_u128 = if !after_dot.is_empty() {
                parse_u128!(after_dot)
            } else {
                0
            };

            // no overflow; we've checked after_dot.len() <= 128
            let exponent = after_dot.len() as u32;
            let mantissa = {
                let Some(tmp) = before_dot_u128.checked_mul(POWERS_10[exponent as usize]) else {
                    return None;
                };
                let Some(tmp) = tmp.checked_add(after_dot_u128) else {
                    return None;
                };
                if tmp > Self::MANTISSA_MASK {
                    return None; // too big
                }
                tmp
            };

            return Some(Decimal128::new(mantissa, exponent, is_negative));
        } else {
            // XXX
            let as_u128 = parse_u128!(s);
            if as_u128 > Self::MANTISSA_MASK {
                return None; // too big
            }
            return Some(Decimal128::new(as_u128, 0, is_negative));
        }

        const fn find_dot(bytes: &[u8]) -> Option<usize> {
            let mut index = 0;
            while index < bytes.len() {
                if bytes[index] == b'.' {
                    return Some(index);
                }
                index += 1;
            }
            None
        }

        // region const version of split_at
        // const_str_split_at feature is not stable yet (it's in FCP now though),
        // so here is const reimplementation
        const fn is_utf8_char_boundary(b: u8) -> bool {
            // This is bit magic equivalent to: b < 128 || b >= 192
            (b as i8) >= -0x40
        }

        const fn is_char_boundary(s: &str, idx: usize) -> bool {
            if idx >= s.len() {
                idx == s.len()
            } else {
                is_utf8_char_boundary(s.as_bytes()[idx])
            }
        }

        const fn split_at(s: &str, mid: usize) -> (&str, &str) {
            match split_at_checked(s, mid) {
                Some(pair) => pair,
                None => panic!(),
            }
        }

        const fn split_at_checked(s: &str, mid: usize) -> Option<(&str, &str)> {
            if is_char_boundary(s, mid) {
                let (head, tail) = s.as_bytes().split_at(mid);
                unsafe {
                    Some((
                        core::str::from_utf8_unchecked(head),
                        core::str::from_utf8_unchecked(tail),
                    ))
                }
            } else {
                None
            }
        }
        // endregion
    }

    #[inline]
    pub fn bytes(&self) -> [u8; 16] {
        self.repr.to_le_bytes()
    }

    #[inline]
    pub const fn is_negative(&self) -> bool {
        (self.repr & Self::SIGN_MASK) != 0
    }

    #[inline]
    pub const fn abs(self) -> Self {
        Self {
            repr: self.repr & !Self::SIGN_MASK,
        }
    }

    pub fn round_digits(mut self, digits: u32) -> Self {
        assert!(digits < DEC_SCALE_MAX);
        if self.exponent() as u32 > digits {
            self = Self::new(
                self.mantissa() / POWERS_10[(self.exponent() as u32 - digits) as usize],
                digits,
                self.is_negative(),
            );
        }
        self
    }

    #[inline]
    const fn exponent(&self) -> u8 {
        (((self.repr & Self::EXPONENT_MASK) >> Self::EXPONENT_SHIFT) & 0xFF) as u8
    }

    #[inline]
    const fn mantissa(&self) -> u128 {
        self.repr & Self::MANTISSA_MASK
    }

    #[inline]
    const fn mantissa_signed(&self) -> i128 {
        self.mantissa() as i128 * if self.is_negative() { -1 } else { 1 }
    }

    pub fn to_f64(&self) -> f64 {
        let mut mantissa = self.mantissa_signed() as f64;

        mantissa /= POWERS_10[self.exponent() as usize] as f64;

        mantissa
    }

    pub fn to_i64(&self) -> Option<i64> {
        let mut mantissa = self.mantissa_signed();

        mantissa /= POWERS_10[self.exponent() as usize] as i128;

        mantissa.try_into().ok()
    }

    pub fn to_i32(&self) -> Option<i32> {
        let mut mantissa = self.mantissa_signed();

        mantissa /= POWERS_10[self.exponent() as usize] as i128;

        mantissa.try_into().ok()
    }
}

impl Debug for Decimal128 {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Decimal128")
            .field(&ToHex(self.bytes()))
            .finish()
    }
}

impl PartialEq<Self> for Decimal128 {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Decimal128 {}

impl PartialOrd for Decimal128 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Decimal128 {
    fn cmp(&self, other: &Self) -> Ordering {
        // based on .NET implementation
        // https://github.com/dotnet/runtime/blob/ad7b8299d8d80eb27cf22838c7017c5872056b56/src/libraries/System.Private.CoreLib/src/System/Decimal.DecCalc.cs#L1215
        fn var_dec_cmp(d1: &Decimal128, d2: &Decimal128) -> Ordering {
            match (d1.mantissa(), d2.mantissa()) {
                // mantissa == 0 means it's zero,
                // so if either is not zero, we can check ordering by seeing sign flag
                (0, 0) => return Ordering::Equal,
                (0, _) => {
                    return if d2.is_negative() {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    };
                }
                (_, 0) => {
                    return if d1.is_negative() {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    };
                }
                _ => {}
            }

            // bool::cmp: true is greater than false
            // negative is less than positive so swap
            let sign = d2.is_negative().cmp(&d1.is_negative());
            if !sign.is_eq() {
                return sign;
            }

            var_dec_cmp_sub(d1, d2)
        }

        // compare decimal by subtract.
        // Requirements: both have the same sign and should not be 0
        fn var_dec_cmp_sub(d1: &Decimal128, d2: &Decimal128) -> Ordering {
            let mut is_negative = d1.is_negative();
            let mut scale: i32 = d2.exponent() as i32 - d1.exponent() as i32;

            let mut mantissa = d1.mantissa();
            let mut d2mantissa = d2.mantissa();

            if scale != 0 {
                // Exponents are not equal.
                // Assume that a larger scale factor (more decimal places)
                // is likely to mean that number is smaller.
                // Start by guessing that the right operand has the larger scale factor.
                if scale < 0 {
                    // Guessed scale factor wrong. Swap operands.
                    // Swap operands will swap the result so negotiate both operands will correct this
                    scale = -scale;
                    is_negative = !is_negative;
                    (mantissa, d2mantissa) = (d2mantissa, mantissa);
                }

                // D1 will need to be multiplied by 10^scale so it will have the same scale as d2.
                // Rust has u128 so use them to calculate

                let power = POWERS_10[scale as usize];

                let Some(tmp128) = mantissa
                    .checked_mul(power)
                    .take_if(|&mut x| x < Decimal128::MANTISSA_MASK)
                else {
                    // d1 mantissa overflows if exponent is the same as d2, so this means
                    // the absolute value of d1 is grater than its of d2.
                    // if they are negative, this means d1 is less and greater otherwise.
                    return if is_negative {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    };
                };
                mantissa = tmp128;
            }

            let mut cmp = mantissa.cmp(&d2mantissa);
            if is_negative {
                cmp = cmp.reverse();
            }
            cmp
        }

        var_dec_cmp(self, other)
    }
}

macro_rules! from_primitive {
    (unsigned $ty: ty) => {
        impl From<$ty> for Decimal128 {
            fn from(value: $ty) -> Self {
                Self::new(value.into(), 0, false)
            }
        }
    };
    (signed $ty: ty) => {
        impl From<$ty> for Decimal128 {
            fn from(value: $ty) -> Self {
                Self::new(value.unsigned_abs().into(), 0, value.is_negative())
            }
        }
    };
}

from_primitive!(unsigned u8);
from_primitive!(unsigned u16);
from_primitive!(unsigned u32);
from_primitive!(unsigned u64);
from_primitive!(signed i8);
from_primitive!(signed i16);
from_primitive!(signed i32);
from_primitive!(signed i64);

pub struct TryFromDecimal128Error(());

impl Debug for TryFromDecimal128Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("Converting to Decimal128 overflows")
    }
}

impl Display for TryFromDecimal128Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("Converting to Decimal128 overflows")
    }
}

impl std::error::Error for TryFromDecimal128Error {}

impl TryFrom<f64> for Decimal128 {
    type Error = TryFromDecimal128Error;

    fn try_from(value: f64) -> Result<Self, TryFromDecimal128Error> {
        return var_dec_from_r8(value);
        // based on https://github.com/dotnet/runtime/blob/e51af404d1ea26be4a3d8e51fe21cf2f09ad34dd/src/libraries/System.Private.CoreLib/src/System/Decimal.DecCalc.cs#L1664

        /// <summary>
        /// Convert double to Decimal
        /// </summary>
        fn var_dec_from_r8(mut input: f64) -> Result<Decimal128, TryFromDecimal128Error> {
            static DOUBLE_POWERS10: &[f64] = &[
                1f64, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14,
                1e15, 1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22, 1e23, 1e24, 1e25, 1e26, 1e27, 1e28,
                1e29, 1e30, 1e31, 1e32, 1e33, 1e34, 1e35, 1e36, 1e37, 1e38, 1e39, 1e40, 1e41, 1e42,
                1e43, 1e44, 1e45, 1e46, 1e47, 1e48, 1e49, 1e50, 1e51, 1e52, 1e53, 1e54, 1e55, 1e56,
                1e57, 1e58, 1e59, 1e60, 1e61, 1e62, 1e63, 1e64, 1e65, 1e66, 1e67, 1e68, 1e69, 1e70,
                1e71, 1e72, 1e73, 1e74, 1e75, 1e76, 1e77, 1e78, 1e79, 1e80,
            ];

            // The most we can scale by is 10^28, which is just slightly more
            // than 2^93.  So a float with an exponent of -94 could just
            // barely reach 0.5, but smaller exponents will always round to zero.
            //
            const DBLBIAS: u32 = 1022;
            let exp: i32 = get_exponent(input) as i32 - DBLBIAS as i32;
            if exp < -94 {
                return Ok(Decimal128::ZERO); // result should be zeroed out
            }

            if exp > 96 {
                return Err(TryFromDecimal128Error(()));
            }

            let mut is_negative = false;
            if input < 0.0 {
                input = -input;
                is_negative = true;
            }

            // Round the input to a 15-digit integer.  The R8 format has
            // only 15 digits of precision, and we want to keep garbage digits
            // out of the Decimal were making.
            //
            // Calculate max power of 10 input value could have by multiplying
            // the exponent by log10(2).  Using scaled integer multiplcation,
            // log10(2) * 2 ^ 16 = .30103 * 65536 = 19728.3.
            //
            let mut dbl = input;
            let mut power = 14 - ((exp * 19728) >> 16);
            // power is between -14 and 43

            #[allow(clippy::collapsible_else_if)]
            if power >= 0 {
                // We have less than 15 digits, scale input up.
                //
                if power > DEC_SCALE_MAX as i32 {
                    power = DEC_SCALE_MAX as i32;
                }

                dbl *= DOUBLE_POWERS10[power as usize];
            } else {
                if power != -1 || dbl >= 1E15 {
                    dbl /= DOUBLE_POWERS10[-power as usize];
                } else {
                    power = 0; // didn't scale it
                }
            }

            debug_assert!(dbl < 1E15);
            if dbl < 1E14 && power < DEC_SCALE_MAX as i32 {
                dbl *= 10.0;
                power += 1;
                debug_assert!(dbl >= 1E14);
            }

            // Round to int64
            //
            let mut mant: u64;
            // with SSE4.1 support ROUNDSD can be used
            //if (X86.Sse41.IsSupported) {
            //    mant = (ulong)(long)Math.Round(dbl);
            //} else
            {
                mant = dbl as i64 as u64;
                dbl -= mant as i64 as f64; // difference between input & integer
                if dbl > 0.5 || dbl == 0.5 && (mant & 1) != 0 {
                    mant += 1;
                }
            }

            if mant == 0 {
                return Ok(Decimal128::ZERO); // result should be zeroed out
            }

            if power < 0 {
                // Add -power factors of 10, -power <= (29 - 15) = 14.
                power = -power;

                let mantissa = (mant as u128) * (POWERS_10[power as usize]);

                Ok(Decimal128::new(mantissa, 0, is_negative))
            } else {
                // Factor out powers of 10 to reduce the scale, if possible.
                // The maximum number we could factor out would be 14.  This
                // comes from the fact we have a 15-digit number, and the
                // MSD must be non-zero -- but the lower 14 digits could be
                // zero.  Note also the scale factor is never negative, so
                // we can't scale by any more than the power we used to
                // get the integer.
                //
                let mut lmax = power;
                if lmax > 14 {
                    lmax = 14;
                }

                macro_rules! div_when_possible {
                    // $actual_value must be 10 ^ $digits
                    // $fast_mask must be one smaller than biggest 2^x divisor of $actual_value
                    ($fast_mask: literal, $actual_value: literal, $digits: literal) => {
                        // (mant & $fast_mask) == 0 checks for it can be divided by ($fast_mask + 1)
                        // Which is biggest 2^x divisor of $actual_value
                        if (mant & $fast_mask) == 0 && lmax >= $digits {
                            const DEN: u64 = $actual_value;
                            let div = mant / DEN;
                            #[allow(unused_assignments)] // inside macro
                            if mant & 0xFFFFFFFF == (div * DEN) & 0xFFFFFFFF {
                                mant = div;
                                power -= $digits;
                                lmax -= $digits;
                            }
                        }
                    };
                }

                div_when_possible!(0xFF, 100000000, 8);
                div_when_possible!(0xF, 10000, 4);
                div_when_possible!(0x3, 100, 2);
                div_when_possible!(0x1, 10, 1);

                Ok(Decimal128::new(mant as u128, power as u32, is_negative))
            }
        }

        fn get_exponent(d: f64) -> u32 {
            // Based on pulling out the exp from this double struct layout
            // typedef struct {
            //   DWORDLONG mant:52;
            //   DWORDLONG signexp:12;
            // } DBLSTRUCT;

            ((d.to_bits() >> 52) & 0x7FF) as u32
        }
    }
}

impl Display for Decimal128 {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let sign = if self.is_negative() { "-" } else { "" };
        let mantissa = self.mantissa().to_string();
        let exponent = self.exponent();
        if exponent == 0 {
            // no decimals
            write!(f, "{}{}", sign, mantissa)
        } else if mantissa.len() > exponent as usize {
            // insert '.' to proper position
            let dot = mantissa.len() - exponent as usize;
            f.write_str(&mantissa[..dot])?;
            f.write_str(".")?;
            f.write_str(&mantissa[dot..])
        } else {
            // print 0.0... and then mantissa
            let zero_len = exponent as usize - mantissa.len();
            let zeros = "0.0000000000000000000000000000";
            f.write_str(&zeros[..(zero_len + 2)])?;
            f.write_str(&mantissa)
        }
    }
}

impl Neg for Decimal128 {
    type Output = Decimal128;
    fn neg(self) -> Self::Output {
        Self::new(self.mantissa(), self.exponent() as u32, !self.is_negative())
    }
}

impl Add for Decimal128 {
    type Output = Decimal128;

    fn add(self, rhs: Self) -> Self::Output {
        let mut mantissa1 = self.mantissa_signed();
        let mut exponent1 = self.exponent();
        let mut mantissa2 = rhs.mantissa_signed();
        let mut exponent2 = rhs.exponent();

        let mut scale = exponent2 as i8 - exponent1 as i8;

        if scale == 0 {
            // Scale factors are equal, no alignment necessary.
            let mut exponent = exponent1;
            let mut mantissa = mantissa1 + mantissa2;
            if mantissa.unsigned_abs() > Self::MANTISSA_MASK {
                if exponent == 0 {
                    panic!("decimal overflows");
                }
                mantissa /= 10;
                exponent -= 1;
            }
            Self::new_signed(mantissa, exponent as u32)
        } else {
            // Scale factors are not equal.  Assume that a larger scale
            // factor (more decimal places) is likely to mean that number
            // is smaller.  Start by guessing that the right operand has
            // the larger scale factor.  The result will have the larger
            // scale factor.
            //

            if scale < 0 {
                // Guessed scale factor wrong. Swap operands.
                (mantissa1, mantissa2) = (mantissa2, mantissa1);
                (exponent1, exponent2) = (exponent2, exponent1);
                scale = -scale;
            }

            debug_assert!(scale > 0);
            debug_assert!(exponent1 < exponent2);

            if let Some(mantissa1) = mantissa1.checked_mul(POWERS_10[scale as usize] as i128) {
                // not overflows

                let mut exponent = exponent2;
                let mut mantissa = mantissa1.checked_add(mantissa2).unwrap_or_else(|| {
                    // overflow wadding two mantissa. divide by 10 to avoid overflow
                    exponent -= 1;
                    mantissa1 / 10 + mantissa2 / 10
                });

                // if mantissa is too big, fix
                while mantissa.unsigned_abs() > Self::MANTISSA_MASK {
                    mantissa /= 10;
                    exponent -= 1;
                }
                Self::new_signed(mantissa, exponent as u32)
            } else {
                // The mantissa overflow.

                // factor * mantissa1 as 192-bit integer.
                //
                // We won't overflow mantissa1 * factor since
                // max value of POWERS_10 is 10000000000000000000000000000,
                // and log2(79228162514264337593543950335 * 10000000000000000000000000000)
                // is 189.01 which means 190 bit is enough

                let factor = POWERS_10[scale as usize];
                let mantissa = I192::mul_u96(factor, mantissa1.unsigned_abs());
                let mantissa = if mantissa1 < 0 {
                    mantissa.neg()
                } else {
                    mantissa
                };
                let mantissa = mantissa.add_i128(mantissa2).0;

                // Scale until we get mantissa less than max mantissa.
                // For simpler implementation, we split the sign flag and actual data.
                let (negative, mantissa) = if mantissa.is_negative() {
                    (true, mantissa.neg())
                } else {
                    (false, mantissa)
                };

                let (mid_low, exponent) = scale_result(mantissa, exponent2 as u32);

                Self::new(mid_low, exponent, negative)
            }
        }
    }
}

impl Sub for Decimal128 {
    type Output = Decimal128;

    fn sub(self, rhs: Decimal128) -> Decimal128 {
        self + -rhs
    }
}

impl Mul for Decimal128 {
    type Output = Decimal128;

    fn mul(self, rhs: Decimal128) -> Decimal128 {
        // self = l_sign * l_mantissa * 10^-l_exponent
        // rhs  = r_sign * r_mantissa * 10^-r_exponent
        // self * rhs = (l_sign * r_sign)
        //            * (l_mantissa * r_mantissa)
        //            * 10^-(l_exponent + r_exponent)

        let sign = self.is_negative() ^ rhs.is_negative();
        let mantissa = I192::mul_u96(self.mantissa(), rhs.mantissa());
        let exponent = (self.exponent() + rhs.exponent()) as u32;

        let (mantissa, exponent) = if mantissa <= const { I192::from_u128(Self::MANTISSA_MASK) }
            && exponent < DEC_SCALE_MAX
        {
            (mantissa.to_u128(), exponent)
        } else {
            scale_result(mantissa, exponent)
        };

        Self::new(mantissa, exponent, sign)
    }
}

impl Div for Decimal128 {
    type Output = Decimal128;

    fn div(self, rhs: Decimal128) -> Decimal128 {
        //uint power;
        //let mut curScale;

        let sign = self.is_negative() ^ rhs.is_negative();
        let mut scale = self.exponent() as i32 - rhs.exponent() as i32;

        if rhs.mantissa() == 0 {
            panic!("divide by zero")
        }

        let r_mantissa = rhs.mantissa();

        let quotient = self.mantissa() / r_mantissa;
        let reminder = self.mantissa() % r_mantissa;

        if reminder == 0 {
            // very simple! we've divided successfully
            if scale < 0 {
                // scale < 0 means the result is big. we have to upscale the value.

                let factor = POWERS_10[-scale as usize];
                let Some(mantissa) = quotient.checked_mul(factor) else {
                    panic!("decimal overflow");
                };
                if mantissa > Self::MANTISSA_MASK {
                    panic!("decimal overflow");
                }

                Self::new(mantissa, 0, sign)
            } else {
                Self::new(quotient, scale as u32, sign)
            }
        } else {
            // we have a reminder so we continue for remaining bits.
            let (lower96, mut reminder) = I192::div_shifted_u96(reminder, r_mantissa);

            // quotient express fixed point at 96 bit
            let mut quotient = I192::from_u92_pair(quotient, lower96);

            // scale up to scale = 0
            while scale < DEC_SCALE_MAX as i32 && (scale < 0 || quotient.low96() != 0) {
                let Some(quotient1) = quotient.checked_mul_u96(10) else {
                    break;
                };
                let lower = reminder * 10 / r_mantissa;
                let reminder1 = reminder * 10 % r_mantissa;
                let (quotient1, overflow) = quotient1.add_i128(lower as i128);
                if overflow {
                    break;
                }

                quotient = quotient1;
                reminder = reminder1;
                scale += 1;
            }

            if scale < 0 {
                panic!("scale overflow");
            }

            let mut mantissa = quotient.high96();

            if quotient.low96() > 0x80000000_00000000_00000000
                || (quotient.low96() == 0x80000000_00000000_00000000 && mantissa & 1 != 0)
            {
                if let Some(mantissa_new) = mantissa.checked_add(1) {
                    mantissa = mantissa_new;
                } else {
                    // the mantissa overflows. scale down by one
                    mantissa /= 10;
                    scale -= 1;
                }
            }

            // remove trailing zero
            while mantissa % 10 == 0 && scale > 0 {
                mantissa /= 10;
                scale -= 1;
            }

            Self::new(mantissa, scale as u32, sign)
        }
    }
}

fn scale_result(mut mantissa: I192, scale: u32) -> (u128, u32) {
    let mut scale = scale as i32;
    // based on ScaleResult
    // https://github.com/dotnet/runtime/blob/ec118c7e798862fd69dc7fa6544c0d9849d32488/src/libraries/System.Private.CoreLib/src/System/Decimal.DecCalc.cs#L607-L765

    // See if we need to scale the result. The combined scale must
    // be <= DEC_SCALE_MAX and the upper 96 bits must be zero.
    //
    // Start by figuring a lower bound on the scaling needed to make
    // the upper 96 bits zero.  hiRes is the index into result[]
    // of the highest non-zero uint.
    //
    let mut new_scale: i32;
    {
        let zeros = mantissa.leading_zeros();
        new_scale = 5 * 32 - 64 - 1 - zeros as i32;

        // Multiply bit position by log10(2) to figure it's power of 10.
        // We scale the log by 256.  log(2) = .30103, * 256 = 77.  Doing this
        // with a multiply saves a 96-byte lookup table.  The power returned
        // is <= the power of the number, so we must add one power of 10
        // to make it's integer part zero after dividing by 256.
        //
        // Note: the result of this multiplication by an approximation of
        // log10(2) have been exhaustively checked to verify it gives the
        // correct result.  (There were only 95 to check...)
        //
        new_scale = ((new_scale * 77) >> 8) + 1;

        // new_scale = min scale factor to make high 96 bits zero, 0 - 29.
        // This reduces the scale factor of the result.  If it exceeds the
        // current scale of the result, we'll overflow.
        //
        if new_scale > scale {
            panic!("decimal overflow");
        }
    }

    // Make sure we scale by enough to bring the current scale factor
    // into valid range.
    //
    if new_scale < scale - DEC_SCALE_MAX as i32 {
        new_scale = scale - DEC_SCALE_MAX as i32;
    }

    if new_scale != 0 {
        // Scale by the power of 10 given by new_scale.  Note that this is
        // NOT guaranteed to bring the number within 96 bits -- it could
        // be 1 power of 10 short.
        //
        scale -= new_scale;
        let mut sticky = 0;
        let mut remainder = 0;

        loop {
            sticky |= remainder; // record remainder as sticky bit

            let mut power;
            // Scaling loop specialized for each power of 10 because division by constant is an order of magnitude faster (especially for 64-bit division that's actually done by 128bit DIV on x64)
            #[allow(clippy::inconsistent_digit_grouping)]
            match new_scale {
                1_ => (mantissa, remainder, power) = mantissa.div(10),
                2_ => (mantissa, remainder, power) = mantissa.div(100),
                3_ => (mantissa, remainder, power) = mantissa.div(1000),
                4_ => (mantissa, remainder, power) = mantissa.div(10000),
                5_ => (mantissa, remainder, power) = mantissa.div(100000),
                6_ => (mantissa, remainder, power) = mantissa.div(1000000),
                7_ => (mantissa, remainder, power) = mantissa.div(10000000),
                8_ => (mantissa, remainder, power) = mantissa.div(100000000),
                9_ => (mantissa, remainder, power) = mantissa.div(1000000000),
                10 => (mantissa, remainder, power) = mantissa.div(10000000000),
                11 => (mantissa, remainder, power) = mantissa.div(100000000000),
                12 => (mantissa, remainder, power) = mantissa.div(1000000000000),
                13 => (mantissa, remainder, power) = mantissa.div(10000000000000),
                14 => (mantissa, remainder, power) = mantissa.div(100000000000000),
                15 => (mantissa, remainder, power) = mantissa.div(1000000000000000),
                16 => (mantissa, remainder, power) = mantissa.div(10000000000000000),
                17 => (mantissa, remainder, power) = mantissa.div(100000000000000000),
                18 => (mantissa, remainder, power) = mantissa.div(1000000000000000000),
                _ => (mantissa, remainder, power) = mantissa.div(10000000000000000000),
            }

            new_scale -= 19;
            if new_scale > 0 {
                continue; // scale some more
            }

            // If we scaled enough, hiRes would be 2 or less.  If not,
            // divide by 10 more.
            //
            if mantissa > const { I192::from_u128(Decimal128::MANTISSA_MASK) } {
                if scale == 0 {
                    panic!("decimal overflow");
                }
                new_scale = 1;
                scale -= 1;
                continue; // scale by 10
            }

            // Round final result.  See if remainder >= 1/2 of divisor.
            // If remainder == 1/2 divisor, round up if odd or sticky bit set.
            //
            power >>= 1; // power of 10 always even
            if power <= remainder && (power < remainder || ((mantissa.low & 1) | sticky) != 0) {
                mantissa = mantissa.add_i128(1).0;
                if mantissa > const { I192::from_u128(Decimal128::MANTISSA_MASK) } {
                    // The rounding caused us to carry beyond 96 bits.
                    // Scale by 10 more.
                    //
                    if scale == 0 {
                        panic!("decimal overflow");
                    }
                    sticky = 0; // no sticky bit
                    remainder = 0; // or remainder
                    new_scale = 1;
                    scale -= 1;
                    continue; // scale by 10
                }
            }

            break;
        } // while (true)
    }

    (mantissa.to_u128(), scale as u32)
}

#[derive(Copy, Clone, Eq, PartialEq)]
struct I192 {
    high: u64,
    mid: u64,
    low: u64,
}

impl I192 {
    const fn from_u128(value: u128) -> I192 {
        Self {
            high: 0,
            mid: (value >> 64) as u64,
            low: (value & u64::MAX as u128) as u64,
        }
    }

    fn from_u92_pair(high: u128, low: u128) -> I192 {
        Self {
            high: (high >> 32) as u64,
            mid: (((high & u32::MAX as u128) as u64) << 32) | ((low >> 64) as u64),
            low: (low & u64::MAX as u128) as u64,
        }
    }

    fn is_negative(self) -> bool {
        (self.high & 0x80000000_00000000) != 0
    }

    fn leading_zeros(self) -> u32 {
        if self.high != 0 {
            self.high.leading_zeros()
        } else if self.mid != 0 {
            self.mid.leading_zeros() + 64
        } else {
            self.low.leading_zeros() + 64 * 2
        }
    }

    fn mul_u96(left: u128, right: u128) -> Self {
        //
        //                 l_m       l_l
        //  X              r_m       r_l
        // ----------------------------------
        //              ======l_l*r_l======
        //  ======l_m*r_l======
        //  ======l_l*r_m======
        //   l_m*r_m
        // ----------------------------------
        //  ======mid_high=====      =low=
        //

        let l_l = left & (u64::MAX as u128);
        let l_m = (left >> 64) & (u64::MAX as u128);

        let r_l = right & (u64::MAX as u128);
        let r_m = (right >> 64) & (u64::MAX as u128);

        let low_mid = l_l * r_l;
        let low = (low_mid & (u64::MAX as u128)) as u64;

        let mid_high = (low_mid >> 64) + l_l * r_m + l_m * r_l + ((l_m * r_m) << 64);

        let mid = (mid_high & (u64::MAX as u128)) as u64;
        let high = (mid_high >> 64) as u64;

        Self { high, mid, low }
    }

    fn checked_mul_u96(self, right: u128) -> Option<Self> {
        //
        //                              s_h       s_m       s_l
        //  X                                     r_m       r_l
        // ---------------------------------------------------------
        //                                     ======s_l*r_l======  |               mid0, low0 |
        //                           ======s_m*r_l======            |        high1, mid1       |
        //                           ======s_l*r_m======            |        high2, mid2       |
        //                 ======s_h*r_l======                      | over3, high3             |
        //                 ======s_m*r_m======                      | over4, high4             |
        //       ======s_h*r_m======              <=== this is overflow so either must have zero
        //

        let r_l = right & (u64::MAX as u128);
        let r_m = (right >> 64) & (u64::MAX as u128);

        let s_h = self.high as u128;
        let s_m = self.mid as u128;
        let s_l = self.low as u128;

        if s_h != 0 && r_m != 0 {
            return None; // overflow
        }

        #[inline]
        fn split(v: u128) -> (u64, u64) {
            ((v >> 64) as u64, (v & u64::MAX as u128) as u64)
        }

        let (mid0, low0) = split(s_l * r_l);
        let (high1, mid1) = split(s_m * r_l);
        let (high2, mid2) = split(s_l * r_m);
        let (over3, high3) = split(s_h * r_l);
        let (over4, high4) = split(s_m * r_m);

        if over3 != 0 || over4 != 0 {
            return None; // overflow
        }

        let low = low0;
        let (carry_to_high, mid) = {
            let (tmp, carry0) = mid0.overflowing_add(mid1);
            let (tmp, carry1) = tmp.overflowing_add(mid2);
            let carry = carry0 as u64 + carry1 as u64;
            (carry, tmp)
        };
        let (carry, high) = {
            let (tmp, carry0) = high1.overflowing_add(high2);
            let (tmp, carry1) = tmp.overflowing_add(high3);
            let (tmp, carry2) = tmp.overflowing_add(high4);
            let (tmp, carry3) = tmp.overflowing_add(carry_to_high);
            let carry = carry0 as u64 + carry1 as u64 + carry2 as u64 + carry3 as u64;
            (carry, tmp)
        };
        if carry != 0 {
            return None; // overflow
        }
        Some(Self { low, mid, high })
    }

    fn add_overflowing(self, rhs: Self) -> (Self, bool) {
        fn full_adder(a: u64, b: u64, carry: bool) -> (u64, bool) {
            let (tmp, c0) = a.overflowing_add(b);
            let (tmp, c1) = tmp.overflowing_add(carry as u64);
            (tmp, c0 || c1)
        }

        let (low, mid_carry) = full_adder(self.low, rhs.low, false);
        let (mid, high_carry) = full_adder(self.mid, rhs.mid, mid_carry);
        let (high, carry) = full_adder(self.high, rhs.high, high_carry);
        (Self { low, mid, high }, carry)
    }

    fn div_shifted_u96(reminder: u128, right: u128) -> (u128, u128) {
        if reminder < u32::MAX as u128 {
            // we can shift 96 bit without overflow; we can process at once.
            let tmp = reminder << 96;
            let lower96 = tmp / right;
            let reminder = tmp % right;

            (lower96, reminder)
        } else if reminder < u64::MAX as u128 {
            // we shift 64 bit, and then process lower 32bit.
            let tmp = reminder << 64;
            let mid64 = tmp / right;
            let reminder = tmp % right;

            let tmp = reminder << 32;
            let low32 = tmp / right;
            let reminder = tmp % right;

            debug_assert!(mid64 <= u64::MAX as u128);
            debug_assert!(low32 <= u32::MAX as u128);

            let lower96 = (mid64 << 32) | low32;
            (lower96, reminder)
        } else {
            // we process 32bit at one time.
            let tmp = reminder << 32;
            let high32 = tmp / right;
            let reminder = tmp % right;

            let tmp = reminder << 32;
            let mid32 = tmp / right;
            let reminder = tmp % right;

            let tmp = reminder << 32;
            let low32 = tmp / right;
            let reminder = tmp % right;

            debug_assert!(high32 <= u32::MAX as u128);
            debug_assert!(mid32 <= u32::MAX as u128);
            debug_assert!(low32 <= u32::MAX as u128);

            let lower96 = (high32 << 64) | (mid32 << 32) | low32;
            (lower96, reminder)
        }
    }

    fn add_i128(self, value: i128) -> (Self, bool) {
        let u_value = value as u128;
        let low = (u_value & u64::MAX as u128) as u64;
        let mid = (u_value >> 64) as u64;
        let high = if value < 0 { u64::MAX } else { 0 };

        fn full_adder(a: u64, b: u64, carry: bool) -> (u64, bool) {
            let (tmp, c0) = a.overflowing_add(b);
            let (tmp, c1) = tmp.overflowing_add(carry as u64);
            (tmp, c0 || c1)
        }

        let (new_low, mid_carry) = full_adder(self.low, low, false);
        let (new_mid, high_carry) = full_adder(self.mid, mid, mid_carry);
        let (new_high, carry) = full_adder(self.high, high, high_carry);
        (
            Self {
                low: new_low,
                mid: new_mid,
                high: new_high,
            },
            carry,
        )
    }

    fn neg(self) -> Self {
        let (low, mid_carry) = (!self.low).overflowing_add(1);
        let (mid, high_carry) = (!self.mid).overflowing_add(mid_carry as u64);
        let (high, carry) = (!self.high).overflowing_add(high_carry as u64);
        let _ = carry;

        Self { low, mid, high }
    }

    fn to_u128(self) -> u128 {
        self.low as u128 | ((self.mid as u128) << 64)
    }

    fn high96(self) -> u128 {
        ((self.high as u128) << 32) | ((self.mid as u128) >> 32)
    }

    fn low96(self) -> u128 {
        self.low as u128 | ((self.mid as u128 & u32::MAX as u128) << 64)
    }

    #[inline]
    fn div(self, power: u64) -> (Self, u64, u64) {
        let mut remainder;

        let (high, mid, low);

        high = self.high / power;
        remainder = self.high % power;

        {
            let num = self.mid as u128 + ((remainder as u128) << 64);
            let tmp = (num / power as u128) as u64;
            mid = tmp;
            remainder = self.mid.wrapping_sub(tmp.wrapping_mul(power));
        }

        {
            let num = self.low as u128 + ((remainder as u128) << 64);
            let tmp = (num / power as u128) as u64;
            low = tmp;
            remainder = self.low.wrapping_sub(tmp.wrapping_mul(power));
        }

        (Self { high, mid, low }, remainder, power)
    }
}

impl PartialOrd for I192 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for I192 {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.is_negative().cmp(&other.is_negative()).reverse())
            .then_with(|| self.high.cmp(&other.high))
            .then_with(|| self.mid.cmp(&other.mid))
            .then_with(|| self.low.cmp(&other.low))
    }
}

#[test]
fn construct_test() {
    macro_rules! construct_test {
        (
            $mantissa: literal,
            $exponent: literal,
            $is_negative: literal,
            $binary: expr
        ) => {
            let from_binary = Decimal128::from_bytes($binary).unwrap();
            let from_tuple = Decimal128::new($mantissa, $exponent, $is_negative);
            assert_eq!(from_binary, from_tuple);
            assert_eq!(from_binary.bytes(), from_tuple.bytes());
            assert_eq!(from_binary.mantissa(), $mantissa);
            assert_eq!(from_binary.exponent(), $exponent);
            assert_eq!(from_binary.is_negative(), $is_negative);
        };
    }

    macro_rules! parse_test {
        ($s: expr, $binary: expr) => {
            assert_eq!(Decimal128::parse($s).unwrap().bytes(), $binary);
        };
    }

    // The arguments are same as C# compiler generated one except for we use 128-bit integer
    // binary part is coming from C# with
    // Decimal.GetBits(value).SelectMany(BitConverter.GetBytes).ToArray()
    construct_test!(
        5,
        0,
        false,
        [5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    );
    construct_test!(
        50,
        1,
        false,
        [50, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0]
    );
    construct_test!(
        51,
        1,
        false,
        [51, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0]
    );

    parse_test!("5", [5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    parse_test!("5.0", [50, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0]);
    parse_test!("5.1", [51, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0]);
    parse_test!("5.1", [51, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0]);

    assert_eq!(
        decimal!(5).bytes(),
        [5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    );
    assert_eq!(
        decimal!(5.).bytes(),
        [5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    );
    assert_eq!(
        decimal!(5.0).bytes(),
        [50, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0]
    );
    assert_eq!(
        decimal!(5.1).bytes(),
        [51, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0]
    );
    assert_eq!(
        decimal!(0.1).bytes(),
        [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0]
    );
    assert_eq!(
        decimal!(0.10).bytes(),
        [10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0]
    );

    // max and min value
    assert_eq!(
        Decimal128::MAX.bytes(),
        [
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0
        ]
    );
    assert_eq!(
        Decimal128::MAX.bytes(),
        decimal!(79228162514264337593543950335).bytes()
    );
    assert_eq!(
        Decimal128::MIN.bytes(),
        [
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 128
        ]
    );
    assert_eq!(
        Decimal128::MIN.bytes(),
        decimal!(-79228162514264337593543950335).bytes()
    );
}

#[test]
fn from_f64_test() {
    // very basic ones
    assert_eq!(Decimal128::try_from(10.0).unwrap(), decimal!(10.0));
    assert_eq!(Decimal128::try_from(10.1).unwrap(), decimal!(10.1));
    assert_eq!(Decimal128::try_from(0.1).unwrap(), decimal!(0.1));

    // copied from .NET
    // https://github.com/dotnet/runtime/blob/e51af404d1ea26be4a3d8e51fe21cf2f09ad34dd/src/libraries/System.Runtime/tests/System.Runtime.Tests/System/DecimalTests.cs#L177-L211

    macro_rules! test_cs {
        ($double: literal, $converted: expr) => {
            let double: f64 = $double;
            let converted: [i32; 4] = $converted;
            let converted_bytes = {
                let mut tmp = [0u8; 16];
                tmp[0..][..4].copy_from_slice(&converted[0].to_le_bytes());
                tmp[4..][..4].copy_from_slice(&converted[1].to_le_bytes());
                tmp[8..][..4].copy_from_slice(&converted[2].to_le_bytes());
                tmp[12..][..4].copy_from_slice(&converted[3].to_le_bytes());
                tmp
            };

            let after = Decimal128::try_from(double).unwrap();
            assert_eq!(after.bytes(), converted_bytes);
        };
    }

    test_cs!(123456789.123456, [-2045800064, 28744, 0, 393216]);
    test_cs!(2.0123456789123456, [-1829795549, 46853, 0, 917504]);
    test_cs!(2E-28, [2, 0, 0, 1835008]);
    test_cs!(2E-29, [0, 0, 0, 0]);
    test_cs!(2E28, [536870912, 2085225666, 1084202172, 0]);
    test_cs!(1.5, [15, 0, 0, 65536]);
    test_cs!(0.0, [0, 0, 0, 0]);
    test_cs!(-0.0, [0, 0, 0, 0]);

    test_cs!(100000000000000.1, [276447232, 23283, 0, 0]);
    test_cs!(10000000000000.1, [276447233, 23283, 0, 65536]);
    test_cs!(1000000000000.1, [1316134913, 2328, 0, 65536]);
    test_cs!(100000000000.1, [-727379967, 232, 0, 65536]);
    test_cs!(10000000000.1, [1215752193, 23, 0, 65536]);
    test_cs!(1000000000.1, [1410065409, 2, 0, 65536]);
    test_cs!(100000000.1, [1000000001, 0, 0, 65536]);
    test_cs!(10000000.1, [100000001, 0, 0, 65536]);
    test_cs!(1000000.1, [10000001, 0, 0, 65536]);
    test_cs!(100000.1, [1000001, 0, 0, 65536]);
    test_cs!(10000.1, [100001, 0, 0, 65536]);
    test_cs!(1000.1, [10001, 0, 0, 65536]);
    test_cs!(100.1, [1001, 0, 0, 65536]);
    test_cs!(10.1, [101, 0, 0, 65536]);
    test_cs!(1.1, [11, 0, 0, 65536]);
    test_cs!(1.0, [1, 0, 0, 0]);
    test_cs!(0.1, [1, 0, 0, 65536]);
    test_cs!(0.01, [1, 0, 0, 131072]);
    test_cs!(0.001, [1, 0, 0, 196608]);
    test_cs!(0.0001, [1, 0, 0, 262144]);
    test_cs!(0.00001, [1, 0, 0, 327680]);
    test_cs!(0.0000000000000000000000000001, [1, 0, 0, 1835008]);
    test_cs!(0.00000000000000000000000000001, [0, 0, 0, 0]);

    // overflows
    assert!(Decimal128::try_from(f64::NAN).is_err());
    assert!(Decimal128::try_from(f64::MAX).is_err());
    assert!(Decimal128::try_from(f64::MIN).is_err());
    assert!(Decimal128::try_from(f64::INFINITY).is_err());
    assert!(Decimal128::try_from(f64::NEG_INFINITY).is_err());
}

#[test]
fn cmp_test() {
    macro_rules! cmp_test {
        ($left: expr, $right: expr, $cmp: ident) => {
            let left: Decimal128 = $left.into();
            let right: Decimal128 = $right.into();
            let comp: Ordering = Ordering::$cmp;

            assert_eq!(left.cmp(&right), comp);
            assert_eq!(right.cmp(&left), comp.reverse());
        };
    }

    // exponent == 0 patterns
    cmp_test!(0, 0, Equal);
    cmp_test!(-1, 0, Less);
    cmp_test!(-10, -1, Less);
    cmp_test!(1, 0, Greater);
    cmp_test!(10, 1, Greater);

    // compare with 0
    cmp_test!(decimal!(0.1), 0, Greater);
    cmp_test!(decimal!(-0.1), 0, Less);

    // compare sign
    cmp_test!(1, -1, Greater);

    // match exponent
    cmp_test!(decimal!(0.1), decimal!(0.10), Equal);
    cmp_test!(decimal!(0.1), decimal!(0.11), Less);

    // overflow on match exponent
    cmp_test!(
        decimal!(7922816251426433.7593543950335),
        decimal!(79228162514264337593543950335),
        Less
    );
}

#[test]
fn display_test() {
    macro_rules! display_test {
        ($value: literal) => {
            let decimal = decimal!($value);
            let text = stringify!($value);
            assert_eq!(decimal.to_string(), text);
        };
    }
    display_test!(0);
    display_test!(1);
    display_test!(100);
    display_test!(1.0);
    display_test!(1.000);
    display_test!(7922816251426433.7593543950335);
    display_test!(79228162514264337593543950335);
    display_test!(0.1);
    display_test!(0.01);
}

#[test]
#[rustfmt::skip]
fn math_test() {
    fn eq_bitwise(left: Decimal128, right: Decimal128) {
        assert_eq!(left.cmp(&right), Ordering::Equal, "{left} and {right}");
        assert_eq!(left, right, "{left} and {right}");
        assert_eq!(left.repr, right.repr, "{left} and {right}");
    }

    // neg
    eq_bitwise(-decimal!(1.0), decimal!(-1.0));

    // add
    // basic
    eq_bitwise(decimal!(1) + decimal!(1), decimal!(2));
    // overflow
    eq_bitwise(decimal!(7922816251426433759354395033.5) + decimal!(7922816251426433759354395033.5), decimal!(15845632502852867518708790067));
    // fit size
    eq_bitwise(decimal!(1.0) + decimal!(2), decimal!(3.0));
    eq_bitwise(decimal!(1) + decimal!(2.0), decimal!(3.0));
    // overflow after fit
    eq_bitwise(decimal!(7922816251426433759354395033.0) + decimal!(7922816251426433759354395033), decimal!(15845632502852867518708790066));
    // 128-bit overflow on fit
    eq_bitwise(decimal!(1701411834604692317316873037) + decimal!(123213213.43176821145), decimal!(1701411834604692317440086250.4));
    // fit size very big amount
    eq_bitwise(decimal!(79228162514260000000000000000) + decimal!(2.00000000000000), decimal!(79228162514260000000000000002));
    eq_bitwise(decimal!(-79228162514260000000000000000) + decimal!(2.00000000000000), decimal!(-79228162514259999999999999998));
    // fit size very big amount (more than 19)
    eq_bitwise(decimal!(-79228162514260000000000000000) + decimal!(2.00000000000000000000000000), decimal!(-79228162514259999999999999998));
    // round to even
    eq_bitwise(decimal!(79228162514260000000000000000) + decimal!(2.50000000000000), decimal!(79228162514260000000000000002));
    eq_bitwise(decimal!(79228162514260000000000000000) + decimal!(3.50000000000000), decimal!(79228162514260000000000000004));
    // overflow on round to even
    eq_bitwise(decimal!(792281625142643375935439503) + decimal!(0.35500000000000), decimal!(792281625142643375935439503.4));
    // some other crash cases
    eq_bitwise(decimal!(340282366920938463463374607) + decimal!(1.431768211456), decimal!(340282366920938463463374608.43));
    // copied from .NET
    eq_bitwise(decimal!(1) + decimal!(1), decimal!(2));
    eq_bitwise(decimal!(-1) + decimal!(1), decimal!(0));
    eq_bitwise(decimal!(1) + decimal!(-1), decimal!(0));
    eq_bitwise(decimal!(1) + decimal!(0), decimal!(1));
    eq_bitwise(decimal!(79228162514264337593543950330) + decimal!(5), Decimal128::MAX);
    eq_bitwise(decimal!(79228162514264337593543950335) + decimal!(-5), decimal!(79228162514264337593543950330));
    eq_bitwise(decimal!(-79228162514264337593543950330) + decimal!(5), decimal!(-79228162514264337593543950325));
    eq_bitwise(decimal!(-79228162514264337593543950330) + decimal!(-5), Decimal128::MIN);
    eq_bitwise(decimal!(1234.5678) + decimal!(0.00009), decimal!(1234.56789));
    eq_bitwise(decimal!(-1234.5678) + decimal!(0.00009), decimal!(-1234.56771));
    eq_bitwise(decimal!(0.1111111111111111111111111111) + decimal!(0.1111111111111111111111111111), decimal!(0.2222222222222222222222222222));
    eq_bitwise(decimal!(0.5555555555555555555555555555) + decimal!(0.5555555555555555555555555555), decimal!(1.1111111111111111111111111110));
    eq_bitwise(Decimal128::MIN + Decimal128::ZERO, Decimal128::MIN);
    eq_bitwise(Decimal128::MAX + Decimal128::ZERO, Decimal128::MAX);

    // subtract
    // copied from .NET
    eq_bitwise(decimal!(1) - decimal!(1), decimal!(0));
    eq_bitwise(decimal!(1) - decimal!(0), decimal!(1));
    eq_bitwise(decimal!(0) - decimal!(1), decimal!(-1));
    eq_bitwise(decimal!(1) - decimal!(1), decimal!(0));
    eq_bitwise(decimal!(-1) - decimal!(1), decimal!(-2));
    eq_bitwise(decimal!(1) - decimal!(-1), decimal!(2));
    eq_bitwise(Decimal128::MAX - Decimal128::ZERO, Decimal128::MAX);
    eq_bitwise(Decimal128::MIN - Decimal128::ZERO, Decimal128::MIN);
    eq_bitwise(decimal!(79228162514264337593543950330) - decimal!(-5), Decimal128::MAX);
    eq_bitwise(decimal!(79228162514264337593543950330) - decimal!(5), decimal!(79228162514264337593543950325));
    eq_bitwise(decimal!(-79228162514264337593543950330) - decimal!(5), Decimal128::MIN);
    eq_bitwise(decimal!(-79228162514264337593543950330) - decimal!(-5), decimal!(-79228162514264337593543950325));
    eq_bitwise(decimal!(1234.5678) - decimal!(0.00009), decimal!(1234.56771));
    eq_bitwise(decimal!(-1234.5678) - decimal!(0.00009), decimal!(-1234.56789));
    eq_bitwise(decimal!(0.1111111111111111111111111111) - decimal!(0.1111111111111111111111111111), decimal!(0.0000000000000000000000000000));
    eq_bitwise(decimal!(0.2222222222222222222222222222) - decimal!(0.1111111111111111111111111111), decimal!(0.1111111111111111111111111111));
    eq_bitwise(decimal!(1.1111111111111111111111111110) - decimal!(0.5555555555555555555555555555), decimal!(0.5555555555555555555555555555));

    // multiply
    // copied from .NET
    eq_bitwise(decimal!(1) * decimal!(1), decimal!(1));
    eq_bitwise(decimal!(7922816251426433759354395033.5) * decimal!(10), Decimal128::MAX);
    eq_bitwise(decimal!(0.2352523523423422342354395033) * decimal!(56033525474612414574574757495), decimal!(13182018677937129120135020796));
    eq_bitwise(decimal!(46161363632634613634.093453337) * decimal!(461613636.32634613634083453337), decimal!(21308714924243214928823669051));
    eq_bitwise(decimal!(0.0000000000000345435353453563) * decimal!(0.0000000000000023525235234234), decimal!(0.0000000000000000000000000001));
    // copied from .NET: Near decimal.MaxValue
    eq_bitwise(decimal!(79228162514264337593543950335) * decimal!(0.9), decimal!(71305346262837903834189555302));
    eq_bitwise(decimal!(79228162514264337593543950335) * decimal!(0.99), decimal!(78435880889121694217608510832));
    eq_bitwise(decimal!(79228162514264337593543950335) * decimal!(0.9999999999999999999999999999), decimal!(79228162514264337593543950327));
    eq_bitwise(decimal!(-79228162514264337593543950335) * decimal!(0.9), decimal!(-71305346262837903834189555302));
    eq_bitwise(decimal!(-79228162514264337593543950335) * decimal!(0.99), decimal!(-78435880889121694217608510832));
    eq_bitwise(decimal!(-79228162514264337593543950335) * decimal!(0.9999999999999999999999999999), decimal!(-79228162514264337593543950327));

    // divide
    // divisible
    eq_bitwise(decimal!(1.50) / decimal!(2), decimal!(0.75));
    eq_bitwise(decimal!(1.5) / decimal!(0.01), decimal!(150));
    eq_bitwise(decimal!(1) / decimal!(0.03), decimal!(33.333333333333333333333333333));
    eq_bitwise(decimal!(1) / decimal!(0.06), decimal!(16.666666666666666666666666667));
    // copied from .NET
    eq_bitwise(decimal!(1) / decimal!(1), decimal!(1));
    eq_bitwise(decimal!(-1) / decimal!(-1), decimal!(1));
    eq_bitwise(decimal!(15) / decimal!(2), decimal!(7.5));
    eq_bitwise(decimal!(10) / decimal!(2), decimal!(5));
    eq_bitwise(decimal!(-10) / decimal!(-2), decimal!(5));
    eq_bitwise(decimal!(10) / decimal!(-2), decimal!(-5));
    eq_bitwise(decimal!(-10) / decimal!(2), decimal!(-5));
    eq_bitwise(decimal!(0.9214206543486529434634231456) / Decimal128::MAX, Decimal128::ZERO);
    eq_bitwise(decimal!(38214206543486529434634231456) / decimal!(0.4921420654348652943463423146), decimal!( 77648730371625094566866001277));
    eq_bitwise(decimal!(-78228162514264337593543950335) / Decimal128::MAX, decimal!(-0.987378225516463811113412343));
    eq_bitwise(Decimal128::MAX / decimal!(-1), Decimal128::MIN);
    eq_bitwise(Decimal128::MIN / Decimal128::MAX, decimal!(-1));
    eq_bitwise(Decimal128::MAX / Decimal128::MAX, decimal!(1));
    eq_bitwise(Decimal128::MIN / Decimal128::MIN, decimal!(1));
    // copied from .NET: Tests near MaxValue
    eq_bitwise(decimal!(792281625142643375935439503.4) / decimal!(0.1), decimal!(7922816251426433759354395034));
    eq_bitwise(decimal!(79228162514264337593543950.34) / decimal!(0.1), decimal!(792281625142643375935439503.4));
    eq_bitwise(decimal!(7922816251426433759354395.034) / decimal!(0.1), decimal!(79228162514264337593543950.34));
    eq_bitwise(decimal!(792281625142643375935439.5034) / decimal!(0.1), decimal!(7922816251426433759354395.034));
    eq_bitwise(decimal!(79228162514264337593543950335) / decimal!(10), decimal!(7922816251426433759354395033.5));
    eq_bitwise(decimal!(79228162514264337567774146561) / decimal!(10), decimal!(7922816251426433756777414656.1));
    eq_bitwise(decimal!(79228162514264337567774146560) / decimal!(10), decimal!(7922816251426433756777414656));
    eq_bitwise(decimal!(79228162514264337567774146559) / decimal!(10), decimal!(7922816251426433756777414655.9));
    eq_bitwise(decimal!(79228162514264337593543950335) / decimal!(1.1), decimal!(72025602285694852357767227577));
    eq_bitwise(decimal!(79228162514264337593543950335) / decimal!(1.01), decimal!(78443725261647859003508861718));
    eq_bitwise(decimal!(79228162514264337593543950335) / decimal!(1.001), decimal!(79149013500763574019524425909));
    eq_bitwise(decimal!(79228162514264337593543950335) / decimal!(1.0001), decimal!(79220240490215316061937756559));
    eq_bitwise(decimal!(79228162514264337593543950335) / decimal!(1.00001), decimal!(79227370240561931974224208093));
    eq_bitwise(decimal!(79228162514264337593543950335) / decimal!(1.000001), decimal!(79228083286181051412492537842));
    eq_bitwise(decimal!(79228162514264337593543950335) / decimal!(1.0000001), decimal!(79228154591448878448656105469));
    eq_bitwise(decimal!(79228162514264337593543950335) / decimal!(1.00000001), decimal!(79228161721982720373716746598));
    eq_bitwise(decimal!(79228162514264337593543950335) / decimal!(1.000000001), decimal!(79228162435036175158507775176));
    eq_bitwise(decimal!(79228162514264337593543950335) / decimal!(1.0000000001), decimal!(79228162506341521342909798201));
    eq_bitwise(decimal!(79228162514264337593543950335) / decimal!(1.00000000001), decimal!(79228162513472055968409229775));
    eq_bitwise(decimal!(79228162514264337593543950335) / decimal!(1.000000000001), decimal!(79228162514185109431029765226));
    eq_bitwise(decimal!(79228162514264337593543950335) / decimal!(1.0000000000001), decimal!(79228162514256414777292524694));
    eq_bitwise(decimal!(79228162514264337593543950335) / decimal!(1.00000000000001), decimal!(79228162514263545311918807700));
    eq_bitwise(decimal!(79228162514264337593543950335) / decimal!(1.000000000000001), decimal!(79228162514264258365381436071));
    eq_bitwise(decimal!(79228162514264337593543950335) / decimal!(1.0000000000000001), decimal!(79228162514264329670727698909));
    eq_bitwise(decimal!(79228162514264337593543950335) / decimal!(1.00000000000000001), decimal!(79228162514264336801262325192));
    eq_bitwise(decimal!(79228162514264337593543950335) / decimal!(1.000000000000000001), decimal!(79228162514264337514315787821));
    eq_bitwise(decimal!(79228162514264337593543950335) / decimal!(1.0000000000000000001), decimal!(79228162514264337585621134084));
    eq_bitwise(decimal!(79228162514264337593543950335) / decimal!(1.00000000000000000001), decimal!(79228162514264337592751668710));
    eq_bitwise(decimal!(79228162514264337593543950335) / decimal!(1.000000000000000000001), decimal!(79228162514264337593464722172));
    eq_bitwise(decimal!(79228162514264337593543950335) / decimal!(1.0000000000000000000001), decimal!(79228162514264337593536027519));
    eq_bitwise(decimal!(79228162514264337593543950335) / decimal!(1.00000000000000000000001), decimal!(79228162514264337593543158053));
    eq_bitwise(decimal!(79228162514264337593543950335) / decimal!(1.000000000000000000000001), decimal!(79228162514264337593543871107));
    eq_bitwise(decimal!(79228162514264337593543950335) / decimal!(1.0000000000000000000000001), decimal!(79228162514264337593543942412));
    eq_bitwise(decimal!(79228162514264337593543950335) / decimal!(1.00000000000000000000000001), decimal!(79228162514264337593543949543));
    eq_bitwise(decimal!(79228162514264337593543950335) / decimal!(1.000000000000000000000000001), decimal!(79228162514264337593543950256));
    eq_bitwise(decimal!(7922816251426433759354395033.5) / decimal!( 0.9999999999999999999999999999), decimal!(7922816251426433759354395034));
    eq_bitwise(decimal!(79228162514264337593543950335) / decimal!(10000000), decimal!(7922816251426433759354.3950335));
    eq_bitwise(decimal!(7922816251426433759354395033.5) / decimal!( 1.000001), decimal!(7922808328618105141249253784.2));
    eq_bitwise(decimal!(7922816251426433759354395033.5) / decimal!( 1.0000000000000000000000000001), decimal!(7922816251426433759354395032.7));
    eq_bitwise(decimal!(7922816251426433759354395033.5) / decimal!( 1.0000000000000000000000000002), decimal!(7922816251426433759354395031.9));
    eq_bitwise(decimal!(7922816251426433759354395033.5) / decimal!( 0.9999999999999999999999999999), decimal!(7922816251426433759354395034));
    eq_bitwise(decimal!(79228162514264337593543950335) / decimal!(1.0000000000000000000000000001), decimal!(79228162514264337593543950327));

    assert_eq!(decimal!(1000.123).round_digits(0), decimal!(1000));
    assert_eq!(decimal!(1000.123).round_digits(1), decimal!(1000.1));
}
