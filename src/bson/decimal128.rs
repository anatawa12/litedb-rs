use super::utils::ToHex;
use std::cmp::Ordering;
use std::fmt::{Debug, Display, Formatter};

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
        assert!(exponent <= 28, "Exponent is too big");

        let repr = mantissa
            | ((exponent as u128) << Self::EXPONENT_SHIFT)
            | (if is_negative { Self::SIGN_MASK } else { 0 });

        Decimal128 { repr }
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
            if after_dot.len() > 28 {
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
    const fn exponent(&self) -> u8 {
        (((self.repr & Self::EXPONENT_MASK) >> Self::EXPONENT_SHIFT) & 0xFF) as u8
    }

    #[inline]
    const fn mantissa(&self) -> u128 {
        self.repr & Self::MANTISSA_MASK
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

            const DEC_SCALE_MAX: i32 = 28;

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
                if power > DEC_SCALE_MAX {
                    power = DEC_SCALE_MAX;
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
            if dbl < 1E14 && power < DEC_SCALE_MAX {
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
    construct_test!(5, 0, false, [
        5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
    ]);
    construct_test!(50, 1, false, [
        50, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0
    ]);
    construct_test!(51, 1, false, [
        51, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0
    ]);

    parse_test!("5", [5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    parse_test!("5.0", [50, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0]);
    parse_test!("5.1", [51, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0]);
    parse_test!("5.1", [51, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0]);

    assert_eq!(decimal!(5).bytes(), [
        5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
    ]);
    assert_eq!(decimal!(5.).bytes(), [
        5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
    ]);
    assert_eq!(decimal!(5.0).bytes(), [
        50, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0
    ]);
    assert_eq!(decimal!(5.1).bytes(), [
        51, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0
    ]);
    assert_eq!(decimal!(0.1).bytes(), [
        1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0
    ]);
    assert_eq!(decimal!(0.10).bytes(), [
        10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0
    ]);

    // max and min value
    assert_eq!(Decimal128::MAX.bytes(), [
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0
    ]);
    assert_eq!(
        Decimal128::MAX.bytes(),
        decimal!(79228162514264337593543950335).bytes()
    );
    assert_eq!(Decimal128::MIN.bytes(), [
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 128
    ]);
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

// TODO: implement display for better visibility
