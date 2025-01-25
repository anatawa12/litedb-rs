use std::fmt::{Debug, Formatter};

pub(super) struct ToHex<const SIZE: usize>(pub(super) [u8; SIZE]);

impl<const SIZE: usize> Debug for ToHex<SIZE> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut as_hex = [[0u8; 2]; SIZE];

        for (i, as_hex) in as_hex.iter_mut().enumerate().take(SIZE) {
            let b = self.0[i];
            let high = (b >> 4) & 0xf;
            let low = (b & 0xf) << 4;
            as_hex[0] = b"0123456789abcdef"[high as usize];
            as_hex[1] = b"0123456789abcdef"[low as usize];
        }

        let str = unsafe {
            std::str::from_utf8_unchecked(std::slice::from_raw_parts(
                as_hex.as_ptr() as *const u8,
                as_hex.len() * 2,
            ))
        };

        f.write_str(str)
    }
}
