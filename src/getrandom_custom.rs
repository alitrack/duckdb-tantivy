//! Deterministic getrandom backend for the wasm32 side module.
//!
//! The extension is linked as an emscripten SIDE_MODULE (`emcc -sSIDE_MODULE=2`).
//! Host syscalls like `getentropy(2)` live in the main module's JS glue and are
//! not reachable from the side module — getrandom's default emscripten backend
//! fails with EAGAIN (os error 6) on every call. That breaks `uuid::Uuid::new_v4`
//! (tantivy SegmentId generation) inside `tantivy_create_index`.
//!
//! getrandom 0.4's `custom` backend (selected via `.cargo/config.toml` rustflags)
//! calls the `__getrandom_v03_custom` symbol below. We back it with a seeded
//! xorshift64* PRNG: segment ids only need to be unique within one engine
//! process, so cryptographic randomness is not required.
//!
//! Native builds never compile this file (`#[cfg(target_arch = "wasm32")]`).

#[cfg(target_arch = "wasm32")]
mod imp {
    use std::sync::atomic::{AtomicU64, Ordering};

    // Seed: fixed constant mixed with the address of a static, so different
    // processes (and ASLR'd loads) diverge while staying deterministic per run.
    static STATE: AtomicU64 = AtomicU64::new(0);

    fn seed() -> u64 {
        // Mix a static address into the fixed seed for run-to-run variety.
        let addr = &STATE as *const _ as u64;
        0x2545_F491_4F6C_DD1D ^ addr.rotate_left(17) ^ 0x9E37_79B9_7F4A_7C15
    }

    #[inline]
    fn next_u64(state: &mut u64) -> u64 {
        // xorshift64* (Marsaglia): cheap, well-scrambled, no dependency on host.
        *state ^= *state >> 12;
        *state ^= *state << 25;
        *state ^= *state >> 27;
        state.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    #[no_mangle]
    pub unsafe extern "Rust" fn __getrandom_v03_custom(
        dest: *mut u8,
        len: usize,
    ) -> Result<(), getrandom::Error> {
        use std::io::Write;

        if dest.is_null() {
            return Err(getrandom::Error::new_stdio());
        }
        // SAFETY: caller (getrandom) guarantees `dest` is valid for `len` writes.
        let buf = unsafe { std::slice::from_raw_parts_mut(dest, len) };
        let mut s = STATE.load(Ordering::Relaxed);
        if s == 0 {
            s = seed();
        }
        let mut w = &mut buf[..];
        while !w.is_empty() {
            s = next_u64(&mut s);
            let bytes = s.to_ne_bytes();
            let n = w.len().min(bytes.len());
            w[..n].copy_from_slice(&bytes[..n]);
            w = &mut w[n..];
        }
        STATE.store(s, Ordering::Relaxed);
        Ok(())
    }
}

#[cfg(target_arch = "wasm32")]
pub use imp::__getrandom_v03_custom;
