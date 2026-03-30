I've recently tasted AMD Zen 5 CPUs (AWS' m8a instances) and... Whooaaa. Even before talking about GPUs and NPUs, the next 5 years of CPUs will be very exciting!

On a m8a.2xlarge virtual instance, pure-Rust ChaCha20 runs at 5.1 GB / s, ChaCha12 at 6.7 GB / s and BLAKE3 at 10.8 GB / s, not bad!

For those who don't know, Zen 5 is the first generation of (AMD) CPUs to have a full 512-bit datapath. For blue-collar developers like me, it means that for the first time we can use AVX-512 SIMD instructions without fear of downclocking and other nasty surprises. On Zen 4, which has a 256-bit datapath, 512-bit SIMD instructions were "double pumped". Older Intel CPUs were downclocking their frequency when using AVX-512 instructions which (sometime?) led to performance worse than when not using AVX-512 acceleration.

You can read more about that in this great Zen5's AVX512 Teardown and in Zen 5's AVX-512 Frequency Behavior.

Most developers probably don't care too much about that other than "ho, my computer is now faster", but for Rust developers it's probably the best end-of-the-year gift that we didn't even dare to dream of.

Why?

Because Rust makes it really easy to add SIMD acceleration to your hot paths without having to deal with assembly. You load data into the SIMD registers and code like if they were normal variables! AVX-512 code can yield more than 10x improvements for less than a day of work.

So here is an introduction on how to write SIMD-accelerated code in pure Rust (no nightly required), after all we all benefit when software goes faster.

You can see an example of production code with SMID acceleration for x86, ARM64 and WebAssembly on Github: https://github.com/skerkour/chacha20-blake3

## SIMD?

SIMD stands for Single Instruction, Multiple Data: CPU instructions that can operate on larger data vectors.

CPUs generally work on values up to 64 bits, we call these "scalar instructions". SIMD instructions, on the other hand, allow CPUs to work on bigger values, up to 512 bits for amd64's AVX-512 instruction set. We call these "vector instructions".

Here is an example in pseudo-code where we want to add 10 to 4 uint64:

```rust
// instead of doing this:
let mut a = [1, 2, 3, 4];
for n in &amp;a {
    *n += 10;
}

// do this
let mut vector = u64x4::from_array([1, 2, 3, 4]); // a 256-bit vector of 4 uint64
let x = u64x4::splat(10); // create a 256-bit vector of 4 uint64: (10, 10, 10, 10)
let vector = vector + x;
// vector = u64x4(11, 12, 13, 4);
```

Instead of generating a loop that can be expensive to execute, the vectorized code will compile to roughly 3 instructions.

One interesting thing to keep in mind is that SIMD instructions may use more power than scalar instructions.

## Thinking in SIMD

Working with SIMDs instructions can be generalized as a 3-step procedure:

**load** -> **compute** -> **store**

First, you **load** your data from memory into the vector registers.

```rust
// loads 8 times the int64 with value 1 in a 512-bit vector
let v1 = _mm512_set1_epi64(1);

// loads the (unaligned) int64 array with 8 elements into a 512-bit vector
let v2 = _mm512_loadu_epi64([1, 2, 3, 4, 5, 6, 7, 8]);
```

Then you perform your **computation** add, xor, subtract, whatever.

```rust
// add the 8 64-bit lanes in parallel
let v_result = _mm512_add_epi64(v1, v2);
// v_result = __m512i(2, 3, 4, 5, 6, 7, 8, 9)
```

And finally you **store** the result back to the memory.

```rust
let result = [0i64, 8];
_mm512_storeu_epi64(result.as_mut_ptr(), v_result);
// result = [2, 3, 4, 5, 6, 7, 8, 9]
```

It's important to understand that loading and storing data from and to memory has a (relatively) huge latency cost and thus should be minimized as much as possible. Your data is better kept warm in the SIMD registers.

Thus, it's important to know how many SIMD registers are available for your target instruction set. For example, NEON provides 32 128-bit registers on arm64: `v0` to `v31`. Thus you can hold up to 32 128-bit vectors to perform your operations without having to touch the "slow" memory.

There are generally two ways to accelerate algorithms with SIMD instructions.

The first way is to find operations that can be performed in parallel for your algorithm, but it's algorithm-specific and often more complex to implement.

The second way, generic and easier to implement, is to "split" your input into chunks that each contain `X` blocks of data, where `X` is the number of available lanes so you can compute the `X` blocks in parallel.

ChaCha20, for example, works on 16 32-bit words that compose 512-bit blocks (16 * 32 bits = 512 bits = 64 bytes).

Therefore, if we have 256-bit vectors available, we are going to operate on 8 blocks (lanes) in parallel (256 / 32 = 8) and thus our chunks of input data will be 8-block long, reaching single-core full speed for inputs of 8 * 64 = 512 bytes or more.

Another example is BLAKE3, which also operates on 32-bit words. BLAKE3 reaches its single-core full speed on machines with AVX-512 instructions for inputs of 16KiB or more: it "splits" the input into 16 blocks (called chunks) of 1024 bytes each, and process these 16 blocks in parallel using AVX-512 instructions, computing 16 32-bit words of state per operation. On machines with AVX2 (256-bit vectors), it reaches its single-core full speed for inputs of 8KiB or more because only 8 32-bit lanes are available in 256-bit vectors.

## Know your target

Implementing SIMD-accelerated code takes time and adds maintenance burden, thus you should know where you code will run in order to focus your efforts.

If your code exclusively runs on high-end Intel / AMD processors (e.g. servers), then focusing your efforts on AVX-512 may be enough.

If, on the other hand, your code runs mostly on consumer machines, then focusing your efforts on AVX2 and NEON may be your best bet.

Also, it make no sense to implement SSE2 SIMDs these days, as most processors produced since 2015 support AVX2.

## CPU features detection

SIMD-accelerated code depends on the instruction sets being available on the CPU it's running on.

There are a few different ways to offer CPU features detection in Rust.

The first one is runtime detection by using the macro provided by the `std::arch` module:

```rust
fn foo() {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if is_x86_feature_detected!("avx2") {
            return unsafe { foo_avx2() };
        }
    }
    // fallback implementation without using AVX2
}
```

This method requires the standard library which may not always be available when working on low-level code.

The second one is by using compile-time features detection:

```
#[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
```

There are a few more esoteric ways to do that, like using cargo features, but I don't recommend it as they will confuse the consumers of your package, especially when your package is a dependency of a dependency of a dependency of a package they are using.

Because runtime detection depends on the standard library which may not be available for some projects (e.g. embedded software), I recommend to provide runtime-detection by default, with a Cargo feature to let the consumers of your package choose build-time-only features detection, so they can target with precision which CPUs their code is going to run on.

Something like:

**Cargo.toml**

```toml
[features]
default = ["std"]

# enables the use of the standard library for CPU features detection on supported platforms
std = []
```

```rust
fn my_function() {
    // use runtime detection
    #[cfg(feature = "std")]
    {
        #[cfg(target_arch = "x86_64")]
        if is_x86_feature_detected!("avx512f") {
            return my_function_avx512();
        }

        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        if is_x86_feature_detected!("avx2") {
            return my_function_avx2();
        }
    }

    // use compile-time detection
    #[cfg(not(feature = "std"))]
    {
        #[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
        return my_function_avx512();

        #[cfg(all(any(target_arch = "x86", target_arch = "x86_64"), target_feature = "avx2"))]
        return my_function_avx2()
    }

    // scalar fallback for when SIMD acceleration is not available
    return my_function_generic();
}
```

## Choosing your Pure-Rust SIMD implementation

There are a few different ways to use SIMD instructions in pure Rust.

The experimental `simd` module from the standard library. It's, unfortunately, currently only available for Rust nightly. We will cover this module later in this post.

The `wide` crate , which is a third-party crate replicating the `simd` module for stable Rust, but is currently limited to 256-bit vectors. I couldn't use it because it pulls too much dependencies.

```rust
use wide::*;

fn main() {
    let a = u32x4::splat(1);
    let b = u32x4::from([1, 2, 3, 4]);
    let result = a + b;
    assert_eq!(result.to_array(), [2, 3, 4, 5]);
}
```

This is the method I recommend if you don't mind the extra dependencies.

The `pulp` crate which is a high-level abstraction over SIMDs, the `rayon` of SIMDs if you like. Like with `wide`, I couldn't use it because it pulls too much dependencies. I'm also not a big fan of runtime detection of SIMD instructions as it currently can't be used for `no_std` targets.

```rust
use pulp::Arch;

fn main() {
    let mut v = (0..1000).map(|i| i as f64).collect::&lt;Vec&lt;_&gt;&gt;();
    let arch = Arch::new();

    arch.dispatch(|| {
        for x in &amp;mut v {
            *x *= 2.0;
        }
    });

    for (i, x) in v.into_iter().enumerate() {
        assert_eq!(x, 2.0 * i as f64);
    }
}
```

Finally, there is the `arch` module from Rust's standard library.

The submodules of `arch`: `x86`, `x86_64`, `aarch64`... expose the raw intrinsics (e.g. `_mm512_add_epi32`) and vector registers (e.g. `__m512i`) available for each platform.

This is the lowest level, and leads to more duplicate code, but it's also the only one that currently works without any dependencies on stable Rust. Thus this is the one I selected for my implementations.

## Auto-vectorization

There is an important point that I want to discuss: auto-vectorization performed by LLVM.

For example, despite a few attempts, it's very hard to implement a faster way to XOR two buffers than the basic way to dot it:

```rust
input_block
    .iter_mut()
    .zip(keystream)
    .for_each(|(plaintext, keystream)| *plaintext ^= *keystream);
```

Indeed, the compiler recognizes this pattern and automatically produces vectorized implementations in function of the instruction sets available.

The most information the compiler has (e.g. size of chunks / blocks...), the most the compiler can perform optimizations such as auto-vectorization. As always, Rust's smart compiler and LLVM are here to cover our back and make our life easier.

My advice is that unless you have a solid proof that it's a bottleneck, don't bother implementing manual SIMD optimizations for common operations such as XORing / adding ... two buffers. The compiler will most probably auto-vectorize it for you, or at least output efficient code.

## Testing

Don't forget to test your implementations with and without the different SIMD instruction sets.

You can use the `RUSTFLAGS` environement variable to selectively disable CPU features:

```sh
# run tests for generic (no SIMD acceleration) code
RUSTFLAGS="-C target-cpu=native -C target-feature=-avx2,-avx512f" make test
# run tests for AVX2 code
RUSTFLAGS="-C target-cpu=native -C target-feature=-avx512f" make test
# run tests for AVX-512 code
make test
```

Note that GitHub actions currently don't support AVX-512 instructions so you will need to run AVX-512 tests on your own machines.

## Portable SIMDs are (hopefully) coming

Portable SIMDs (Rust's simd module) may be one the most exciting features of Rust that is currently available on nightly.

It will greatly alleviate the maintenance burden on developers who want to provide fast and efficient, yet maintainable code.

It will allow us to implement our algorithms only once for every vector size with high-level code, such as `u32x8` to manipulate a 256-bit vector with 8 32-bit lanes, and then the Rust compiler will choose at compile-time what specific instructions to use for the different CPU architectures, with an automatic fallback to scalar.

The code is similar than with `wide`, but without any third-party dependencies and supporting vectors up to 512-bit (vs 256-bit for `wide`).

```rust
fn main() {
    // a 128-bit vector for all the platforms that support 128-bit registers
    let a = u32x4::splat(1);
    let b = u32x4::from([1, 2, 3, 4]);
    let result = a + b;
    assert_eq!(result.to_array(), [2, 3, 4, 5]);
}
```

This is incredible, first because we will no longer have to bother learning the specific names of the intrisincs for each different platform / vector sizes.

Second because it will greatly simplify our code. For example, I've had to implement ChaCha20 with 128-bit vectors 2 times. One time for NEON (arm64) and one time for wasm32's simd128. It was not very difficult as the code is almost the same, only the name of types and intrinsics change, but it's more code to test, maintain and document.

With portable SIMDs, I would just need to implement it over the `u32x4` type, a 128-bit vector with 4 32-bit lanes, and Rust will compile it to optimized code for any platform with 128-bit vector instructions (NEON on arm64, SSE2 on x86, simd128 on wasm32...).

It will also greatly simplify the testing of SIMD code, as a platform-agnostic implementation using `u32x4` can be tested on any platform that supports 128-bit vectors, while the `std::arch` module requires the specific hardware to be able to run the tests.

I really can't wait for this feature to land on Rust stable!

## Some Closing Thoughts

The more you use Rust, the more you understand why it will inevitably eat the entire computing stack, from microcontrollers to big servers, passing by WebAssembly, robots, satellites and everything in between.

As mentioned in a previous article, **more than 37% of vulnerabilities in cryptographic libraries are memory safety issues**, so it's pretty clear that C and assembly are on their way out for crypto code, which is one of the most fundamental parts of the digital era, and Rust is the only replacement that makes sense.

If you want to learn backend development with Rust, take a look at my article Architecting and building medium-sized web services in Rust with Axum, SQLx and PostgreSQL. If you want to learn embedded development, take a look at Introduction to embedded development with Rust: Overview of the ecosystem.

If you want to learn how to do black-wizard things such as applied cryptography, security engineering, how to write secure and production-ready Rust code, take a look at my book **Black Hat Rust** where, among other things, you will build an end-to-end encrypted Remote Access Tool, exploits and a web server in Rust.
