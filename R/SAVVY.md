## Keyboard shortcuts

Press ← or → to navigate between chapters

Press S or / to search in the book

Press ? to show this help

Press Esc to hide this help

1. Introduction

2. Get Started
3. **1.** Get started

   1. Prerequisite
      1. Rust
      2. A helper R package
   2. Create a new R package
      1. Package structure
   3. Write your own function
      1. Write some Rust code
      2. Update wrapper files

4. User Guide
5. **2.** Key ideas
6. **3.** #[savvy] macro
7. **4.** Handling Vector Input
8. **5.** Handling Vector Output
9. **6.** Handling Scalar
10. **7.** Optional Argument
11. **8.** Type-specific Topics
    1. **8.1.** Integer, Real, String, Logical, Raw, And Complex
    2. **8.2.** List
    3. **8.3.** Struct
    4. **8.4.** Enum
12. **9.** Error-handling
13. **10.** Handling Attributes
    1. **10.1.** Handling Data Frames
    2. **10.2.** Handling Factors
    3. **10.3.** Handling Matrices And Arrays
14. **11.** Calling R Function
15. **12.** Testing
16. **13.** Advanced Topics
    1. **13.1.** Initialization Routine
    2. **13.2.** ALTREP
    3. **13.3.** Linkage
17. **14.** Comparison with extendr

## Savvy - A simple R extension interface using Rust

## Get Started

## Prerequisite

### Rust

First of all, you need a Rust toolchain installed. You can follow the official instruction.

If you are on Windows, you need an additional step of installing `x86_64-pc-windows-gnu` target.

```
rustup target add x86_64-pc-windows-gnu
```

### A helper R package

Then, install a helper R package for savvy.

```
install.packages(
  "savvy",
  repos = c("https://yutannihilation.r-universe.dev", "https://cloud.r-project.org")
)
```

Note that, under the hood, this is just a simple wrapper around `savvy-cli`. So, if you prefer shell, you can directly use the CLI instead, which is available on the releases.

## Create a new R package

First, create a new R package. `usethis::create_package()` is convenient for this.

```
usethis::create_package("path/to/foo")
```

Then, move to the package directory and generate necessary files like `Makevars` and `Cargo.toml`, as well as the C and R wrapper code corresponding to the Rust code. `savvy::savvy_init()` does this all (under the hood, this simply runs `savvy-cli init`).

Lastly, run `devtools::document()` to generate `NAMESPACE` and documents.

```
savvy::savvy_init()
devtools::document()
```

Now, this package is ready to install! After installing (e.g. by running “Install Package” on RStudio IDE), confirm you can run this example function that multiplies the first argument by the second argument.

```
library(<your package>)

int_times_int(1:4, 2L)
#> [1] 2 4 6 8
```

### Package structure

After `savvy::savvy_init()`, the structure of your R package should look like below.

```
.
├── .Rbuildignore
├── DESCRIPTION
├── NAMESPACE
├── R
│   └── 000-wrappers.R      <-------(1)
├── configure               <-------(2)
├── configure.win           <-------(2)
├── cleanup                 <-------(2)
├── cleanup.win             <-------(2)
├── foofoofoofoo.Rproj
└── src
    ├── Makevars.in         <-------(2)
    ├── Makevars.win.in     <-------(2)
    ├── init.c              <-------(3)
    ├── <your package>-win.def  <---(4)
    └── rust
        ├── .cargo
        │   └── config.toml <-------(4)
        ├── api.h           <-------(3)
        ├── Cargo.toml      <-------(5)
        └── src
            └── lib.rs      <-------(5)
```

1. `000-wrappers.R`: R functions for the corresponding Rust functions
2. `configure*`, `cleanup*`, `Makevars.in`, and `Makevars.win.in`: Necessary build settings for compiling Rust code
3. `init.c` and `api.h`: C functions for the corresponding Rust functions
4. `<your package>-win.def` and `.cargo/config.toml`: These are tricks to avoid a minor error on Windows. See extendr/rextendr#211 and savvy#98 for the details.
5. `Cargo.toml` and `lib.rs`: Rust code

## Write your own function

The most revolutionary point of `savvy::savvy_init()` is that it kindly leaves the most important task to you; let’s define a typical hello-world function for practice!

### Write some Rust code

Open `src/rust/lib.rs` and add the following lines. `r_println!` is the R version of `println!` macro.

```
/// @export
#[savvy]
fn hello() -> savvy::Result<()> {
    savvy::r_println!("Hello world!");
    Ok(())
}
```

### Update wrapper files

Every time you modify or add some Rust code, you need to update the C and R wrapper files by running `savvy::savvy_update()` (under the hood, this simply runs `savvy-cli update`). Don’t forget to run `devtools::document()` as well.

```
savvy::savvy_update()
devtools::document()
```

After re-installing your package, you should be able to run the `hello()` function on your R session.

```
hello()
#> Hello world!
```
