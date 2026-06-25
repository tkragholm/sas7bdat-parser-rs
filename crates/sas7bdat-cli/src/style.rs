//! Minimal ANSI styling that disables itself when output isn't a terminal or when
//! `NO_COLOR` is set. No dependency — just enough for status lines and headers.

use std::io::IsTerminal;

#[derive(Clone, Copy)]
pub struct Style {
    enabled: bool,
}

impl Style {
    /// Styling tuned for stdout (success summaries, tables).
    #[must_use]
    pub fn for_stdout() -> Self {
        Self::resolve(std::io::stdout().is_terminal())
    }

    /// Styling tuned for stderr (error lines).
    #[must_use]
    pub fn for_stderr() -> Self {
        Self::resolve(std::io::stderr().is_terminal())
    }

    fn resolve(is_terminal: bool) -> Self {
        // Honor the de-facto NO_COLOR convention regardless of TTY state.
        let enabled = is_terminal && std::env::var_os("NO_COLOR").is_none();
        Self { enabled }
    }

    fn paint(self, code: &str, text: &str) -> String {
        if self.enabled {
            format!("\u{1b}[{code}m{text}\u{1b}[0m")
        } else {
            text.to_owned()
        }
    }

    #[must_use]
    pub fn bold(self, text: &str) -> String {
        self.paint("1", text)
    }

    #[must_use]
    pub fn dim(self, text: &str) -> String {
        self.paint("2", text)
    }

    #[must_use]
    pub fn green(self, text: &str) -> String {
        self.paint("32", text)
    }

    #[must_use]
    pub fn red(self, text: &str) -> String {
        self.paint("31", text)
    }

    #[must_use]
    pub fn cyan(self, text: &str) -> String {
        self.paint("36", text)
    }

    /// A green check mark, or a plain ASCII fallback when styling is off.
    #[must_use]
    pub fn check(self) -> String {
        if self.enabled {
            self.green("\u{2713}")
        } else {
            "OK".to_owned()
        }
    }

    /// A red cross, or a plain ASCII fallback when styling is off.
    #[must_use]
    pub fn cross(self) -> String {
        if self.enabled {
            self.red("\u{2717}")
        } else {
            "ERROR".to_owned()
        }
    }
}
