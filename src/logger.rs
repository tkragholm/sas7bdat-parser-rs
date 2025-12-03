use std::fs::File;
use std::io::{Result as IoResult, Write};
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use std::{cell::RefCell, fmt::Display};

static LOG_FILE: OnceLock<Arc<Mutex<File>>> = OnceLock::new();
thread_local! {
    static LOG_PREFIX: RefCell<Option<String>> = const { RefCell::new(None) };
}

fn with_prefix<F, R>(f: F) -> R
where
    F: FnOnce(Option<String>) -> R,
{
    LOG_PREFIX.with(|prefix| f(prefix.borrow().clone()))
}

fn format_with_prefix(message: impl Display) -> String {
    with_prefix(|prefix| prefix.map_or_else(|| message.to_string(), |p| format!("{p}: {message}")))
}

/// Configures a log file for warnings/errors emitted by the converter.
///
/// # Errors
///
/// Returns an error if the log file cannot be created.
pub fn set_log_file(path: &Path) -> IoResult<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = File::create(path)?;
    // Ignore error if already set; keep the first writer.
    let _ = LOG_FILE.set(Arc::new(Mutex::new(file)));
    Ok(())
}

/// Sets a thread-local prefix that will be prepended to subsequent log
/// messages. Returns a guard that restores the previous prefix on drop.
pub fn set_log_prefix(prefix: impl Into<String>) -> LogPrefixGuard {
    let prefix = prefix.into();
    let previous = with_prefix(|p| p);
    LOG_PREFIX.with(|slot| {
        *slot.borrow_mut() = Some(prefix);
    });
    LogPrefixGuard { previous }
}

pub struct LogPrefixGuard {
    previous: Option<String>,
}

impl Drop for LogPrefixGuard {
    fn drop(&mut self) {
        let prev = self.previous.clone();
        LOG_PREFIX.with(|slot| {
            *slot.borrow_mut() = prev;
        });
    }
}

pub fn log_warn(message: &str) {
    let message = format_with_prefix(message);
    eprintln!("{message}");
    if let Some(writer) = LOG_FILE.get()
        && let Ok(mut file) = writer.lock()
    {
        let _ = writeln!(file, "warning: {message}");
    }
}

pub fn log_error(message: &str) {
    let message = format_with_prefix(message);
    eprintln!("{message}");
    if let Some(writer) = LOG_FILE.get()
        && let Ok(mut file) = writer.lock()
    {
        let _ = writeln!(file, "error: {message}");
    }
}
