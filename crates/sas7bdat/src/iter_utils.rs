use crate::Result;

pub fn next_from_result<T, U>(
    result: Result<Option<T>>,
    map_ok: impl FnOnce(T) -> U,
    on_error: impl FnOnce(),
) -> Option<Result<U>> {
    match result {
        Ok(Some(value)) => Some(Ok(map_ok(value))),
        Ok(None) => None,
        Err(err) => {
            on_error();
            Some(Err(err))
        }
    }
}
