//! Limits documented in the Bot API, checked on decoded values. Violations are
//! structured; the gateway maps them to Telegram's exact error texts.

/// A broken documented limit, with the path of the offending field
/// (e.g. `reply_markup.inline_keyboard[0][1].callback_data`).
#[derive(Debug, Clone, PartialEq)]
pub struct Violation {
    pub path: String,
    pub rule: Rule,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Rule {
    /// Length in characters (Unicode scalar values) outside `min..=max`.
    Chars { min: Option<u64>, max: Option<u64>, actual: u64 },
    /// Length in UTF-8 bytes outside `min..=max`.
    Bytes { min: Option<u64>, max: Option<u64>, actual: u64 },
    /// Number outside `min..=max`.
    Range { min: Option<f64>, max: Option<f64>, actual: f64 },
    /// Array size outside `min..=max`.
    Items { min: Option<u64>, max: Option<u64>, actual: u64 },
    /// String not among the documented values.
    OneOf { allowed: &'static [&'static str], actual: String },
}

/// Implemented by every generated type and method; recurses into nested values.
pub trait Validate {
    fn validate_into(&self, path: &str, out: &mut Vec<Violation>);

    fn validate(&self) -> Result<(), Vec<Violation>> {
        let mut out = Vec::new();
        self.validate_into("", &mut out);
        if out.is_empty() { Ok(()) } else { Err(out) }
    }
}

/// `prefix.field`, or just `field` at the top level.
pub fn join(prefix: &str, field: &str) -> String {
    if prefix.is_empty() { field.to_owned() } else { format!("{prefix}.{field}") }
}

pub fn index(prefix: &str, i: usize) -> String {
    format!("{prefix}[{i}]")
}

pub fn chars(path: &str, value: &str, min: Option<u64>, max: Option<u64>, out: &mut Vec<Violation>) {
    let actual = value.chars().count() as u64;
    if min.is_some_and(|m| actual < m) || max.is_some_and(|m| actual > m) {
        out.push(Violation { path: path.to_owned(), rule: Rule::Chars { min, max, actual } });
    }
}

pub fn bytes(path: &str, value: &str, min: Option<u64>, max: Option<u64>, out: &mut Vec<Violation>) {
    let actual = value.len() as u64;
    if min.is_some_and(|m| actual < m) || max.is_some_and(|m| actual > m) {
        out.push(Violation { path: path.to_owned(), rule: Rule::Bytes { min, max, actual } });
    }
}

pub fn range(path: &str, value: f64, min: Option<f64>, max: Option<f64>, out: &mut Vec<Violation>) {
    if min.is_some_and(|m| value < m) || max.is_some_and(|m| value > m) {
        out.push(Violation { path: path.to_owned(), rule: Rule::Range { min, max, actual: value } });
    }
}

pub fn items<T>(path: &str, value: &[T], min: Option<u64>, max: Option<u64>, out: &mut Vec<Violation>) {
    let actual = value.len() as u64;
    if min.is_some_and(|m| actual < m) || max.is_some_and(|m| actual > m) {
        out.push(Violation { path: path.to_owned(), rule: Rule::Items { min, max, actual } });
    }
}

pub fn one_of(path: &str, value: &str, allowed: &'static [&'static str], out: &mut Vec<Violation>) {
    if !allowed.contains(&value) {
        out.push(Violation {
            path: path.to_owned(),
            rule: Rule::OneOf { allowed, actual: value.to_owned() },
        });
    }
}

// Leaf values have no limits of their own; containers recurse.

macro_rules! leaf {
    ($($t:ty),*) => {$(
        impl Validate for $t {
            fn validate_into(&self, _: &str, _: &mut Vec<Violation>) {}
        }
    )*};
}

leaf!(i64, f64, bool, String, super::ChatId, super::InputFile);

impl<T: Validate> Validate for Option<T> {
    fn validate_into(&self, path: &str, out: &mut Vec<Violation>) {
        if let Some(value) = self {
            value.validate_into(path, out);
        }
    }
}

impl<T: Validate> Validate for Vec<T> {
    fn validate_into(&self, path: &str, out: &mut Vec<Violation>) {
        for (i, value) in self.iter().enumerate() {
            value.validate_into(&index(path, i), out);
        }
    }
}

impl<T: Validate> Validate for Box<T> {
    fn validate_into(&self, path: &str, out: &mut Vec<Violation>) {
        (**self).validate_into(path, out);
    }
}
