use futures::{stream::FuturesUnordered, Future, StreamExt};
use s3s::dto::Object;

// https://users.rust-lang.org/t/usage-of-futures-select-ok/46068/11
// allows the futures to not need to be boxed
pub async fn select_ok<F, A, B>(futs: impl IntoIterator<Item = F>) -> Result<A, B>
where
    F: Future<Output = Result<A, B>>,
{
    let mut futs: FuturesUnordered<F> = futs.into_iter().collect();

    let mut last_error: Option<B> = None;
    while let Some(next) = futs.next().await {
        match next {
            Ok(ok) => return Ok(ok),
            Err(err) => {
                last_error = Some(err);
            }
        }
    }
    Err(last_error.expect("Empty iterator."))
}

pub async fn select_some<F, A>(futs: impl IntoIterator<Item = F>) -> Option<A>
where
    F: Future<Output = Option<A>>,
{
    let mut futs: FuturesUnordered<F> = futs.into_iter().collect();

    while let Some(next) = futs.next().await {
        if next.is_some() {
            return next;
        }
    }
    None
}

pub struct ComparableObject(pub Object);

impl PartialEq for ComparableObject {
    fn eq(&self, other: &Self) -> bool {
        self.0.key == other.0.key
    }
}

impl PartialOrd for ComparableObject {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.0.key.cmp(&other.0.key))
    }
}

impl Eq for ComparableObject {}

impl Ord for ComparableObject {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.key.cmp(&other.0.key)
    }
}
