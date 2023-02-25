use futures::{stream::FuturesUnordered, Future, StreamExt};

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
