# iter

Synchronous (`Iter[T]`) and asynchronous (`AsyncIter[T]`) iterators with
mandatory disposal, extracted from archivist-node.

- `Iter[T]` — synchronous iterator with a `next()` proc, `items`/`pairs`
  iterators, `map`/`flatMap`/`filter`/`mapFilter`, `collect`, `empty`,
  slices and ranges.
- `AsyncIter[T]` — `Iter[Future[T]]` plus async-aware combinators
  (`map`, `mapFilter`, `filter`, `delayBy`, `collectAsync`). Usage contract:
  single-consumer, dispose when done, no concurrent dispose.
