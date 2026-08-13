# iter

Synchronous (`Iter[T]`) and asynchronous (`AsyncIter[T]`) iterators with mandatory disposal.

## Usage contract

Both iterator types share four rules:

1. **Single-consumer**: do not call `next()` concurrently from multiple threads or tasks.
2. **Disposal required**: always call `dispose()` when done (use `defer` or `try/finally`).
3. **No concurrent dispose**: do not call `dispose()` while `next()` is in flight.
4. **After dispose**: calling `next()` raises.

`dispose()` is idempotent and sets `finished = true`. There is deliberately no `=destroy` auto-dispose: the closures captured in `disposeImpl` may reference objects the GC collects before the `Iter` itself.

## `Iter[T]` - synchronous

Construction:

- `Iter[T].new(genNext, isFinished, dispose, isDisposed, finishOnErr = true)` - generic constructor from supplier callbacks (`dispose`/`isDisposed` are required and assert when nil)
- `Iter[T].new(a, b, step = 1)` / `Iter[T].new(slice)` - ordinal ranges
- `Iter[T].new(items: seq[T])` - from a sequence
- `Iter[T].new(iter: iterator)` - from a native iterator
- `Iter[T].empty` - empty iterator

Access: `next()`, `finished`, `disposed`, `finish()`, `items` (iterates to completion), `pairs` (yields `(index, item)`).

Combinators (each chains `dispose()` to the underlying iterator):

- `map(iter, fn)` - `Iter[T]` to `Iter[U]`
- `mapFilter(iter, mapPredicate)` - `fn: T -> Option[U]`, skips `none`
- `filter(iter, predicate)` - implemented via `mapFilter`

```nim
let iter = Iter[int].new(0 ..< 10)
defer: iter.dispose()
let doubled = iter.map((i: int) => i * 2)
defer: doubled.dispose()   # chains to iter.dispose()
for n in doubled:
  echo n
```

## `AsyncIter[T]` - asynchronous

`AsyncIter[T]` extends `Iter[Future[T]]`: it inherits the finished-state machinery and the `items`/`pairs` iterators, and adds an async dispose contract (`await iter.dispose()`, wrapped in `noCancel` so cleanup completes even if the caller is cancelled).

Construction:

- `AsyncIter[T].new(genNext, isFinished, dispose = default, isDisposed = default, finishOnErr = true)`
- `AsyncIter[T].new(a, b, step = 1)` / `AsyncIter[T].new(slice)` - via `mapAsync`
- `AsyncIter[T].empty`

Access: `next()` (returns `Future[T]`), `items`/`pairs` (yield futures), `finished`, `disposed`.

Combinators:

- `flatMap(fut, fn)` - on single futures
- `map(iter: AsyncIter[T], fn)` - `fn: T -> Future[U]`, chains dispose
- `mapAsync(iter: Iter[T], fn)` - lifts a sync `Iter` into `AsyncIter[U]`
- `mapFilter(iter, mapPredicate)` / `filter(iter, predicate)` - async predicates; return `Future[AsyncIter[U]]` (construction fetches the first matching element)
- `delayBy(iter, duration)` - delays each item
- `collectAsync(iter)` - `Future[seq[T]]`, the async analog of sync `toSeq(iter)`; the first failing item aborts the collection and its error propagates (raised, never returned as a value)

`collectAsync` is named `collectAsync` (not `collect`) so it never collides with the `std/sugar` `collect` template when both are in scope.

```nim
let it = AsyncIter[int].new(0 ..< 10)
defer: await it.dispose()
let delayed = it.delayBy(50.milliseconds)
let results = await collectAsync(delayed)
```

## Real-world example

Streaming a paginated remote record set: lift a sync page-number range into an async fetch, flatten the pages into one record stream, filter out the small records and collect the ids of the rest. `dispose()` is deferred so cleanup runs even when the pipeline is cancelled.

```nim
import std/sugar

import pkg/chronos
import pkg/iter

type
  Record = object
    id: int
    bytes: int

# Simulated paged remote store: each page arrives after a network-like delay.
proc fetchPage(page: int): Future[seq[Record]] {.async.} =
  await sleepAsync(1.milliseconds)
  @[
    Record(id: page * 10 + 0, bytes: page + 1),
    Record(id: page * 10 + 1, bytes: page + 2),
  ]

proc records(): AsyncIter[Record] =
  # Fetch pages 0..2 and flatten each page into a single record stream.
  flatMap[seq[Record], Record](
    mapAsync[int, seq[Record]](Iter[int].new(0 ..< 3), fetchPage),
    proc(page: seq[Record]): Future[AsyncIter[Record]] {.async.} =
      var i = 0
      AsyncIter[Record].new(
        proc(): Future[Record] {.async.} =
          let item = page[i]
          inc i
          item,
        proc(): bool {.gcsafe, raises: [].} = i >= page.len,
      )
  )

proc main() {.async.} =
  let iter = records()
  defer: await iter.dispose()   # mandatory; runs even on cancellation

  let filtered = await filter[Record](
    iter,
    proc(r: Record): Future[bool] {.async.} =
      r.bytes > 1,
  )
  let kept = await collectAsync(
    map[Record, int](
      filtered,
      proc(r: Record): Future[int] {.async.} =
        r.id,
    )
  )
  doAssert kept == @[1, 10, 11, 20, 21]

when isMainModule:
  waitFor main()
```

Notes:

- **Explicit type arguments**: combinators take them (`mapAsync[int, seq[Record]]`) - the `Function[T, Future[U]]` callback type does not reliably infer `U`.
- **`filter`/`mapFilter` are async constructors**: they return `Future[AsyncIter[T]]` (construction fetches the first matching element), so await them before chaining.
- **Error model**: the first failing item aborts `collectAsync` with an `IteratorError` whose `parent` carries the wrapped cause.

  ```nim
  try:
    let kept = await collectAsync(iter)
  except IteratorError as err:
    echo err.parent.msg
  ```

- **`flatMap` needs non-empty inner iterators**: an empty inner iterator exhausts the source with `IteratorError`.

## Install

```bash
nimble install https://github.com/durability-labs/iter@#main
```

Requires Nim >= 2.0.14, chronos 4.0.x, questionable 0.10.x. Tests: `nimble test`.
