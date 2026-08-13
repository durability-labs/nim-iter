## Copyright (c) 2026 Archivist Authors
## Licensed under either of
##  * Apache License, version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.

## AsyncIter[T] - Asynchronous iterator extending Iter[Future[T]]
##
## Inherits the sync machinery (finished state, next field, items/pairs
## iterators) from `Iter[Future[T]]`; the disposal contract is async, so
## the subclass adds its own async dispose state and shadows `dispose`/
## `disposed` with async versions.
##
## USAGE CONTRACT:
## 1. Single-consumer: Do NOT call next() concurrently from multiple tasks
## 2. Disposal required: Always call dispose() when done (use defer or try/finally)
## 3. No concurrent dispose: Do NOT call dispose() concurrently or while next() is in-flight
## 4. After dispose: Calling next() will raise an error
##
## ERROR MODEL:
## The `Iter` error model applies unchanged (see `iter`): the machinery
## fails futures with `IteratorError` only.  Additionally, futures may be
## CANCELLED - cancellation is control flow, not an error, and is never
## wrapped or swallowed.
##
## Example:
##   let iter = createIterator()
##   defer: await iter.dispose()
##   while not iter.finished:
##     let item = await iter.next()
##     # process item

import std/sugar

import pkg/questionable
import pkg/questionable/results
import pkg/chronos

import ./iter

export iter

type
  AsyncDispose* = proc(): Future[void] {.async, gcsafe, closure.}
  AsyncIsDisposed* = proc(): bool {.raises: [], gcsafe, closure.}

  AsyncIter*[T] = ref object of Iter[Future[T]]
    asyncDisposeImpl: AsyncDispose
    asyncDisposedImpl: AsyncIsDisposed

proc defaultAsyncDispose(): Future[void] {.async.} =
  discard

proc defaultAsyncIsDisposed(): bool =
  false

proc disposed*[T](self: AsyncIter[T]): bool =
  self.asyncDisposedImpl()

proc dispose*[T](self: AsyncIter[T]): Future[void] {.async.} =
  ## Dispose the iterator and release any underlying resources.
  ## Caller is responsible for calling this when done with the iterator.
  ## Idempotent - safe to call multiple times.
  ## Sets finished = true to prevent further iteration.
  ## Uses noCancel to ensure cleanup completes even if caller is cancelled.
  if not self.disposed:
    self.finish
    await noCancel self.asyncDisposeImpl()

proc flatMap*[T, U](fut: Future[T], fn: Function[T, Future[U]]): Future[U] {.async.} =
  try:
    let t = await fut
    await fn(t)
  except IteratorError as err:
    raise err
  except CancelledError as err:
    raise err
  except CatchableError as err:
    raise toIteratorError(err)

proc new*[T](
    _: type AsyncIter[T],
    genNext: GenNext[Future[T]],
    isFinished: IsFinished,
    dispose: AsyncDispose = defaultAsyncDispose,
    isDisposed: AsyncIsDisposed = defaultAsyncIsDisposed,
    finishOnErr: bool = true,
): AsyncIter[T] =
  ## Creates a new AsyncIter using elements returned by supplier function `genNext`.
  ## Iter is finished whenever `isFinished` returns true.
  ## Caller is responsible for calling `dispose()` when done with the iterator.

  var iter = AsyncIter[T](asyncDisposeImpl: dispose, asyncDisposedImpl: isDisposed)

  proc next(): Future[T] {.async.} =
    if iter.disposed:
      raise newException(IteratorError, "AsyncIter is disposed - cannot call next()")
    if not iter.finished:
      var item: T
      try:
        item = await genNext()
      except CancelledError as err:
        iter.finish
        raise err
      except CatchableError as err:
        if finishOnErr or isFinished():
          iter.finish
        raise toIteratorError(err)

      if isFinished():
        iter.finish
      return item
    else:
      raise
        newException(IteratorError, "AsyncIter is finished but next item was requested")

  if isFinished():
    iter.finish

  iter.next = next
  return iter

proc mapAsync*[T, U](iter: Iter[T], fn: Function[T, Future[U]]): AsyncIter[U] =
  # Chain dispose to underlying sync iterator
  AsyncIter[U].new(
    genNext = () => fn(iter.next()),
    isFinished = () => iter.finished(),
    dispose = proc(): Future[void] {.async.} =
      iter.dispose(),
    isDisposed = () => iter.disposed,
  )

proc new*[U, V: Ordinal](_: type AsyncIter[U], slice: HSlice[U, V]): AsyncIter[U] =
  ## Creates new Iter from a slice
  ##

  let iter = Iter[U].new(slice)
  mapAsync[U, U](
    iter,
    proc(i: U): Future[U] {.async.} =
      i,
  )

proc new*[U, V, S: Ordinal](
    _: type AsyncIter[U], a: U, b: V, step: S = 1
): AsyncIter[U] =
  ## Creates new Iter in range a..b with specified step (default 1)
  ##

  let iter = Iter[U].new(a, b, step)
  mapAsync[U, U](
    iter,
    proc(i: U): Future[U] {.async.} =
      i,
  )

proc empty*[T](_: type AsyncIter[T]): AsyncIter[T] =
  ## Creates an empty AsyncIter
  ##

  var disposed = false

  proc genNext(): Future[T] {.raises: [IteratorError], closure.} =
    raise newException(IteratorError, "Next item requested from an empty AsyncIter")

  proc isFinished(): bool =
    true

  proc isDisposed(): bool =
    disposed

  AsyncIter[T].new(
    genNext,
    isFinished,
    dispose = proc(): Future[void] {.async.} =
      disposed = true,
    isDisposed,
  )

proc map*[T, U](iter: AsyncIter[T], fn: Function[T, Future[U]]): AsyncIter[U] =
  multiSync(AsyncIter[U], T, U, iter, fn)

proc mapFilter*[T, U](
    iter: AsyncIter[T], mapPredicate: Function[T, Future[Option[U]]]
): Future[AsyncIter[U]] {.async: (raises: [CancelledError]).} =
  var nextFutU: Option[Future[U]]

  proc tryFetch(): Future[void] {.async: (raises: [CancelledError]).} =
    nextFutU = Future[U].none
    while not iter.finished:
      let futT = iter.next()
      try:
        if u =? await futT.flatMap(mapPredicate):
          let futU = newFuture[U]("AsyncIter.mapFilterAsync")
          futU.complete(u)
          nextFutU = some(futU)
          break
      except CancelledError as err:
        raise err
      except CatchableError as err:
        let errFut = newFuture[U]("AsyncIter.mapFilterAsync")
        errFut.fail(toIteratorError(err))
        nextFutU = some(errFut)
        break

  proc genNext(): Future[U] {.async.} =
    let futU = nextFutU.unsafeGet
    await tryFetch()
    await futU

  proc isFinished(): bool =
    nextFutU.isNone

  await tryFetch()
  # Chain dispose to underlying iterator
  AsyncIter[U].new(
    genNext,
    isFinished,
    dispose = () => iter.dispose(),
    isDisposed = () => iter.disposed,
  )

proc filter*[T](
    iter: AsyncIter[T], predicate: Function[T, Future[bool]]
): Future[AsyncIter[T]] {.async.} =
  proc wrappedPredicate(t: T): Future[Option[T]] {.async.} =
    if await predicate(t):
      some(t)
    else:
      T.none

  # mapFilter already chains dispose to iter
  await mapFilter[T, T](iter, wrappedPredicate)

proc flatMap*[T, U](
    iter: AsyncIter[T], fn: Function[T, Future[AsyncIter[U]]]
): AsyncIter[U] =
  ## Applies `fn` to each item, flattening the returned async iterators
  ## into a single stream.  Inner iterators are owned by `fn`'s caller -
  ## dispose them alongside the source via the chained dispose.
  multiSyncFlatMap(
    AsyncIter[U],
    T,
    U,
    iter,
    fn,
    syncAdvance = fn(iter.next()),
    asyncAdvance = await fn(await iter.next()),
  )

proc delayBy*[T](iter: AsyncIter[T], d: Duration): AsyncIter[T] =
  ## Delays emitting each item by given duration
  ##

  # map already chains dispose to iter
  map[T, T](
    iter,
    proc(t: T): Future[T] {.async.} =
      await sleepAsync(d)
      t,
  )

proc collectAsync*[T](iter: AsyncIter[T]): Future[seq[T]] {.async: (raises: [IteratorError, CancelledError]).} =
  ## Collect all items of an async iterator into a seq - the async analog
  ## of sync `toSeq(iter)`.  The first failing item aborts the collection:
  ## its `IteratorError` propagates (`parent` discriminates contract
  ## violations from wrapped user errors), and cancellation propagates as
  ## control flow - errors are raised, never returned as values.
  ##
  ## Named `collectAsync` (not `collect`) so it never collides with the
  ## std/sugar `collect` template when both are in scope.
  ##
  var res: seq[T]
  for item in iter:
    let captured = catch(await item)
    if captured.isErr:
      if captured.error of CancelledError:
        raise (ref CancelledError)(captured.error)
      raise (ref IteratorError)(captured.error)
    res.add(captured.value)
  res
