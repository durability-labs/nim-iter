## Copyright (c) 2026 Archivist Authors
## Licensed under either of
##  * Apache License, version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.

## Iter[T] - Synchronous iterator with mandatory disposal
##
## USAGE CONTRACT:
## 1. Single-consumer: Do NOT call next() concurrently from multiple threads/tasks
## 2. Disposal required: Always call dispose() when done (use defer or try/finally)
## 3. No concurrent dispose: Do NOT call dispose() while next() is in-flight
## 4. After dispose: Calling next() will raise an error
##
## ERROR MODEL:
## `IteratorError` is the only exception type the iterator machinery raises.
## It has two classes, discriminated by `parent`:
## - contract violation: parent == nil (next() after finish/dispose)
## - wrapped user error: parent carries the original exception from a
##   supplier or combinator function
## Cancellation is never part of the sync iterator's error model.
##
## Example:
##   let iter = createIterator()
##   defer: iter.dispose()
##   while not iter.finished:
##     let item = iter.next()
##     # process item

import std/sugar

from pkg/chronos import Future
# `from`-import only: a full chronos import exports an `err` symbol that
# breaks questionable's `without` errorname resolution in this module.
import pkg/questionable
import pkg/questionable/results

type
  IteratorError* = object of CatchableError
    ## The only exception type the iterator machinery raises.
    ## `parent == nil` marks a contract violation (misuse);
    ## a non-nil `parent` carries the wrapped user error.

  Function*[T, U] = proc(t: T): U {.raises: [CatchableError], gcsafe, closure.}
  IsFinished* = proc(): bool {.raises: [], gcsafe, closure.}
  IsDisposed* = proc(): bool {.raises: [], gcsafe, closure.}
  Dispose* = proc() {.raises: [], gcsafe, closure.}
  GenNext*[T] = proc(): T {.raises: [CatchableError], gcsafe, closure.}
  Iterator[T] = iterator (): T

  IterObj[T] = object of RootObj
    finished: bool
    next*: GenNext[T]
    disposeImpl: Dispose
    disposedImpl: IsDisposed

  Iter*[T] = ref IterObj[T]

# Note: We intentionally don't use =destroy to auto-dispose because closures
# captured in disposeImpl might reference objects that are garbage collected
# before the Iter itself. Callers MUST call dispose() explicitly.

proc toIteratorError*(err: ref CatchableError): ref IteratorError =
  ## Wraps a user error at the supplier boundary. The original exception
  ## stays reachable via `parent` for typed discrimination.  Already-wrapped
  ## errors pass through unchanged so combinator chains never nest.
  if err of IteratorError:
    return (ref IteratorError)(err)
  let e = newException(IteratorError, err.msg)
  e.parent = err
  e

proc finish*[T](self: Iter[T]): void =
  self.finished = true

proc finished*[T](self: Iter[T]): bool =
  self.finished

proc disposed*[T](self: Iter[T]): bool =
  # AsyncIter subclasses carry their own async dispose state; the base
  # callbacks stay nil on those instances.
  if self.disposeImpl == nil:
    false
  else:
    self.disposedImpl()

proc dispose*[T](self: Iter[T]) =
  ## Dispose the iterator and release any underlying resources.
  ## Caller is responsible for calling this when done with the iterator.
  ## Idempotent - safe to call multiple times.
  ## Sets finished = true to prevent further iteration.
  if not self.disposed:
    self.finished = true
    if self.disposeImpl != nil:
      self.disposeImpl()

iterator items*[T](self: Iter[T]): T =
  while not self.finished:
    yield self.next()

iterator pairs*[T](self: Iter[T]): tuple[key: int, val: T] {.inline.} =
  var i = 0
  while not self.finished:
    yield (i, self.next())
    inc(i)

template multiSync*(
    iterType: typedesc, T, U: typedesc, iter: untyped, fn: untyped
): untyped =
  ## Generates the `map` combinator body for `Iter` or `AsyncIter`.
  ## An fn returning `Future[X]` selects the async variant (the source
  ## iterator is awaited before the fn, and the fn's future is awaited);
  ## any other return type selects the sync variant.
  block:
    mixin dispose, disposed
    let src = iter
    let f = fn
    when typeof(f(default(T))) is Future:
      proc genNext(): Future[U] {.async.} =
        await f(await src.next())

      proc isFinished(): bool =
        src.finished

      proc onDispose(): Future[void] {.async.} =
        await src.dispose()

      proc onIsDisposed(): bool =
        src.disposed

      iterType.new(genNext, isFinished, onDispose, onIsDisposed)
    else:
      proc genNext(): U {.raises: [CatchableError], closure.} =
        f(src.next())

      proc isFinished(): bool =
        src.finished

      proc onDispose(): void {.raises: [].} =
        src.dispose()

      proc onIsDisposed(): bool =
        src.disposed

      iterType.new(genNext, isFinished, onDispose, onIsDisposed)

template multiSyncFlatMap*(
    iterType: typedesc,
    T, U: typedesc,
    iter: untyped,
    fn: untyped,
    syncAdvance: untyped,
    asyncAdvance: untyped,
): untyped =
  ## Generates the `flatMap` combinator body for `Iter` or `AsyncIter`.
  ## `syncAdvance`/`asyncAdvance` are the caller's per-item advance
  ## expressions (`fn(iter.next())` / `await fn(await iter.next())`);
  ## the template owns the current-iterator tracking and the assignment.
  block:
    mixin dispose, disposed
    var current: iterType = nil
    when typeof(fn(default(T))) is Future:
      proc genNext(): Future[U] {.async.} =
        try:
          while current == nil or current.finished:
            if iter.finished:
              raise newException(IteratorError, "flatMap exhausted its source iterator")
            current = asyncAdvance
          await current.next()
        except IteratorError as err:
          raise err
        except CancelledError as err:
          raise err
        except CatchableError as err:
          raise toIteratorError(err)

      proc isFinished(): bool =
        iter.finished and (current == nil or current.finished)

      proc onDispose(): Future[void] {.async.} =
        await iter.dispose()

      proc onIsDisposed(): bool =
        iter.disposed

      iterType.new(genNext, isFinished, onDispose, onIsDisposed)
    else:
      proc genNext(): U {.raises: [IteratorError], closure.} =
        try:
          while current == nil or current.finished:
            if iter.finished:
              raise newException(IteratorError, "flatMap exhausted its source iterator")
            current = syncAdvance
          current.next()
        except IteratorError as err:
          raise err
        except CatchableError as err:
          raise toIteratorError(err)

      proc isFinished(): bool =
        iter.finished and (current == nil or current.finished)

      proc onDispose(): void {.raises: [].} =
        iter.dispose()

      proc onIsDisposed(): bool =
        iter.disposed

      iterType.new(genNext, isFinished, onDispose, onIsDisposed)

proc new*[T](
    _: type Iter[T],
    genNext: GenNext[T],
    isFinished: IsFinished,
    dispose: Dispose,
    isDisposed: IsDisposed,
    finishOnErr: bool = true,
): Iter[T] =
  ## Creates a new Iter using elements returned by supplier function `genNext`.
  ## Iter is finished whenever `isFinished` returns true.
  ## Caller is responsible for calling `dispose()` when done with the iterator.
  ##
  ## IMPORTANT: dispose and isDisposed callbacks are REQUIRED - passing nil will assert.

  doAssert dispose != nil, "dispose callback is required"
  doAssert isDisposed != nil, "isDisposed callback is required"

  var iter = Iter[T](disposeImpl: dispose, disposedImpl: isDisposed)

  proc next(): T {.raises: [IteratorError].} =
    if iter.disposed:
      raise newException(IteratorError, "Iter is disposed - cannot call next()")
    if not iter.finished:
      var item: T
      try:
        item = genNext()
      except CatchableError as err:
        if finishOnErr or isFinished():
          iter.finish
        raise toIteratorError(err)

      if isFinished():
        iter.finish
      return item
    else:
      raise newException(IteratorError, "Iter is finished but next item was requested")

  if isFinished():
    iter.finish

  iter.next = next
  return iter

proc new*[U, V, S: Ordinal](_: type Iter[U], a: U, b: V, step: S = 1): Iter[U] =
  ## Creates a new Iter in range a..b with specified step (default 1)
  ##

  var
    i = a
    disposed = false

  proc genNext(): U =
    let u = i
    inc(i, step)
    u

  proc isFinished(): bool =
    (step > 0 and i > b) or (step < 0 and i < b)

  proc onDispose() =
    disposed = true

  proc isDisposed(): bool =
    disposed

  Iter[U].new(genNext, isFinished, onDispose, isDisposed)

proc new*[U, V: Ordinal](_: type Iter[U], slice: HSlice[U, V]): Iter[U] =
  ## Creates a new Iter from a slice
  ##

  Iter[U].new(slice.a.int, slice.b.int, 1)

proc new*[T](_: type Iter[T], items: seq[T]): Iter[T] =
  ## Creates a new Iter from a sequence
  ##

  Iter[int].new(0 ..< items.len).map((i: int) => items[i])

proc new*[T](_: type Iter[T], iter: Iterator[T]): Iter[T] =
  ## Creates a new Iter from an iterator
  ##
  var
    nextOrErr: Option[?!T]
    disposed = false

  proc tryNext(): void =
    nextOrErr = none(?!T)
    while not iter.finished:
      try:
        let t: T = iter()
        if not iter.finished:
          nextOrErr = some(success(t))
        break
      except CatchableError as err:
        nextOrErr = some(T.failure(err))

  proc genNext(): T {.raises: [IteratorError].} =
    if nextOrErr.isNone:
      raise newException(IteratorError, "Iterator finished but genNext was called")

    without u =? nextOrErr.unsafeGet, err:
      raise toIteratorError(err)

    tryNext()
    return u

  proc isFinished(): bool =
    nextOrErr.isNone

  proc onDispose() =
    disposed = true

  proc isDisposed(): bool =
    disposed

  tryNext()
  Iter[T].new(genNext, isFinished, onDispose, isDisposed)

proc empty*[T](_: type Iter[T]): Iter[T] =
  ## Creates an empty Iter
  ##

  var disposed = false

  proc genNext(): T {.raises: [IteratorError].} =
    raise newException(IteratorError, "Next item requested from an empty Iter")

  proc isFinished(): bool =
    true

  proc onDispose() =
    disposed = true

  proc isDisposed(): bool =
    disposed

  Iter[T].new(genNext, isFinished, onDispose, isDisposed)

proc map*[T, U](iter: Iter[T], fn: Function[T, U]): Iter[U] =
  multiSync(Iter[U], T, U, iter, fn)

proc mapFilter*[T, U](iter: Iter[T], mapPredicate: Function[T, Option[U]]): Iter[U] =
  var nextUOrErr: Option[?!U]

  proc tryFetch(): void =
    nextUOrErr = none(?!U)
    while not iter.finished:
      try:
        let t = iter.next()
        if u =? mapPredicate(t):
          nextUOrErr = some(success(u))
          break
      except CatchableError as err:
        nextUOrErr = some(U.failure(err))

  proc genNext(): U {.raises: [IteratorError].} =
    if nextUOrErr.isNone:
      raise newException(IteratorError, "Iterator finished but genNext was called")

    # at this point nextUOrErr should always be some(..)
    without u =? nextUOrErr.unsafeGet, err:
      raise toIteratorError(err)

    tryFetch()
    return u

  proc isFinished(): bool =
    nextUOrErr.isNone

  tryFetch()
  # Chain dispose to underlying iterator
  Iter[U].new(
    genNext,
    isFinished,
    dispose = () => iter.dispose(),
    isDisposed = () => iter.disposed,
  )

proc filter*[T](iter: Iter[T], predicate: Function[T, bool]): Iter[T] =
  proc wrappedPredicate(t: T): Option[T] =
    if predicate(t):
      some(t)
    else:
      T.none

  mapFilter[T, T](iter, wrappedPredicate)

proc flatMap*[T, U](iter: Iter[T], fn: Function[T, Iter[U]]): Iter[U] =
  ## Applies `fn` to each item, flattening the returned iterators into a
  ## single stream.  Inner iterators are owned by `fn`'s caller - dispose
  ## them alongside the source via the chained dispose.
  multiSyncFlatMap(
    Iter[U],
    T,
    U,
    iter,
    fn,
    syncAdvance = fn(iter.next()),
    asyncAdvance = await fn(await iter.next()),
  )
