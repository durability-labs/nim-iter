import pkg/questionable
import pkg/chronos
import pkg/iter

import pkg/asynctest/chronos/unittest2

suite "Test AsyncIter":
  test "Should be finished":
    let iter = AsyncIter[int].empty()

    check:
      iter.finished == true

  test "Should map each item using `map`":
    let
      iter1 = AsyncIter[int].new(0 ..< 5).delayBy(10.millis)
      iter2 = map[int, string](
        iter1,
        proc(i: int): Future[string] {.async.} =
          $i,
      )

    var collected: seq[string]

    for fut in iter2:
      collected.add(await fut)

    check:
      collected == @["0", "1", "2", "3", "4"]

  test "Should leave only odd items using `filter`":
    let
      iter1 = AsyncIter[int].new(0 ..< 5).delayBy(10.millis)
      iter2 = await filter[int](
        iter1,
        proc(i: int): Future[bool] {.async.} =
          (i mod 2) == 1,
      )

    var collected: seq[int]

    for fut in iter2:
      collected.add(await fut)

    check:
      collected == @[1, 3]

  test "Should leave only odd items using `mapFilter`":
    let
      iter1 = AsyncIter[int].new(0 ..< 5).delayBy(10.millis)
      iter2 = await mapFilter[int, string](
        iter1,
        proc(i: int): Future[?string] {.async.} =
          if (i mod 2) == 1:
            some($i)
          else:
            string.none,
      )

    var collected: seq[string]

    for fut in iter2:
      collected.add(await fut)

    check:
      collected == @["1", "3"]

  test "Should yield all items before err using `map`":
    let
      iter1 = AsyncIter[int].new(0 ..< 5).delayBy(10.millis)
      iter2 = map[int, string](
        iter1,
        proc(i: int): Future[string] {.async.} =
          if i < 3:
            return $i
          else:
            raise newException(CatchableError, "Some error"),
      )

    var collected: seq[string]

    expect IteratorError:
      for fut in iter2:
        collected.add(await fut)

    check:
      collected == @["0", "1", "2"]
      iter2.finished

  test "Should yield all items before err using `filter`":
    let
      iter1 = AsyncIter[int].new(0 ..< 5).delayBy(10.millis)
      iter2 = await filter[int](
        iter1,
        proc(i: int): Future[bool] {.async.} =
          if i < 3:
            return true
          else:
            raise newException(CatchableError, "Some error"),
      )

    var collected: seq[int]

    expect IteratorError:
      for fut in iter2:
        collected.add(await fut)

    check:
      collected == @[0, 1, 2]
      iter2.finished

  test "Should yield all items before err using `mapFilter`":
    let
      iter1 = AsyncIter[int].new(0 ..< 5).delayBy(10.millis)
      iter2 = await mapFilter[int, string](
        iter1,
        proc(i: int): Future[?string] {.async.} =
          if i < 3:
            return some($i)
          else:
            raise newException(CatchableError, "Some error"),
      )

    var collected: seq[string]

    expect IteratorError:
      for fut in iter2:
        collected.add(await fut)

    check:
      collected == @["0", "1", "2"]
      iter2.finished

  test "Should propagate cancellation error immediately":
    let fut = newFuture[?string]("testasynciter")

    let
      iter1 = AsyncIter[int].new(0 ..< 5).delayBy(10.millis)
      iter2 = await mapFilter[int, string](
        iter1,
        proc(i: int): Future[?string] {.async.} =
          if i < 3:
            return some($i)
          else:
            return await fut,
      )

    proc cancelFut(): Future[void] {.async.} =
      await sleepAsync(100.millis)
      await fut.cancelAndWait()

    asyncSpawn(cancelFut())

    var collected: seq[string]

    expect CancelledError:
      for fut in iter2:
        collected.add(await fut)

    check:
      collected == @["0", "1"]
      iter2.finished

  test "Should not nest IteratorError across async combinators and should preserve the wrapped cause":
    type UserError = object of CatchableError
    let iter = map[int, string](
      map[int, int](
        AsyncIter[int].new(0 ..< 5),
        proc(i: int): Future[int] {.async.} =
          i,
      ),
      proc(i: int): Future[string] {.async.} =
        if i < 3:
          $i
        else:
          raise newException(UserError, "boom"),
    )

    var collected: seq[string]
    var raised: ref IteratorError = nil
    try:
      for fut in iter:
        collected.add(await fut)
    except IteratorError as e:
      raised = e

    check:
      collected == @["0", "1", "2"]
      raised != nil
      raised.parent != nil
      raised.parent of UserError

  test "Contract violation through an async chain keeps parent == nil":
    let iter = map[int, int](
      AsyncIter[int].new(0 ..< 2),
      proc(i: int): Future[int] {.async.} =
        i,
    )
    discard await iter.next()
    discard await iter.next()
    var raised: ref IteratorError = nil
    try:
      discard await iter.next()
    except IteratorError as e:
      raised = e

    check:
      raised != nil
      raised.parent == nil

  test "Should flatMap each item using `flatMap`":
    let iter = flatMap[int, int](
      AsyncIter[int].new(0 ..< 3),
      proc(i: int): Future[AsyncIter[int]] {.async.} =
        AsyncIter[int].new(i ..< i + 2),
    )

    var collected: seq[int]

    for fut in iter:
      collected.add(await fut)

    check:
      collected == @[0, 1, 1, 2, 2, 3]

  test "Should propagate cancellation from a cancelled item":
    let fut = newFuture[int]("testcollect")

    let iter = map[int, int](
      AsyncIter[int].new(0 ..< 3),
      proc(i: int): Future[int] {.async.} =
        if i == 1:
          discard await fut
        i,
    )

    proc cancelFut(): Future[void] {.async.} =
      await sleepAsync(50.millis)
      await fut.cancelAndWait()

    asyncSpawn(cancelFut())

    var raised = false
    try:
      discard await collectAsync(iter)
    except CancelledError:
      raised = true

    check:
      raised
