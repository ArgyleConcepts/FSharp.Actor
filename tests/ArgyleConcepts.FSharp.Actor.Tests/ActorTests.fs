module ArgyleConcepts.FSharp.Actor.Tests.ActorTests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open ArgyleConcepts.FSharp.Actor
open Xunit

#region Test Helpers

/// Default timeout for async tests to prevent hanging
let defaultTimeout = TimeSpan.FromSeconds(5.0)

/// Runs an async test with a timeout
let withTimeout (timeout: TimeSpan) (test: CancellationToken -> Task) : Task =
    task {
        use cts = new CancellationTokenSource(timeout)

        try
            do! test cts.Token
        with :? OperationCanceledException ->
            Assert.Fail($"Test timed out after %O{timeout.TotalSeconds} seconds")
    }

/// Reads every remaining message from the enumerator into messages until the stream ends
let private readAll (enumerator: IAsyncEnumerator<'T>) (messages: ResizeArray<'T>) : Task =
    task {
        let mutable hasMore = true

        while hasMore do
            let! next = enumerator.MoveNextAsync()

            if next then
                messages.Add(enumerator.Current)
            else
                hasMore <- false
    }

/// Collects all external messages until the stream completes. Any exception the stream ends with propagates.
let collectMessages (stream: IAsyncEnumerable<'T>) (_ct: CancellationToken) : Task<'T list> =
    task {
        let messages = ResizeArray<'T>()
        use enumerator = stream.GetAsyncEnumerator()

        do! readAll enumerator messages

        return messages |> Seq.toList
    }

/// Collects all external messages until the stream ends with the expected terminal InvalidOperationException.
/// Any other exception, or a stream that completes without one, fails the test.
let collectMessagesUntilTerminal (expectedMessage: string) (stream: IAsyncEnumerable<'T>) : Task<'T list> =
    task {
        let messages = ResizeArray<'T>()
        use enumerator = stream.GetAsyncEnumerator()

        let! terminal =
            Assert.ThrowsAsync<InvalidOperationException>(fun () -> readAll enumerator messages)

        Assert.Equal(expectedMessage, terminal.Message)

        return messages |> Seq.toList
    }

/// Collects up to n external messages from the stream
let collectNMessages (n: int) (stream: IAsyncEnumerable<'T>) (_ct: CancellationToken) : Task<'T list> =
    task {
        let messages = ResizeArray<'T>()

        try
            let enumerator = stream.GetAsyncEnumerator()
            let mutable shouldContinue = true

            while shouldContinue && messages.Count < n do
                let! hasNext = enumerator.MoveNextAsync()

                if hasNext then
                    messages.Add(enumerator.Current)
                else
                    shouldContinue <- false

            do! enumerator.DisposeAsync()
        with :? OperationCanceledException ->
            ()

        return messages |> Seq.toList
    }

#endregion

#region Simple Test Actor Types

type CounterMsg =
    | Increment
    | Decrement
    | GetValue
    | Stop

type CounterCmd =
    | NoOp
    | Reply of int

type CounterExtMsg = ValueChanged of int

let counterUpdate (msg: CounterMsg) (model: int) : int * CounterCmd list * CounterExtMsg list =
    match msg with
    | Increment -> (model + 1, [ NoOp ], [ ValueChanged(model + 1) ])
    | Decrement -> (model - 1, [ NoOp ], [ ValueChanged(model - 1) ])
    | GetValue -> (model, [ Reply model ], [])
    | Stop -> (-1, [ NoOp ], [])

/// Commands in most of these tests cannot fail, so routing a failure means the test's own setup
/// is wrong rather than the actor misbehaving. Tests that exercise failure routing supply their own.
let unexpectedCommandFailure (error: exn) : 'Msg =
    raise (InvalidOperationException($"Unexpected command failure: %O{error}", error))

let counterExecute (_ct: CancellationToken) (cmd: CounterCmd) : Task<CounterMsg list> = Task.FromResult []

let counterStatus (model: int) : ActorStatus =
    if model = -1 then
        ActorStatus.Failed(InvalidOperationException("Stopped"))
    else
        ActorStatus.Running

let createCounterConfig (initial: int) =
    { Init = (initial, [], [])
      Update = counterUpdate
      Execute = counterExecute
      OnCommandFailure = unexpectedCommandFailure
      Subscribe = (fun _ -> [])
      Status = counterStatus
      ShutdownTimeout = Actor.defaultShutdownTimeout }

#endregion

#region Basic Message Processing Tests

[<Fact>]
let ``Actor processes messages and updates model`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let config = createCounterConfig 0
            let actor, stream = Actor.start ct config

            Actor.post actor Increment |> ignore
            Actor.post actor Increment |> ignore
            Actor.post actor Increment |> ignore
            Actor.post actor Stop |> ignore

            let! messages = collectMessagesUntilTerminal "Stopped" stream

            Assert.Equal(3, messages.Length)
            Assert.Equal(ValueChanged 1, messages[0])
            Assert.Equal(ValueChanged 2, messages[1])
            Assert.Equal(ValueChanged 3, messages[2])
        })

[<Fact>]
let ``Actor model reflects current state`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let config = createCounterConfig 10
            let actor, stream = Actor.start ct config

            Actor.post actor Increment |> ignore
            Actor.post actor Increment |> ignore

            // Wait for messages to be processed
            let! _ = collectNMessages 2 stream ct

            Assert.Equal(12, Actor.model actor)
        })

[<Fact>]
let ``Actor completion is pending while the actor is running`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let config = createCounterConfig 0

            let actor, _stream = Actor.start ct config

            Assert.False((Actor.completion actor).IsCompleted)
        })

[<Fact>]
let ``Actor post returns true when channel is open`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let config = createCounterConfig 0

            let actor, _stream = Actor.start ct config

            let result = Actor.post actor Increment

            Assert.True(result)
        })

[<Fact>]
let ``Actor processes messages with no external message`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let config = createCounterConfig 0
            let actor, stream = Actor.start ct config

            // GetValue produces no external message
            Actor.post actor GetValue |> ignore
            Actor.post actor Increment |> ignore
            Actor.post actor Stop |> ignore

            let! messages = collectMessagesUntilTerminal "Stopped" stream

            // Only Increment produces an external message
            Assert.Equal(1, messages.Length)
            Assert.Equal(ValueChanged 1, messages[0])
        })

#endregion

#region Cancellation Tests

[<Fact>]
let ``Actor stops gracefully when cancellation token is cancelled`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            use actorCts = new CancellationTokenSource()
            let config = createCounterConfig 0

            let actor, stream = Actor.start actorCts.Token config

            Actor.post actor Increment |> ignore

            // Wait for the message to be processed
            let! _ = (collectNMessages 1 stream actorCts.Token).WaitAsync(ct)

            // Cancel the actor
            actorCts.Cancel()

            // Wait for cleanup to finish
            do! (Actor.completion actor).WaitAsync(ct)

            // Post should return false after cancellation (channel closed)
            let result = Actor.post actor Increment

            Assert.False(result)
        })

[<Fact>]
let ``Actor stream completes when cancelled`` () =
    withTimeout defaultTimeout (fun _ ->
        task {
            use actorCts = new CancellationTokenSource()
            let config = createCounterConfig 0

            let _actor, stream = Actor.start actorCts.Token config

            actorCts.Cancel()

            // Stream should complete without throwing
            let! messages = collectMessages stream CancellationToken.None

            Assert.Empty(messages)
        })

[<Fact>]
let ``Actor routes a command failure through OnCommandFailure`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let failure = InvalidOperationException("Command failed")

            // A command failure is a message now, so the model decides what it means rather than
            // the failure tearing the loop down where it happened.
            let config: ActorConfig<Result<unit, exn>, exn list, unit, exn> =
                { Init = ([], [], [])
                  Update =
                    (fun msg model ->
                        match msg with
                        | Ok() -> (model, [ () ], [])
                        | Error error -> (model @ [ error ], [], [ error ]))
                  Execute = (fun _ _ -> Task.FromException<Result<unit, exn> list>(failure))
                  OnCommandFailure = Error
                  Subscribe = (fun _ -> [])
                  Status = (fun _ -> ActorStatus.Running)
                  ShutdownTimeout = Actor.defaultShutdownTimeout }

            let actor, stream = Actor.start ct config

            Actor.post actor (Ok()) |> ignore

            let! messages = collectNMessages 1 stream ct

            Assert.Same(failure, Assert.Single messages)
            Assert.Same(failure, Assert.Single(Actor.model actor))

            Actor.stop actor
            do! (Actor.completion actor).WaitAsync(ct)
        })

[<Fact>]
let ``Actor routes the failure of a command that faults after the loop moved on`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let failure = InvalidOperationException("Command failed later")

            let started =
                TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

            let release =
                TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

            let config: ActorConfig<Result<unit, exn>, exn list, unit, exn> =
                { Init = ([], [], [])
                  Update =
                    (fun msg model ->
                        match msg with
                        | Ok() -> (model, [ () ], [])
                        | Error error -> (model @ [ error ], [], [ error ]))
                  Execute =
                    (fun _ _ ->
                        started.SetResult()

                        task {
                            do! release.Task
                            return raise failure
                        })
                  OnCommandFailure = Error
                  Subscribe = (fun _ -> [])
                  Status = (fun _ -> ActorStatus.Running)
                  ShutdownTimeout = Actor.defaultShutdownTimeout }

            let actor, stream = Actor.start ct config

            Actor.post actor (Ok()) |> ignore
            do! started.Task.WaitAsync(ct)

            release.SetResult()

            let! messages = collectNMessages 1 stream ct

            Assert.Same(failure, Assert.Single messages)

            Actor.stop actor
            do! (Actor.completion actor).WaitAsync(ct)
        })

[<Fact>]
let ``Actor drops a command cancellation caused by its own shutdown`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let started =
                TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

            let routed = ResizeArray<exn>()

            // The command observes the actor's token, so cancelling the actor cancels the command.
            let config: ActorConfig<int, int, int, int> =
                { Init = (0, [], [])
                  Update = (fun msg model -> (model + msg, [ msg ], []))
                  Execute =
                    (fun token _ ->
                        started.SetResult()

                        task {
                            do! Task.Delay(Timeout.InfiniteTimeSpan, token)
                            return []
                        })
                  OnCommandFailure =
                    (fun error ->
                        routed.Add error
                        0)
                  Subscribe = (fun _ -> [])
                  Status = (fun _ -> ActorStatus.Running)
                  ShutdownTimeout = TimeSpan.FromMilliseconds(50.0) }

            use actorCts = new CancellationTokenSource()
            let actor, stream = Actor.start actorCts.Token config
            let collecting = collectMessages stream CancellationToken.None

            Actor.post actor 1 |> ignore
            do! started.Task.WaitAsync(ct)

            actorCts.Cancel()

            let! _ = collecting.WaitAsync(ct)
            do! (Actor.completion actor).WaitAsync(ct)

            Assert.Empty routed
        })

[<Fact>]
let ``Actor stream completes when cancelled while a command is executing`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            use actorCts = new CancellationTokenSource()

            let executeStarted =
                TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

            let releaseExecute =
                TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

            // Execute ignores the actor token, so cancellation is only seen once the loop resumes.
            let config: ActorConfig<unit, int, unit, unit> =
                { Init = (0, [], [])
                  Update = (fun (_: unit) model -> (model + 1, [ () ], []))
                  Execute =
                    (fun _ _ ->
                        task {
                            executeStarted.SetResult()
                            do! releaseExecute.Task
                            return []
                        })
                  OnCommandFailure = unexpectedCommandFailure
                  Subscribe = (fun _ -> [])
                  Status = (fun _ -> ActorStatus.Running)
                  ShutdownTimeout = Actor.defaultShutdownTimeout }

            let actor, stream = Actor.start actorCts.Token config
            let collecting = collectMessages stream CancellationToken.None

            Actor.post actor () |> ignore
            do! executeStarted.Task.WaitAsync(ct)

            actorCts.Cancel()
            releaseExecute.SetResult()

            let! messages = collecting.WaitAsync(ct)
            do! (Actor.completion actor).WaitAsync(ct)

            Assert.Empty(messages)
            Assert.False(Actor.post actor ())
        })

[<Fact>]
let ``Actor releases a pending stream read when cancelled`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            use actorCts = new CancellationTokenSource()
            let actor, stream = Actor.start actorCts.Token (createCounterConfig 0)
            let enumerator = stream.GetAsyncEnumerator(CancellationToken.None)
            // No external messages are produced, so this consumer stays in the stream read until shutdown.
            let pendingRead = enumerator.MoveNextAsync().AsTask()

            Assert.False(pendingRead.IsCompleted)
            actorCts.Cancel()

            let! hasMessage = pendingRead.WaitAsync(ct)
            do! (Actor.completion actor).WaitAsync(ct)
            do! enumerator.DisposeAsync()

            Assert.False(hasMessage)
            Assert.False(Actor.post actor Increment)
        })

#endregion

#region Terminal State Tests

[<Fact>]
let ``Actor terminates when status is Failed`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let config = createCounterConfig 0
            let actor, stream = Actor.start ct config

            Actor.post actor Increment |> ignore
            Actor.post actor Stop |> ignore // This sets model to -1, triggering terminal

            let! messages = collectMessagesUntilTerminal "Stopped" stream

            Assert.Equal(1, messages.Length)
            Assert.Equal(ValueChanged 1, messages[0])
        })

[<Fact>]
let ``Actor post returns false after terminal state`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let config = createCounterConfig 0
            let actor, stream = Actor.start ct config

            Actor.post actor Stop |> ignore

            // Wait for terminal state to be reached
            let! _ = (collectMessagesUntilTerminal "Stopped" stream).WaitAsync(ct)

            // Wait for cleanup to finish
            do! (Actor.completion actor).WaitAsync(ct)

            let result = Actor.post actor Increment

            Assert.False(result)
        })

[<Fact>]
let ``Actor stream throws exception from terminal state`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            // Create a config that terminates with a specific exception
            let customException = InvalidOperationException("Custom terminal error")

            let terminalConfig: ActorConfig<unit, int, unit, unit> =
                { Init = (0, [], [])
                  Update = (fun (_: unit) (model: int) -> (model, [ () ], []))
                  Execute = (fun _ _ -> Task.FromResult [])
                  OnCommandFailure = unexpectedCommandFailure
                  Subscribe = (fun _ -> [])
                  Status = (fun _ -> ActorStatus.Failed customException)
                  ShutdownTimeout = Actor.defaultShutdownTimeout }

            let actor, stream = Actor.start ct terminalConfig

            Actor.post actor () |> ignore

            // The stream should throw the exception
            let mutable caughtException: exn option = None

            try
                let enumerator = stream.GetAsyncEnumerator()

                let mutable hasMore = true

                while hasMore do
                    let! next = enumerator.MoveNextAsync()

                    if next then () else hasMore <- false

                do! enumerator.DisposeAsync()
            with ex ->
                caughtException <- Some ex

            Assert.True(caughtException.IsSome)

            match caughtException with
            | Some(:? InvalidOperationException as ioe) -> Assert.Equal("Custom terminal error", ioe.Message)
            | Some ex -> Assert.Fail($"Expected InvalidOperationException but got %O{ex.GetType().Name}")
            | None -> Assert.Fail("Expected exception but none was thrown")
        })

[<Fact>]
let ``Actor stream completes with terminal exception when cleanup cancellation throws`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let terminalException = InvalidOperationException("Terminal")

            // The registration throws when cleanup cancels the actor token.
            let config: ActorConfig<unit, int, unit, unit> =
                { Init = (0, [], [])
                  Update = (fun (_: unit) model -> (model + 1, [ () ], []))
                  Execute =
                    (fun token _ ->
                        token.Register(fun () -> raise (ApplicationException("Registration failed")))
                        |> ignore

                        Task.FromResult [])
                  OnCommandFailure = unexpectedCommandFailure
                  Subscribe = (fun _ -> [])
                  Status =
                    (fun model ->
                        if model > 0 then
                            ActorStatus.Failed terminalException
                        else
                            ActorStatus.Running)
                  ShutdownTimeout = Actor.defaultShutdownTimeout }

            let actor, stream = Actor.start ct config
            let enumerator = stream.GetAsyncEnumerator(ct)

            Actor.post actor () |> ignore

            let completion = Actor.completion actor
            do! completion.WaitAsync(ct)

            Assert.True(completion.IsCompletedSuccessfully)

            let! actual =
                Assert.ThrowsAsync<InvalidOperationException>(fun () -> enumerator.MoveNextAsync().AsTask())

            Assert.Same(terminalException, actual)
        })

[<Fact>]
let ``Actor stream fails with the exception thrown from Update`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let failure = InvalidOperationException("Update failed")

            let config: ActorConfig<unit, int, unit, unit> =
                { Init = (0, [], [])
                  Update = (fun (_: unit) (_: int) -> raise failure)
                  Execute = (fun _ _ -> Task.FromResult [])
                  OnCommandFailure = unexpectedCommandFailure
                  Subscribe = (fun _ -> [])
                  Status = (fun _ -> ActorStatus.Running)
                  ShutdownTimeout = Actor.defaultShutdownTimeout }

            let actor, stream = Actor.start ct config
            let enumerator = stream.GetAsyncEnumerator(ct)

            Actor.post actor () |> ignore

            let! actual =
                Assert.ThrowsAsync<InvalidOperationException>(fun () -> enumerator.MoveNextAsync().AsTask())

            Assert.Same(failure, actual)

            let completion = Actor.completion actor
            do! completion.WaitAsync(ct)

            Assert.True(completion.IsCompletedSuccessfully)
            Assert.False(Actor.post actor ())
        })

[<Fact>]
let ``Actor stream fails with the exception thrown from Subscribe for the initial model`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let failure = InvalidOperationException("Subscribe failed")

            let config: ActorConfig<unit, int, unit, unit> =
                { Init = (0, [], [])
                  Update = (fun (_: unit) (model: int) -> (model, [], []))
                  Execute = (fun _ _ -> Task.FromResult [])
                  OnCommandFailure = unexpectedCommandFailure
                  Subscribe = (fun (_: int) -> raise failure)
                  Status = (fun _ -> ActorStatus.Running)
                  ShutdownTimeout = Actor.defaultShutdownTimeout }

            let actor, stream = Actor.start ct config
            let enumerator = stream.GetAsyncEnumerator(ct)

            // Start itself is the failing step, so this must fail rather than hang.
            let! actual =
                Assert.ThrowsAsync<InvalidOperationException>(fun () -> enumerator.MoveNextAsync().AsTask())

            Assert.Same(failure, actual)

            let completion = Actor.completion actor
            do! completion.WaitAsync(ct)

            Assert.True(completion.IsCompletedSuccessfully)
            Assert.False(Actor.post actor ())
        })

[<Fact>]
let ``Actor stream completes without an exception when status is Stopped`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let config: ActorConfig<int, int, unit, int> =
                { Init = (0, [], [])
                  Update = (fun msg _ -> (msg, [ () ], [ msg ]))
                  Execute = (fun _ _ -> Task.FromResult [])
                  OnCommandFailure = unexpectedCommandFailure
                  Subscribe = (fun _ -> [])
                  Status =
                    (fun model ->
                        if model < 0 then
                            ActorStatus.Stopped
                        else
                            ActorStatus.Running)
                  ShutdownTimeout = Actor.defaultShutdownTimeout }

            let actor, stream = Actor.start ct config

            Actor.post actor 1 |> ignore
            Actor.post actor -1 |> ignore

            // collectMessages propagates any exception the stream ends with.
            let! messages = (collectMessages stream ct).WaitAsync(ct)

            Assert.Equal<int list>([ 1; -1 ], messages)

            let completion = Actor.completion actor
            do! completion.WaitAsync(ct)

            Assert.True(completion.IsCompletedSuccessfully)
            Assert.False(Actor.post actor 2)
        })

[<Fact>]
let ``Actor stop processes every message posted before it without cancelling the running command`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let firstStarted =
                TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

            let releaseFirst =
                TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

            let firstToken = ref CancellationToken.None

            let config: ActorConfig<int, int, int, int> =
                { Init = (0, [], [])
                  Update = (fun msg _ -> (msg, [ msg ], [ msg ]))
                  Execute =
                    (fun token cmd ->
                        if cmd = 1 then
                            firstToken.Value <- token
                            firstStarted.SetResult()

                            task {
                                do! releaseFirst.Task
                                return []
                            }
                        else
                            Task.FromResult [])
                  OnCommandFailure = unexpectedCommandFailure
                  Subscribe = (fun _ -> [])
                  Status = (fun _ -> ActorStatus.Running)
                  ShutdownTimeout = TimeSpan.FromMinutes(1.0) }

            let actor, stream = Actor.start ct config
            let collecting = collectMessages stream ct

            for i in 1..3 do
                Actor.post actor i |> ignore

            do! firstStarted.Task.WaitAsync(ct)

            Actor.stop actor

            Assert.False(Actor.post actor 4)
            Assert.False(firstToken.Value.IsCancellationRequested)

            releaseFirst.SetResult()

            // collectMessages propagates any exception the stream ends with.
            let! messages = collecting.WaitAsync(ct)

            Assert.Equal<int list>([ 1; 2; 3 ], messages)

            let completion = Actor.completion actor
            do! completion.WaitAsync(ct)

            Assert.True(completion.IsCompletedSuccessfully)
        })

[<Fact>]
let ``Actor stop completes cleanly when a running command posts after the mailbox closes`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let started =
                TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

            let release =
                TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

            let config: ActorConfig<int, int, int, int> =
                { Init = (0, [], [])
                  Update = (fun msg _ -> (msg, [ msg ], [ msg ]))
                  Execute =
                    (fun _ cmd ->
                        task {
                            if cmd = 1 then
                                started.SetResult()
                                do! release.Task
                                return [ 2 ]
                            else
                                return []
                        })
                  OnCommandFailure = unexpectedCommandFailure
                  Subscribe = (fun _ -> [])
                  Status = (fun _ -> ActorStatus.Running)
                  ShutdownTimeout = Actor.defaultShutdownTimeout }

            let actor, stream = Actor.start ct config
            let collecting = collectMessages stream ct

            Actor.post actor 1 |> ignore
            do! started.Task.WaitAsync(ct)

            Actor.stop actor
            release.SetResult()

            // The follow-up is posted after the mailbox closed, so it is dropped and the stream ends cleanly.
            let! messages = collecting.WaitAsync(ct)

            Assert.Equal<int list>([ 1 ], messages)
        })

[<Fact>]
let ``Actor terminal status does not process messages queued behind it`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let terminalStarted =
                TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

            let releaseTerminal =
                TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

            let terminalStatusPostResult =
                TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously)

            let mutable postDuringTerminalStatus: (int -> bool) option = None

            let config: ActorConfig<int, int, int, int> =
                { Init = (0, [], [])
                  Update = (fun msg _ -> (msg, [ msg ], [ msg ]))
                  Execute =
                    (fun _ cmd ->
                        if cmd < 0 then
                            terminalStarted.SetResult()

                            task {
                                do! releaseTerminal.Task
                                return []
                            }
                        else
                            Task.FromResult [])
                  OnCommandFailure = unexpectedCommandFailure
                  Subscribe = (fun _ -> [])
                  Status =
                    (fun model ->
                        if model < 0 then
                            let post =
                                match postDuringTerminalStatus with
                                | Some post -> post
                                | None -> invalidOp "Actor post must be initialized before terminal status."

                            terminalStatusPostResult.SetResult(post 2)
                            ActorStatus.Stopped
                        else
                            ActorStatus.Running)
                  ShutdownTimeout = Actor.defaultShutdownTimeout }

            let actor, stream = Actor.start ct config
            postDuringTerminalStatus <- Some(Actor.post actor)
            let collecting = collectMessages stream ct

            Actor.post actor 1 |> ignore
            Actor.post actor -1 |> ignore
            do! terminalStarted.Task.WaitAsync(ct)
            releaseTerminal.SetResult()

            let! messages = collecting.WaitAsync(ct)
            let! terminalStatusPostSucceeded = terminalStatusPostResult.Task.WaitAsync(ct)

            Assert.Equal<int list>([ 1; -1 ], messages)
            Assert.True(terminalStatusPostSucceeded)
        })

[<Fact>]
let ``Actor emits a step's external messages without waiting for its commands`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            // Only the actor loop writes to this, and the test reads it while a command is blocked.
            let executed = ResizeArray<int>()

            let secondStarted =
                TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

            let releaseSecond =
                TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

            let config: ActorConfig<int, int, int, int> =
                { Init = (0, [], [])
                  Update = (fun msg _ -> (msg, [ msg; msg * 10 ], [ msg; msg * 100 ]))
                  Execute =
                    (fun _ cmd ->
                        executed.Add cmd

                        if cmd = 10 then
                            secondStarted.SetResult()

                            task {
                                do! releaseSecond.Task
                                return []
                            }
                        else
                            Task.FromResult [])
                  OnCommandFailure = unexpectedCommandFailure
                  Subscribe = (fun _ -> [])
                  Status = (fun _ -> ActorStatus.Running)
                  ShutdownTimeout = Actor.defaultShutdownTimeout }

            let actor, stream = Actor.start ct config
            let enumerator = stream.GetAsyncEnumerator(ct)

            Actor.post actor 1 |> ignore
            do! secondStarted.Task.WaitAsync(ct)

            // Both commands have been started and the second is still running, yet the step's
            // external messages are already readable: commands no longer hold the loop.
            Assert.Equal<int list>([ 1; 10 ], List.ofSeq executed)

            let! hasFirst = enumerator.MoveNextAsync()

            Assert.True(hasFirst)
            Assert.Equal(1, enumerator.Current)

            let! hasSecond = enumerator.MoveNextAsync()

            Assert.True(hasSecond)
            Assert.Equal(100, enumerator.Current)

            releaseSecond.SetResult()

            Actor.stop actor
            do! (Actor.completion actor).WaitAsync(ct)
        })

[<Fact>]
let ``Actor keeps processing messages while a command is still running`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let firstStarted =
                TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

            let releaseFirst =
                TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

            // 1 blocks until released and then reports back as 3; every other message is a no-op.
            let config: ActorConfig<int, int list, int, int> =
                { Init = ([], [], [])
                  Update = (fun msg model -> (model @ [ msg ], [ msg ], [ msg ]))
                  Execute =
                    (fun _ cmd ->
                        if cmd = 1 then
                            firstStarted.SetResult()

                            task {
                                do! releaseFirst.Task
                                return [ 3 ]
                            }
                        else
                            Task.FromResult [])
                  OnCommandFailure = unexpectedCommandFailure
                  Subscribe = (fun _ -> [])
                  Status = (fun _ -> ActorStatus.Running)
                  ShutdownTimeout = Actor.defaultShutdownTimeout }

            let actor, stream = Actor.start ct config
            let enumerator = stream.GetAsyncEnumerator(ct)

            Actor.post actor 1 |> ignore
            do! firstStarted.Task.WaitAsync(ct)

            // The command for 1 is still running, so under the old inline dispatch the loop would
            // be stuck here and 2 could not be processed.
            Actor.post actor 2 |> ignore

            let! hasFirst = enumerator.MoveNextAsync()
            Assert.True(hasFirst)
            Assert.Equal(1, enumerator.Current)

            let! hasSecond = enumerator.MoveNextAsync()
            Assert.True(hasSecond)
            Assert.Equal(2, enumerator.Current)

            // The blocked command's message is processed once it completes.
            releaseFirst.SetResult()

            let! hasThird = enumerator.MoveNextAsync()
            Assert.True(hasThird)
            Assert.Equal(3, enumerator.Current)

            Assert.Equal<int list>([ 1; 2; 3 ], Actor.model actor)

            Actor.stop actor
            do! (Actor.completion actor).WaitAsync(ct)
        })

[<Fact>]
let ``Actor enqueues the messages of synchronous commands in command order`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let config: ActorConfig<int, int list, int, int> =
                { Init = ([], [], [])
                  // Only the first message produces commands, so the model records the order their
                  // messages were enqueued in.
                  Update =
                    (fun msg model ->
                        if msg = 0 then
                            (model, [ 1; 2; 3 ], [])
                        else
                            (model @ [ msg ], [], [ msg ]))
                  Execute = (fun _ cmd -> Task.FromResult [ cmd * 10 ])
                  OnCommandFailure = unexpectedCommandFailure
                  Subscribe = (fun _ -> [])
                  Status = (fun _ -> ActorStatus.Running)
                  ShutdownTimeout = Actor.defaultShutdownTimeout }

            let actor, stream = Actor.start ct config

            Actor.post actor 0 |> ignore

            let! messages = collectNMessages 3 stream ct

            Assert.Equal<int list>([ 10; 20; 30 ], messages)

            Actor.stop actor
            do! (Actor.completion actor).WaitAsync(ct)
        })

[<Fact>]
let ``Actor shutdown waits for an in-flight command up to the timeout`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let started =
                TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

            let release =
                TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
            // Records whether the command or cleanup finished first.
            let order = ResizeArray<string>()

            // The shutdown timeout outlasts the test, so only releasing the command ends the wait.
            let config: ActorConfig<int, int, int, int> =
                { Init = (0, [], [])
                  Update = (fun msg model -> (model + msg, [ msg ], []))
                  Execute =
                    (fun _ _ ->
                        started.SetResult()

                        task {
                            do! release.Task
                            order.Add "command"
                            return []
                        })
                  OnCommandFailure = unexpectedCommandFailure
                  Subscribe = (fun _ -> [])
                  Status = (fun _ -> ActorStatus.Running)
                  ShutdownTimeout = TimeSpan.FromMinutes(1.0) }

            let actor, _stream = Actor.start ct config

            Actor.post actor 1 |> ignore
            do! started.Task.WaitAsync(ct)

            Actor.stop actor
            let completion = Actor.completion actor

            // Cleanup cannot finish while the command is in flight. An actor that did not track it
            // would complete here instead of waiting to be released.
            let! finishedFirst =
                Task.WhenAny(completion, Task.Delay(TimeSpan.FromMilliseconds(250.0), ct))

            Assert.False(completion.IsCompleted, "Cleanup finished while a command was still in flight.")

            Assert.NotSame(completion, finishedFirst)

            release.SetResult()
            do! completion.WaitAsync(ct)
            order.Add "cleanup"

            // The ordering is the real proof: cleanup ended only after the command it was waiting on.
            Assert.Equal<string list>([ "command"; "cleanup" ], List.ofSeq order)
        })

[<Fact>]
let ``Actor does not start Init commands when the supplied token is already cancelled`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let executed = ResizeArray<int>()

            let config: ActorConfig<int, int, int, int> =
                { Init = (0, [ 1; 2 ], [])
                  Update = (fun msg model -> (model + msg, [], []))
                  Execute =
                    (fun _ cmd ->
                        executed.Add cmd
                        Task.FromResult [])
                  OnCommandFailure = unexpectedCommandFailure
                  Subscribe = (fun _ -> [])
                  Status = (fun _ -> ActorStatus.Running)
                  ShutdownTimeout = Actor.defaultShutdownTimeout }

            use cancelled = new CancellationTokenSource()
            cancelled.Cancel()

            let actor, stream = Actor.start cancelled.Token config
            let! _ = (collectMessages stream CancellationToken.None).WaitAsync(ct)
            do! (Actor.completion actor).WaitAsync(ct)

            // Starting with a cancelled token aborts the actor, so its commands never run.
            Assert.Empty executed
            Assert.False(Actor.post actor 3)
        })

[<Fact>]
let ``Actor ends with the failure when OnCommandFailure itself throws`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let configFailure = InvalidOperationException("OnCommandFailure is broken")

            let release =
                TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

            // The command fails asynchronously, so its failure is routed from the observer rather
            // than from the loop, and routing it throws.
            let config: ActorConfig<int, int, int, int> =
                { Init = (0, [], [])
                  Update = (fun msg model -> (model + msg, [ msg ], []))
                  Execute =
                    (fun _ _ ->
                        task {
                            do! release.Task
                            return raise (InvalidOperationException("Command failed"))
                        })
                  OnCommandFailure = (fun _ -> raise configFailure)
                  Subscribe = (fun _ -> [])
                  Status = (fun _ -> ActorStatus.Running)
                  ShutdownTimeout = Actor.defaultShutdownTimeout }

            let actor, stream = Actor.start ct config
            let collecting = collectMessages stream CancellationToken.None

            Actor.post actor 1 |> ignore
            release.SetResult()

            let! actual = Assert.ThrowsAsync<InvalidOperationException>(fun () -> collecting)

            Assert.Same(configFailure, actual)
            do! (Actor.completion actor).WaitAsync(ct)
        })

[<Fact>]
let ``Actor stop and drain waits for commands beyond the shutdown timeout`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let started =
                TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

            let release =
                TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

            let config: ActorConfig<int, int, int, int> =
                { Init = (0, [], [])
                  Update = (fun msg model -> (model + msg, [ msg ], []))
                  Execute =
                    (fun _ _ ->
                        task {
                            started.SetResult()
                            do! release.Task
                            return []
                        })
                  OnCommandFailure = unexpectedCommandFailure
                  Subscribe = (fun _ -> [])
                  Status = (fun _ -> ActorStatus.Running)
                  ShutdownTimeout = TimeSpan.FromMilliseconds(10.0) }

            let actor, _stream = Actor.start ct config
            Actor.post actor 1 |> ignore
            do! started.Task.WaitAsync(ct)

            let drained = Actor.stopAndDrain actor
            do! (Actor.completion actor).WaitAsync(ct)

            Assert.False(drained.IsCompleted)
            release.SetResult()
            do! drained.WaitAsync(ct)
        })

[<Fact>]
let ``Actor drops the messages of a command that completes after shutdown`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let started =
                TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

            let release =
                TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

            let config: ActorConfig<int, int list, int, int> =
                { Init = ([], [], [])
                  Update = (fun msg model -> (model @ [ msg ], [ msg ], [ msg ]))
                  Execute =
                    (fun _ cmd ->
                        if cmd = 1 then
                            started.SetResult()

                            task {
                                do! release.Task
                                return [ 99 ]
                            }
                        else
                            Task.FromResult [])
                  OnCommandFailure = unexpectedCommandFailure
                  Subscribe = (fun _ -> [])
                  Status = (fun _ -> ActorStatus.Running)
                  // Short enough that shutdown gives up on the blocked command.
                  ShutdownTimeout = TimeSpan.FromMilliseconds(20.0) }

            let actor, stream = Actor.start ct config
            let collecting = collectMessages stream CancellationToken.None

            Actor.post actor 1 |> ignore
            do! started.Task.WaitAsync(ct)

            Actor.stop actor
            do! (Actor.completion actor).WaitAsync(ct)

            // The command reports back only after the mailbox is closed, so 99 is never processed.
            release.SetResult()

            let! messages = collecting.WaitAsync(ct)

            Assert.Equal<int list>([ 1 ], messages)
            Assert.Equal<int list>([ 1 ], Actor.model actor)
        })

[<Fact>]
let ``Actor emits the external messages Init supplies before its commands run`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            use actorCts = new CancellationTokenSource()

            // The command cancels the actor before returning. Everything the actor does after
            // starting a command is therefore skipped, so the initial external messages can only
            // be readable if they were written first.
            let config: ActorConfig<int, int list, int, int> =
                { Init = ([], [ 5 ], [ 1; 2 ])
                  Update = (fun msg model -> (model @ [ msg ], [], [ msg ]))
                  Execute =
                    (fun _ _ ->
                        actorCts.Cancel()
                        Task.FromResult [])
                  OnCommandFailure = unexpectedCommandFailure
                  Subscribe = (fun _ -> [])
                  Status = (fun _ -> ActorStatus.Running)
                  ShutdownTimeout = Actor.defaultShutdownTimeout }

            let actor, stream = Actor.start actorCts.Token config

            let! messages = collectMessages stream CancellationToken.None
            do! (Actor.completion actor).WaitAsync(ct)

            Assert.Equal<int list>([ 1; 2 ], messages)
        })

[<Fact>]
let ``Actor starts the commands Init supplies`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let config: ActorConfig<int, int list, int, int> =
                { Init = ([], [ 7; 8 ], [])
                  Update = (fun msg model -> (model @ [ msg ], [], [ msg ]))
                  Execute = (fun _ cmd -> Task.FromResult [ cmd ])
                  OnCommandFailure = unexpectedCommandFailure
                  Subscribe = (fun _ -> [])
                  Status = (fun _ -> ActorStatus.Running)
                  ShutdownTimeout = Actor.defaultShutdownTimeout }

            let actor, stream = Actor.start ct config

            let! messages = collectNMessages 2 stream ct

            Assert.Equal<int list>([ 7; 8 ], messages)

            Actor.stop actor
            do! (Actor.completion actor).WaitAsync(ct)
        })

[<Fact>]
let ``Actor runs every command and emits every external message in order`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            // Only the actor loop writes to this, and the test reads it after shutdown.
            let executed = ResizeArray<int>()

            let config: ActorConfig<int, int, int, int> =
                { Init = (0, [], [])
                  Update = (fun msg _ -> (msg, [ msg; msg * 10 ], [ msg; msg * 100 ]))
                  Execute =
                    (fun _ cmd ->
                        executed.Add cmd
                        Task.FromResult [])
                  OnCommandFailure = unexpectedCommandFailure
                  Subscribe = (fun _ -> [])
                  Status = (fun _ -> ActorStatus.Running)
                  ShutdownTimeout = Actor.defaultShutdownTimeout }

            let actor, stream = Actor.start ct config
            let collecting = collectMessages stream ct

            Actor.post actor 1 |> ignore
            Actor.post actor 2 |> ignore
            Actor.stop actor

            let! messages = collecting.WaitAsync(ct)

            Assert.Equal<int list>([ 1; 100; 2; 200 ], messages)
            Assert.Equal<int list>([ 1; 10; 2; 20 ], List.ofSeq executed)
        })

[<Fact>]
let ``Actor runs no command and emits nothing for empty lists`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let executed = ResizeArray<int>()

            let config: ActorConfig<int, int, int, int> =
                { Init = (0, [], [])
                  Update = (fun msg model -> (model + msg, [], []))
                  Execute =
                    (fun _ cmd ->
                        executed.Add cmd
                        Task.FromResult [])
                  OnCommandFailure = unexpectedCommandFailure
                  Subscribe = (fun _ -> [])
                  Status = (fun _ -> ActorStatus.Running)
                  ShutdownTimeout = Actor.defaultShutdownTimeout }

            let actor, stream = Actor.start ct config
            let collecting = collectMessages stream ct

            Actor.post actor 5 |> ignore
            Actor.stop actor

            let! messages = collecting.WaitAsync(ct)

            Assert.Empty(messages)
            Assert.Empty(executed)
            Assert.Equal(5, Actor.model actor)
        })

#endregion

#region Command Execution Tests

type CmdActorMsg =
    | DoWork of int
    | WorkDone of int
    | Terminate

type CmdActorCmd =
    | Execute of int
    | Noop

let cmdUpdate (msg: CmdActorMsg) (model: int list) : int list * CmdActorCmd list * int list =
    match msg with
    | DoWork n -> (model, [ Execute n ], [])
    | WorkDone n -> (model @ [ n ], [ Noop ], [ n ])
    | Terminate -> ([ -1 ], [ Noop ], [])

let cmdExecute (ct: CancellationToken) (cmd: CmdActorCmd) : Task<CmdActorMsg list> =
    task {
        match cmd with
        | Execute n ->
            do! Task.Delay(10, ct) // Simulate async work
            return [ WorkDone(n * 2) ]
        | Noop -> return []
    }

let cmdStatus (model: int list) : ActorStatus =
    if model = [ -1 ] then
        ActorStatus.Failed(InvalidOperationException("Terminated"))
    else
        ActorStatus.Running

[<Fact>]
let ``Actor executes commands and posts follow-up messages`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let config =
                { Init = ([], [], [])
                  Update = cmdUpdate
                  Execute = cmdExecute
                  OnCommandFailure = unexpectedCommandFailure
                  Subscribe = (fun _ -> [])
                  Status = cmdStatus
                  ShutdownTimeout = Actor.defaultShutdownTimeout }

            let actor, stream = Actor.start ct config

            Actor.post actor (DoWork 1) |> ignore
            Actor.post actor (DoWork 2) |> ignore

            // Wait for the WorkDone messages to be processed (Execute has Task.Delay(10))
            let! messages = collectNMessages 2 stream ct

            // Now terminate
            Actor.post actor Terminate |> ignore

            // Collect any remaining messages
            let! _ = collectMessagesUntilTerminal "Terminated" stream

            // Each DoWork's command reports back as WorkDone. The two commands run concurrently
            // now, so they can finish in either order; what the actor guarantees is that both
            // results are processed, not which lands first.
            Assert.Equal<int list>([ 2; 4 ], List.sort messages)
        })

#endregion

#region Message Ordering Tests

[<Fact>]
let ``Actor processes messages in FIFO order`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let orderUpdate (msg: int) (model: int list) = (model @ [ msg ], [ () ], [ msg ])

            let orderExecute _ _ = Task.FromResult []
            let orderStatus (_: int list) = ActorStatus.Running

            let config: ActorConfig<int, int list, unit, int> =
                { Init = ([], [], [])
                  Update = orderUpdate
                  Execute = orderExecute
                  OnCommandFailure = unexpectedCommandFailure
                  Subscribe = (fun _ -> [])
                  Status = orderStatus
                  ShutdownTimeout = Actor.defaultShutdownTimeout }

            let actor, stream = Actor.start ct config

            // Post messages rapidly
            for i in 1..10 do
                Actor.post actor i |> ignore

            let! messages = collectNMessages 10 stream ct

            // Messages should be in order
            Assert.Equal<int list>([ 1; 2; 3; 4; 5; 6; 7; 8; 9; 10 ], messages)
        })

#endregion

#region Concurrent Posting Tests

[<Fact>]
let ``Actor handles concurrent posts from multiple threads`` () =
    withTimeout (TimeSpan.FromSeconds(10.0)) (fun ct ->
        task {
            let mutable processedCount = 0

            let concurrentUpdate (msg: int) (model: int) =
                Interlocked.Increment(&processedCount) |> ignore

                (model + msg, [ () ], [ model + msg ])

            let concurrentExecute _ _ = Task.FromResult []

            let concurrentStatus (_: int) = ActorStatus.Running

            let config: ActorConfig<int, int, unit, int> =
                { Init = (0, [], [])
                  Update = concurrentUpdate
                  Execute = concurrentExecute
                  OnCommandFailure = unexpectedCommandFailure
                  Subscribe = (fun _ -> [])
                  Status = concurrentStatus
                  ShutdownTimeout = Actor.defaultShutdownTimeout }

            let actor, stream = Actor.start ct config

            let numTasks = 10
            let messagesPerTask = 100

            // Start multiple tasks posting concurrently
            let tasks =
                [| for _ in 1..numTasks do
                       Task.Run(
                           (fun () ->
                               for _ in 1..messagesPerTask do
                                   Actor.post actor 1 |> ignore),
                           ct
                       ) |]

            do! Task.WhenAll(tasks)

            // Collect all messages
            let! messages = collectNMessages (numTasks * messagesPerTask) stream ct

            Assert.Equal(numTasks * messagesPerTask, messages.Length)
        })

#endregion

#region Edge Cases

[<Fact>]
let ``Actor with initial terminal state terminates immediately`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let immediateTerminal (_: int) : ActorStatus =
                ActorStatus.Failed(InvalidOperationException("Already terminal"))

            let config: ActorConfig<unit, int, unit, int> =
                { Init = (0, [], [])
                  Update = (fun (_: unit) (model: int) -> (model, [ () ], [ model ]))
                  Execute = (fun _ _ -> Task.FromResult [])
                  OnCommandFailure = unexpectedCommandFailure
                  Subscribe = (fun _ -> [])
                  Status = immediateTerminal
                  ShutdownTimeout = Actor.defaultShutdownTimeout }

            let actor, stream = Actor.start ct config

            Actor.post actor () |> ignore

            let mutable caughtException = false

            try
                let enumerator = stream.GetAsyncEnumerator()

                let mutable hasMore = true

                while hasMore do
                    let! next = enumerator.MoveNextAsync()

                    if next then () else hasMore <- false

                do! enumerator.DisposeAsync()
            with _ ->
                caughtException <- true

            Assert.True(caughtException)
        })

[<Fact>]
let ``Actor handles rapid start and stop`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            for _ in 1..10 do
                use cts = new CancellationTokenSource()
                let config = createCounterConfig 0

                let actor, stream = Actor.start cts.Token config

                Actor.post actor Increment |> ignore

                // Wait for the message to be processed
                let! _ = (collectNMessages 1 stream ct).WaitAsync(ct)

                cts.Cancel()

                // Verify the actor shuts down without error
                let completion = Actor.completion actor
                do! completion.WaitAsync(ct)

                Assert.True(completion.IsCompletedSuccessfully)
        })

[<Fact>]
let ``Actor model function returns initial model before any messages`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let config = createCounterConfig 42

            let actor, _stream = Actor.start ct config

            Assert.Equal(42, Actor.model actor)
        })

#endregion

#region Task Tracking Tests

type TrackingMsg =
    | StartBackgroundWork
    | BackgroundWorkComplete
    | Stop

[<Fact>]
let ``Actor chains the messages its commands return until the model is terminal`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            // Each command asks for the next message, so the actor walks itself to its terminal
            // state through nothing but command results.
            let postExecute (_ct: CancellationToken) (cmd: int) : Task<int list> =
                if cmd > 0 && cmd < 5 then
                    Task.FromResult [ cmd + 1 ]
                else
                    Task.FromResult []

            let postUpdate (msg: int) (model: int) = (model + 1, [ msg ], [ model ])

            let postStatus model =
                if model >= 5 then
                    ActorStatus.Failed(InvalidOperationException("Done"))
                else
                    ActorStatus.Running

            let config =
                { Init = (0, [], [])
                  Update = postUpdate
                  Execute = postExecute
                  OnCommandFailure = unexpectedCommandFailure
                  Subscribe = (fun _ -> [])
                  Status = postStatus
                  ShutdownTimeout = Actor.defaultShutdownTimeout }

            let actor, stream = Actor.start ct config

            Actor.post actor 1 |> ignore

            // Each message triggers post of next message until 5
            let! messages = collectNMessages 4 stream ct

            // Should have received 4 external messages (values 0,1,2,3)
            Assert.Equal(4, messages.Length)
        })

/// Starts an actor whose command faults after the loop has moved on, stops it, and waits for
/// shutdown. The command's task is what the in-flight set holds, so this is the faulted in-flight
/// work shutdown has to observe.
let stopActorWithFaultedTrackedTask (failure: exn) (ct: CancellationToken) : Task =
    task {
        let tracked =
            TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

        let config: ActorConfig<unit, int, unit, unit> =
            { Init = (0, [], [])
              Update = (fun (_: unit) model -> (model + 1, [ () ], []))
              Execute =
                (fun _ _ ->
                    task {
                        tracked.SetResult()
                        do! Task.Yield()
                        return raise failure
                    })
              OnCommandFailure = (fun _ -> ())
              Subscribe = (fun _ -> [])
              Status = (fun _ -> ActorStatus.Running)
              ShutdownTimeout = Actor.defaultShutdownTimeout }

        let actor, _stream = Actor.start CancellationToken.None config

        Actor.post actor () |> ignore
        do! tracked.Task.WaitAsync(ct)

        Actor.stop actor
        do! (Actor.completion actor).WaitAsync(ct)
    }

[<Fact>]
let ``Actor shutdown observes a faulted tracked task`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let failure = ApplicationException("Tracked task failed") :> exn
            let unobserved = ref false

            let handler =
                EventHandler<UnobservedTaskExceptionEventArgs>(fun _ args ->
                    if
                        args.Exception.InnerExceptions
                        |> Seq.exists (LanguagePrimitives.PhysicalEquality failure)
                    then
                        unobserved.Value <- true)

            TaskScheduler.UnobservedTaskException.AddHandler handler

            try
                // Signal from a continuation so the helper task has completed before the GC runs, and
                // keep no reference to it so it cannot root the actor's tasks. Its outcome, including
                // any fault, is copied to `stopped`.
                let stopped =
                    TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

                (stopActorWithFaultedTrackedTask failure ct)
                    .ContinueWith(
                        (fun (helper: Task) -> stopped.SetFromTask(helper)),
                        ct,
                        TaskContinuationOptions.None,
                        TaskScheduler.Default
                    )
                |> ignore

                do! stopped.Task.WaitAsync(ct)

                GC.Collect()
                GC.WaitForPendingFinalizers()
                GC.Collect()
                GC.WaitForPendingFinalizers()

                Assert.False(unobserved.Value)
            finally
                TaskScheduler.UnobservedTaskException.RemoveHandler handler
        })

[<Fact>]
let ``Actor shutdown does not wait beyond the configured shutdown timeout`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let tracked =
                TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

            let neverCompletes = TaskCompletionSource()

            // The command never finishes, so only the timeout can end shutdown.
            let config: ActorConfig<unit, int, unit, unit> =
                { Init = (0, [], [])
                  Update = (fun (_: unit) model -> (model + 1, [ () ], []))
                  Execute =
                    (fun _ _ ->
                        task {
                            tracked.SetResult()
                            do! neverCompletes.Task
                            return []
                        })
                  OnCommandFailure = unexpectedCommandFailure
                  Subscribe = (fun _ -> [])
                  Status = (fun _ -> ActorStatus.Running)
                  ShutdownTimeout = TimeSpan.FromMilliseconds(10.0) }

            let actor, _stream = Actor.start ct config

            Actor.post actor () |> ignore
            do! tracked.Task.WaitAsync(ct)

            Actor.stop actor

            // Well under the one-second default, so only the configured timeout can satisfy it.
            do! (Actor.completion actor).WaitAsync(TimeSpan.FromMilliseconds(500.0), ct)
        })

[<Theory>]
[<InlineData(-2.0)>]
[<InlineData(4294967295.0)>]
let ``Actor start rejects an unsupported shutdown timeout`` (milliseconds: float) =
    let config =
        { createCounterConfig 0 with
            ShutdownTimeout = TimeSpan.FromMilliseconds(milliseconds) }

    let ex =
        Assert.Throws<ArgumentOutOfRangeException>(fun () -> Actor.start CancellationToken.None config |> ignore)

    Assert.Equal("ShutdownTimeout", ex.ParamName)

[<Fact>]
let ``Actor start accepts an infinite shutdown timeout`` () =
    let config =
        { createCounterConfig 0 with
            ShutdownTimeout = Timeout.InfiniteTimeSpan }

    let actor, _stream = Actor.start CancellationToken.None config

    Actor.stop actor

[<Fact>]
let ``Actor shutdown waits for tracked tasks before completing`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let tracked =
                TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

            let release =
                TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

            // The shutdown timeout outlasts the test timeout, so only releasing the tracked task ends the wait.
            let config: ActorConfig<unit, int, unit, unit> =
                { Init = (0, [], [])
                  Update = (fun (_: unit) model -> (model + 1, [ () ], []))
                  Execute =
                    (fun _ _ ->
                        task {
                            tracked.TrySetResult() |> ignore
                            do! release.Task
                            return []
                        })
                  OnCommandFailure = unexpectedCommandFailure
                  Subscribe = (fun _ -> [])
                  Status = (fun _ -> ActorStatus.Running)
                  ShutdownTimeout = TimeSpan.FromMinutes(1.0) }

            let actor, stream = Actor.start ct config
            let enumerator = stream.GetAsyncEnumerator(ct)

            Actor.post actor () |> ignore
            do! tracked.Task.WaitAsync(ct)

            Actor.stop actor

            // A bounded negative check, not synchronization: while the tracked task is held, only its
            // release or the one-minute timeout can complete shutdown, so this window cannot fail
            // spuriously. Without the wait, shutdown would complete well inside it.
            let completion = Actor.completion actor

            let! first =
                Task.WhenAny(completion, Task.Delay(TimeSpan.FromMilliseconds(200.0), ct))

            Assert.NotSame(completion, first)

            release.SetResult()
            do! completion.WaitAsync(ct)

            Assert.True(completion.IsCompletedSuccessfully)

            let! hasNext = enumerator.MoveNextAsync()
            Assert.False(hasNext)
        })

#endregion

#region Completion Tests

[<Fact>]
let ``Actor completion completes after stop`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let actor, stream = Actor.start ct (createCounterConfig 0)

            Actor.post actor Increment |> ignore
            let! _ = collectNMessages 1 stream ct

            let completion = Actor.completion actor
            Assert.False(completion.IsCompleted)

            Actor.stop actor
            do! completion.WaitAsync(ct)

            Assert.True(completion.IsCompletedSuccessfully)
        })

[<Fact>]
let ``Actor completion completes successfully after terminal state`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let actor, stream = Actor.start ct (createCounterConfig 0)
            let enumerator = stream.GetAsyncEnumerator(ct)

            let completion = Actor.completion actor
            Assert.False(completion.IsCompleted)

            Actor.post actor CounterMsg.Stop |> ignore
            do! completion.WaitAsync(ct)

            Assert.True(completion.IsCompletedSuccessfully)

            let! terminal =
                Assert.ThrowsAsync<InvalidOperationException>(fun () -> enumerator.MoveNextAsync().AsTask())

            Assert.Equal("Stopped", terminal.Message)
        })

#endregion

#region Subscription Tests

/// A subscription that reports when it starts and when it stops, and posts on demand.
type private TestSource() =
    let started =
        TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

    let stopped =
        TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

    let mutable starts = 0

    member _.Started = started.Task
    member _.Stopped = stopped.Task
    member _.Starts = Volatile.Read(&starts)

    member this.Subscription(key: string) : Subscription<int> =
        { Key = key
          Start =
            fun _post token ->
                task {
                    Interlocked.Increment(&starts) |> ignore
                    started.TrySetResult() |> ignore

                    try
                        do! Task.Delay(Timeout.InfiniteTimeSpan, token)
                    finally
                        stopped.TrySetResult() |> ignore
                } }

[<Fact>]
let ``Actor starts a subscription when the model calls for it and leaves it alone while it does`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let source = TestSource()

            let config: ActorConfig<int, int, int, int> =
                { Init = (0, [], [])
                  Update = (fun msg model -> (model + msg, [], [ msg ]))
                  Execute = (fun _ _ -> Task.FromResult [])
                  OnCommandFailure = unexpectedCommandFailure
                  // Wanted from the first message onwards.
                  Subscribe = (fun model -> if model > 0 then [ source.Subscription "listen" ] else [])
                  Status = (fun _ -> ActorStatus.Running)
                  ShutdownTimeout = Actor.defaultShutdownTimeout }

            let actor, stream = Actor.start ct config
            let enumerator = stream.GetAsyncEnumerator(ct)

            Assert.False source.Started.IsCompleted

            Actor.post actor 1 |> ignore
            do! source.Started.WaitAsync(ct)

            // Further updates keep the key, so the source is not restarted.
            Actor.post actor 1 |> ignore
            let! _ = enumerator.MoveNextAsync()
            let! _ = enumerator.MoveNextAsync()

            Assert.Equal(1, source.Starts)
            Assert.False source.Stopped.IsCompleted

            Actor.stop actor
            do! (Actor.completion actor).WaitAsync(ct)
        })

[<Fact>]
let ``Actor cancels a subscription when the model stops calling for it`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let source = TestSource()
            let routed = ResizeArray<exn>()

            let config: ActorConfig<int, int, int, int> =
                { Init = (1, [], [])
                  Update = (fun msg model -> (model + msg, [], [ msg ]))
                  Execute = (fun _ _ -> Task.FromResult [])
                  OnCommandFailure =
                    (fun error ->
                        routed.Add error
                        0)
                  Subscribe = (fun model -> if model > 0 then [ source.Subscription "listen" ] else [])
                  Status = (fun _ -> ActorStatus.Running)
                  ShutdownTimeout = Actor.defaultShutdownTimeout }

            let actor, stream = Actor.start ct config
            let collecting = collectMessages stream CancellationToken.None

            do! source.Started.WaitAsync(ct)

            // Takes the model to zero, which no longer calls for the source.
            Actor.post actor -1 |> ignore

            do! source.Stopped.WaitAsync(ct)

            Actor.stop actor
            do! (Actor.completion actor).WaitAsync(ct)

            // Retirement is the model's decision. Reporting the cancellation it causes as a
            // failure would, for a consumer that fails on OnCommandFailure, end the session.
            let! _ = collecting.WaitAsync(ct)

            Assert.Empty routed
        })

[<Fact>]
let ``Actor cancels and awaits its subscriptions at shutdown`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let cancelled =
                TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

            let releaseSource =
                TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

            // The source refuses to finish until the test lets it, so cleanup can only complete
            // if it is genuinely waiting for it.
            let config: ActorConfig<int, int, int, int> =
                { Init = (1, [], [])
                  Update = (fun msg model -> (model + msg, [], []))
                  Execute = (fun _ _ -> Task.FromResult [])
                  OnCommandFailure = unexpectedCommandFailure
                  Subscribe =
                    (fun _ ->
                        [ { Key = "listen"
                            Start =
                              fun _ token ->
                                  task {
                                      use _registration = token.Register(fun () -> cancelled.TrySetResult() |> ignore)

                                      do! releaseSource.Task
                                  } } ])
                  Status = (fun _ -> ActorStatus.Running)
                  ShutdownTimeout = TimeSpan.FromMinutes(1.0) }

            let actor, _stream = Actor.start ct config

            Actor.stop actor
            let completion = Actor.completion actor

            // Shutdown cancelled the source, and is now waiting for it rather than finishing.
            do! cancelled.Task.WaitAsync(ct)
            do! Task.Delay(TimeSpan.FromMilliseconds 100.0, ct)

            Assert.False(completion.IsCompleted, "Cleanup finished without waiting for its subscription.")

            releaseSource.TrySetResult() |> ignore
            do! completion.WaitAsync(ct)
        })

[<Fact>]
let ``Actor restarts a retired subscription once the model calls for it again`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let unwind =
                TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

            let mutable starts = 0
            let mutable live = 0
            let mutable mostLive = 0

            // The source holds on after being cancelled until the test lets it go, so the model
            // asks for the key again while the previous source is still alive.
            let config: ActorConfig<int, int, int, int> =
                { Init = (1, [], [])
                  Update = (fun msg model -> (model + msg, [], [ msg ]))
                  Execute = (fun _ _ -> Task.FromResult [])
                  OnCommandFailure = unexpectedCommandFailure
                  Subscribe =
                    (fun model ->
                        if model > 0 then
                            [ { Key = "listen"
                                Start =
                                  fun _ token ->
                                      task {
                                          Interlocked.Increment(&starts) |> ignore
                                          let current = Interlocked.Increment(&live)
                                          Interlocked.Exchange(&mostLive, max mostLive current) |> ignore

                                          try
                                              try
                                                  do! Task.Delay(Timeout.InfiniteTimeSpan, token)
                                              with :? OperationCanceledException ->
                                                  do! unwind.Task
                                          finally
                                              Interlocked.Decrement(&live) |> ignore
                                      } } ]
                        else
                            [])
                  Status = (fun _ -> ActorStatus.Running)
                  ShutdownTimeout = Actor.defaultShutdownTimeout }

            let actor, stream = Actor.start ct config
            let enumerator = stream.GetAsyncEnumerator(ct)

            let waitFor (condition: unit -> bool) =
                task {
                    let deadline = DateTimeOffset.UtcNow.AddSeconds 5.0

                    while not (condition ()) && DateTimeOffset.UtcNow < deadline do
                        do! Task.Delay(TimeSpan.FromMilliseconds 10.0, ct)
                }

            do! waitFor (fun () -> Volatile.Read(&starts) = 1)

            // Retire it, then ask for it again while it is still unwinding.
            Actor.post actor -1 |> ignore
            let! _ = enumerator.MoveNextAsync()
            Actor.post actor 1 |> ignore
            let! _ = enumerator.MoveNextAsync()

            // A second source must not run alongside the first.
            do! Task.Delay(TimeSpan.FromMilliseconds 100.0, ct)
            Assert.Equal(1, Volatile.Read(&mostLive))

            unwind.TrySetResult() |> ignore

            // Once the old source is gone the key is free, and the model still wants it.
            do! waitFor (fun () -> Volatile.Read(&starts) = 2)

            Assert.Equal(2, Volatile.Read(&starts))
            Assert.Equal(1, Volatile.Read(&mostLive))

            Actor.stop actor
            do! (Actor.completion actor).WaitAsync(ct)
        })

[<Fact>]
let ``Actor routes a subscription failure through OnCommandFailure`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let failure = InvalidOperationException("the source died")

            let config: ActorConfig<Result<unit, exn>, exn list, unit, exn> =
                { Init = ([], [], [])
                  Update =
                    (fun msg model ->
                        match msg with
                        | Ok() -> (model, [], [])
                        | Error error -> (model @ [ error ], [], [ error ]))
                  Execute = (fun _ _ -> Task.FromResult [])
                  OnCommandFailure = Error
                  Subscribe =
                    (fun _ ->
                        [ { Key = "listen"
                            Start = fun _ _ -> Task.FromException failure } ])
                  Status = (fun _ -> ActorStatus.Running)
                  ShutdownTimeout = Actor.defaultShutdownTimeout }

            let actor, stream = Actor.start ct config

            let! messages = collectNMessages 1 stream ct

            Assert.Same(failure, Assert.Single messages)

            Actor.stop actor
            do! (Actor.completion actor).WaitAsync(ct)
        })

[<Fact>]
let ``Actor does not route a subscription cancelled by its own shutdown`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let routed = ResizeArray<exn>()
            let source = TestSource()

            let config: ActorConfig<int, int, int, int> =
                { Init = (1, [], [])
                  Update = (fun msg model -> (model + msg, [], []))
                  Execute = (fun _ _ -> Task.FromResult [])
                  OnCommandFailure =
                    (fun error ->
                        routed.Add error
                        0)
                  Subscribe = (fun _ -> [ source.Subscription "listen" ])
                  Status = (fun _ -> ActorStatus.Running)
                  ShutdownTimeout = Actor.defaultShutdownTimeout }

            let actor, _stream = Actor.start ct config

            do! source.Started.WaitAsync(ct)

            Actor.stop actor
            do! (Actor.completion actor).WaitAsync(ct)

            Assert.Empty routed
        })

[<Fact>]
let ``Actor processes the messages a subscription posts`` () =
    withTimeout defaultTimeout (fun ct ->
        task {
            let config: ActorConfig<int, int list, int, int> =
                { Init = ([], [], [])
                  Update = (fun msg model -> (model @ [ msg ], [], [ msg ]))
                  Execute = (fun _ _ -> Task.FromResult [])
                  OnCommandFailure = unexpectedCommandFailure
                  Subscribe =
                    (fun _ ->
                        [ { Key = "ticks"
                            Start =
                              fun post token ->
                                  task {
                                      for tick in 1..3 do
                                          if not token.IsCancellationRequested then
                                              post tick |> ignore

                                      do! Task.Delay(Timeout.InfiniteTimeSpan, token)
                                  } } ])
                  Status = (fun _ -> ActorStatus.Running)
                  ShutdownTimeout = Actor.defaultShutdownTimeout }

            let actor, stream = Actor.start ct config

            let! messages = collectNMessages 3 stream ct

            Assert.Equal<int list>([ 1; 2; 3 ], messages)

            Actor.stop actor
            do! (Actor.completion actor).WaitAsync(ct)
        })

#endregion
