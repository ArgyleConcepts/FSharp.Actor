namespace ArgyleConcepts.FSharp.Actor

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open System.Threading.Channels
open FSharp.Control

/// <summary>
/// A long-running source of messages, active for as long as the model calls for it. Unlike a
/// command, which runs once and reports what it produced, a subscription keeps producing until the
/// model no longer asks for it or the actor shuts down.
/// </summary>
[<NoEquality; NoComparison>]
type Subscription<'Msg> =
    {
        /// <summary>
        /// Identity of the source. A key already running is left alone, so a model change that
        /// does not affect the subscription does not restart it.
        /// </summary>
        Key: string

        /// <summary>
        /// Starts the source. It posts through the supplied function, which returns false once the
        /// mailbox has closed, and stops when the supplied token is cancelled, which happens when
        /// the model retires it or the actor shuts down. Anything the source must register before
        /// it can receive should be registered before its first await.
        /// </summary>
        Start: ('Msg -> bool) -> CancellationToken -> Task
    }

/// A subscription the actor has started: what cancels it, the task observing it, and whether the
/// model has asked for it to stop. Retired entries stay until their source has finished unwinding,
/// so a key is never running twice.
[<NoEquality; NoComparison>]
type internal RunningSubscription =
    { Cts: CancellationTokenSource
      Source: Task
      mutable Retiring: bool }

/// <summary>Whether the actor keeps running or ends, and how, given its current model.</summary>
[<RequireQualifiedAccess; NoComparison>]
type ActorStatus =
    /// <summary>The actor keeps processing messages.</summary>
    | Running
    /// <summary>The actor stops and its external stream completes without an exception.</summary>
    | Stopped
    /// <summary>The actor stops and its external stream completes with the exception.</summary>
    | Failed of exn

/// <summary>
/// Configuration for creating an actor that emits external messages.
/// </summary>
[<NoEquality; NoComparison>]
type ActorConfig<'Msg, 'Model, 'Cmd, 'ExtMsg> =
    {
        /// <summary>
        /// The initial state of the actor, the commands to start it with, and the external
        /// messages to emit before them. A consumer whose first message both starts work and
        /// announces a state change can therefore express it here rather than posting it.
        /// </summary>
        Init: 'Model * 'Cmd list * 'ExtMsg list

        /// <summary>
        /// The update function that processes a message and returns the new model, the commands to
        /// execute in order, and the external messages to emit in order.
        /// </summary>
        Update: 'Msg -> 'Model -> 'Model * 'Cmd list * 'ExtMsg list

        /// <summary>
        /// Starts a command and returns the messages it produced. The actor starts commands in
        /// order but does not wait for them, so a slow command does not block the loop; the
        /// messages of a command that completes later are queued when it completes.
        /// </summary>
        Execute: CancellationToken -> 'Cmd -> Task<'Msg list>

        /// <summary>
        /// Turns a command's or subscription's failure into a message. A cancellation observed
        /// while the actor's own token is cancelled is dropped rather than routed here.
        /// </summary>
        OnCommandFailure: exn -> 'Msg

        /// <summary>
        /// The long-running sources the model calls for. Recomputed after every update: sources
        /// whose key is new are started, and sources whose key is gone are cancelled.
        /// </summary>
        Subscribe: 'Model -> Subscription<'Msg> list

        /// <summary>
        /// Determines from the model whether the actor keeps running, stops cleanly, or fails.
        /// </summary>
        Status: 'Model -> ActorStatus

        /// <summary>
        /// How long shutdown waits for tracked background tasks before completing the stream.
        /// </summary>
        ShutdownTimeout: TimeSpan
    }

/// <summary>
/// An opaque handle to an actor that emits external messages.
/// Interact with actors using the Actor module functions.
/// </summary>
[<NoEquality; NoComparison>]
type Actor<'Msg, 'Model, 'Cmd, 'ExtMsg> =
    private
        { MsgChannel: Channel<'Msg>
          ExtChannel: Channel<'ExtMsg>
          // In-flight commands and running subscriptions, which shutdown waits on.
          InFlight: ConcurrentDictionary<Task, byte>
          Cts: CancellationTokenSource
          // Replaced, never mutated in place, so Actor.model can read a complete snapshot from any thread.
          mutable Model: 'Model ref
          mutable Completion: Task
          mutable CleanUpStarted: int
          Update: 'Msg -> 'Model -> 'Model * 'Cmd list * 'ExtMsg list
          Execute: CancellationToken -> 'Cmd -> Task<'Msg list>
          OnCommandFailure: exn -> 'Msg
          Subscribe: 'Model -> Subscription<'Msg> list
          // Keyed by subscription key, holding what cancels each running source. Owned by the
          // loop, and read by cleanup once the loop has stopped.
          Subscriptions: Dictionary<string, RunningSubscription>
          // Signalled when a source finishes, so the loop re-examines the subscription set even
          // if no message arrives.
          Resync: Channel<unit>
          InitialCommands: 'Cmd list
          InitialExtMsgs: 'ExtMsg list
          Status: 'Model -> ActorStatus
          ShutdownTimeout: TimeSpan }

/// <summary>
/// Functions for creating and interacting with actors that emit external messages.
/// </summary>
[<RequireQualifiedAccess>]
module Actor =

    /// <summary>The default time shutdown waits for in-flight work.</summary>
    let defaultShutdownTimeout = TimeSpan.FromSeconds(1.0)

    let private pruneCompletedTasks (state: Actor<'Msg, 'Model, 'Cmd, 'ExtMsg>) =
        for task in state.InFlight.Keys do
            if task.IsCompleted then
                state.InFlight.Remove(task) |> ignore

    let private track (state: Actor<'Msg, 'Model, 'Cmd, 'ExtMsg>) (task: Task) =
        pruneCompletedTasks state
        state.InFlight.TryAdd(task, 0uy) |> ignore

    let private tryCancel (cts: CancellationTokenSource) =
        try
            cts.Cancel()
        with
        | :? ObjectDisposedException
        | :? AggregateException -> ()

    let private cleanUp (state: Actor<'Msg, 'Model, 'Cmd, 'ExtMsg>) (ex: exn option) =
        task {
            if Interlocked.Exchange(&state.CleanUpStarted, 1) = 0 then
                state.MsgChannel.Writer.TryComplete() |> ignore

                // Stop subscriptions promptly, but give commands already started the shutdown
                // wait to finish before cancelling the actor token.
                for running in state.Subscriptions.Values do
                    tryCancel running.Cts

                let tasks = state.InFlight.Keys |> Seq.toArray

                if tasks.Length > 0 then
                    let tracked = Task.WhenAll(tasks)

                    try
                        do! tracked.WaitAsync(state.ShutdownTimeout, CancellationToken.None)
                    with
                    | :? TimeoutException ->
                        // Observe a fault from tasks that outlive the timeout.
                        tracked.ContinueWith(
                            (fun (t: Task) -> t.Exception |> ignore),
                            CancellationToken.None,
                            TaskContinuationOptions.OnlyOnFaulted,
                            TaskScheduler.Default
                        )
                        |> ignore
                    // Awaiting observed the tracked task fault; shutdown deliberately continues.
                    | _ -> ()

                // Keep effects that outlived the configured shutdown timeout visible to callers
                // that must drain them before performing dependent cleanup.
                pruneCompletedTasks state
                tryCancel state.Cts

                // A source that outlived the wait above may still touch its token, so its token
                // source is disposed when it finally unwinds rather than now.
                for running in state.Subscriptions.Values do
                    if running.Source.IsCompleted then
                        running.Cts.Dispose()
                    else
                        running.Source.ContinueWith(
                            (fun (_: Task) -> running.Cts.Dispose()),
                            CancellationToken.None,
                            TaskContinuationOptions.None,
                            TaskScheduler.Default
                        )
                        |> ignore

                state.Subscriptions.Clear()

                match ex with
                | Some e -> state.ExtChannel.Writer.TryComplete(e) |> ignore
                | None -> state.ExtChannel.Writer.TryComplete() |> ignore

                state.Cts.Dispose()
        }

    let private processLoop (state: Actor<'Msg, 'Model, 'Cmd, 'ExtMsg>) : Task =
        task {
            let token = state.Cts.Token
            let execute = state.Execute token
            let mutable status = ActorStatus.Running
            let mutable mailboxOpen = true

            // Set by a command observer that could not route its failure, and read by the loop once
            // it has drained, so the actor ends with that exception.
            let mutable dispatchFailure: exn | null = null

            let enqueue (msgs: 'Msg list) =
                // Dropped once the mailbox has closed, so a command that finishes after shutdown
                // has started cannot revive the actor.
                for msg in msgs do
                    state.MsgChannel.Writer.TryWrite(msg) |> ignore

            let routeFailure (error: exn) =
                match error with
                | :? OperationCanceledException when token.IsCancellationRequested -> ()
                | _ -> enqueue [ state.OnCommandFailure error ]

            let observe (command: Task<'Msg list>) : Task =
                task {
                    try
                        try
                            let! msgs = command
                            enqueue msgs
                        with error ->
                            routeFailure error
                    with error ->
                        // OnCommandFailure itself threw, which the actor cannot route anywhere.
                        // Recording it and closing the mailbox ends the actor with that exception
                        // rather than leaving this observer faulted where nobody looks.
                        Interlocked.CompareExchange(&dispatchFailure, error, null) |> ignore
                        state.MsgChannel.Writer.TryComplete() |> ignore
                }

            // Subscriptions report failures the way commands do, and a source cancelled because
            // its key was retired, or because the actor is shutting down, is not a failure.
            let observeSource (sourceToken: CancellationToken) (source: Task) : Task =
                task {
                    try
                        try
                            do! source
                        with
                        // Retiring a source cancels it. That is the model's decision, not a
                        // failure, so it is not routed the way a fault is.
                        | :? OperationCanceledException when sourceToken.IsCancellationRequested -> ()
                        | error -> routeFailure error
                    with error ->
                        Interlocked.CompareExchange(&dispatchFailure, error, null) |> ignore
                        state.MsgChannel.Writer.TryComplete() |> ignore
                }

            let startSubscription (subscription: Subscription<'Msg>) =
                let cts = CancellationTokenSource.CreateLinkedTokenSource(token)

                let source =
                    try
                        subscription.Start state.MsgChannel.Writer.TryWrite cts.Token
                    with error ->
                        Task.FromException(error)

                let observed = observeSource cts.Token source

                // The loop owns the subscription set, so a source that has finished asks it to
                // look again rather than editing that set from here. Signalled from a continuation
                // rather than from inside the observer, so the loop cannot see the signal before
                // the task it checks has completed and leave the entry behind.
                observed.ContinueWith(
                    (fun (_: Task) -> state.Resync.Writer.TryWrite() |> ignore),
                    CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default
                )
                |> ignore

                state.Subscriptions[subscription.Key] <-
                    { Cts = cts
                      Source = observed
                      Retiring = false }

                // Tracked so shutdown waits for it alongside in-flight commands.
                track state observed

            let forget (key: string) (running: RunningSubscription) =
                state.Subscriptions.Remove key |> ignore
                // Safe only because the source has finished with the token by now.
                running.Cts.Dispose()

            let syncSubscriptions (model: 'Model) =
                let desired = state.Subscribe model
                let desiredKeys = HashSet<string>(desired |> List.map _.Key)

                for KeyValue(key, running) in List.ofSeq state.Subscriptions do
                    if not (desiredKeys.Contains key) then
                        if not running.Retiring then
                            running.Retiring <- true
                            tryCancel running.Cts

                        if running.Source.IsCompleted then
                            forget key running
                    elif running.Retiring && running.Source.IsCompleted then
                        // Retired and then wanted again: it has finished unwinding, so the key is
                        // free for a fresh source below.
                        forget key running

                for subscription in desired do
                    if not (state.Subscriptions.ContainsKey subscription.Key) then
                        startSubscription subscription

            let startCommand cmd : Task =
                let running =
                    try
                        execute cmd
                    with error ->
                        Task.FromException<'Msg list>(error)

                if running.IsCompleted then
                    // Awaiting a command that has already finished does not yield, so the messages
                    // of synchronous commands stay in command order.
                    observe running
                else
                    // The command joins the in-flight set, which shutdown waits on, and reports
                    // back through the mailbox whenever it finishes.
                    track state running
                    observe running |> ignore
                    Task.CompletedTask

            try
                // Cancelling the supplied token aborts the actor, so a cancelled start emits and
                // runs nothing. Start is inside the try so a Subscribe that throws for the initial
                // model ends the actor the way it would for any later model: through cleanup, with
                // the stream completing.
                if not token.IsCancellationRequested then
                    for extMsg in state.InitialExtMsgs do
                        do! state.ExtChannel.Writer.WriteAsync(extMsg)

                    // Sources the initial model calls for start before its commands, so a listener
                    // is running before anything a command does can produce a reply.
                    syncSubscriptions state.Model.Value

                for cmd in state.InitialCommands do
                    if not token.IsCancellationRequested then
                        do! startCommand cmd

                // Held across iterations so abandoning it is not a per-message cost; it is only
                // replaced once it has fired.
                let mutable resyncWait = ValueNone

                while mailboxOpen && status = ActorStatus.Running && not token.IsCancellationRequested do
                    let mailboxWait = state.MsgChannel.Reader.WaitToReadAsync(token).AsTask()

                    let sourceFinishedWait =
                        match resyncWait with
                        | ValueSome pending -> pending
                        | ValueNone ->
                            let pending = state.Resync.Reader.WaitToReadAsync(token).AsTask()
                            resyncWait <- ValueSome pending
                            pending

                    let! winner = Task.WhenAny(mailboxWait, sourceFinishedWait)

                    // A source finished. The subscription set is the loop's to edit, so it is
                    // brought up to date here rather than from the source's own continuation.
                    if LanguagePrimitives.PhysicalEquality winner sourceFinishedWait then
                        resyncWait <- ValueNone
                        state.Resync.Reader.TryRead() |> ignore
                        syncSubscriptions state.Model.Value
                    else
                        let! available = mailboxWait

                        if not available then
                            // stop completed the mailbox and every message queued before it has been processed.
                            mailboxOpen <- false
                        else
                            match state.MsgChannel.Reader.TryRead() with
                            | true, msg ->
                                let newModel, cmds, extMsgs = state.Update msg state.Model.Value

                                Volatile.Write(&state.Model, ref newModel)

                                for extMsg in extMsgs do
                                    do! state.ExtChannel.Writer.WriteAsync(extMsg)

                                syncSubscriptions newModel

                                for cmd in cmds do
                                    do! startCommand cmd

                                status <- state.Status newModel
                            | false, _ -> ()
            with
            // Only the actor's own token cancels the mailbox wait. A cancellation from anywhere
            // else came out of Update, Subscribe or Status, and is a failure like any other.
            | :? OperationCanceledException when token.IsCancellationRequested -> ()
            | ex -> status <- ActorStatus.Failed ex

            match status, Volatile.Read(&dispatchFailure) with
            | ActorStatus.Failed _, _
            | _, null -> ()
            | _, error -> status <- ActorStatus.Failed error

            let failure =
                match status with
                | ActorStatus.Failed ex -> Some ex
                | ActorStatus.Running
                | ActorStatus.Stopped -> None

            do! cleanUp state failure
        }

    /// <summary>
    /// Creates and starts an actor, returning the actor handle and an async stream of external messages.
    /// Cancelling the supplied token aborts the actor: the pending read is cancelled, queued messages are
    /// dropped, and cleanup runs. Use <c>stop</c> to shut down gracefully instead.
    /// </summary>
    let start (cancellationToken: CancellationToken) (config: ActorConfig<'Msg, 'Model, 'Cmd, 'ExtMsg>) : Actor<'Msg, 'Model, 'Cmd, 'ExtMsg> * IAsyncEnumerable<'ExtMsg> =
        let shutdownTimeout = config.ShutdownTimeout

        // Match Task.WaitAsync's accepted range so an invalid timeout fails here rather than during cleanup.
        if
            shutdownTimeout <> Timeout.InfiniteTimeSpan
            && (shutdownTimeout < TimeSpan.Zero
                || shutdownTimeout.TotalMilliseconds > float UInt32.MaxValue - 1.0)
        then
            raise (
                ArgumentOutOfRangeException(
                    nameof config.ShutdownTimeout,
                    shutdownTimeout,
                    "ShutdownTimeout must be Timeout.InfiniteTimeSpan or between zero and 4294967294 milliseconds."
                )
            )

        let msgOptions = UnboundedChannelOptions(SingleReader = true, SingleWriter = false)

        let extOptions = UnboundedChannelOptions(SingleReader = true, SingleWriter = true)

        let initialModel, initialCommands, initialExtMsgs = config.Init

        let actor =
            { MsgChannel = Channel.CreateUnbounded<'Msg>(msgOptions)
              ExtChannel = Channel.CreateUnbounded<'ExtMsg>(extOptions)
              InFlight = ConcurrentDictionary<Task, byte>()
              Cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
              Model = ref initialModel
              Completion = Task.CompletedTask
              CleanUpStarted = 0
              Update = config.Update
              Execute = config.Execute
              OnCommandFailure = config.OnCommandFailure
              Subscribe = config.Subscribe
              Subscriptions = Dictionary<string, RunningSubscription>()
              Resync = Channel.CreateBounded<unit>(BoundedChannelOptions(1, FullMode = BoundedChannelFullMode.DropWrite))
              InitialCommands = initialCommands
              InitialExtMsgs = initialExtMsgs
              Status = config.Status
              ShutdownTimeout = config.ShutdownTimeout }

        actor.Completion <- processLoop actor
        (actor, actor.ExtChannel.Reader.ReadAllAsync())

    /// <summary>Gets the current model state of the actor.</summary>
    let model (actor: Actor<'Msg, 'Model, 'Cmd, 'ExtMsg>) : 'Model = Volatile.Read(&actor.Model).Value

    /// <summary>
    /// Gets a task that completes when the actor's loop has exited and cleanup has finished.
    /// A terminal state's exception is surfaced through the external message stream, not this task.
    /// </summary>
    let completion (actor: Actor<'Msg, 'Model, 'Cmd, 'ExtMsg>) : Task = actor.Completion

    /// <summary>
    /// Posts a message to the actor for processing.
    /// Returns true if the message was successfully queued, false if the channel is closed.
    /// </summary>
    let post (actor: Actor<'Msg, 'Model, 'Cmd, 'ExtMsg>) (msg: 'Msg) : bool = actor.MsgChannel.Writer.TryWrite(msg)

    /// <summary>
    /// Stops the actor gracefully. The mailbox closes at once, so later posts return false, but every
    /// message already queued is still processed while the model's status stays Running; a terminal
    /// status ends the actor without processing the rest. Once the mailbox is drained, cleanup cancels the
    /// actor's token, waits up to the shutdown timeout for in-flight work, and completes the
    /// stream. Await <c>completion</c> to observe the end of shutdown.
    /// </summary>
    let stop (actor: Actor<'Msg, 'Model, 'Cmd, 'ExtMsg>) : unit =
        actor.MsgChannel.Writer.TryComplete() |> ignore

    /// <summary>
    /// Stops the actor and waits for every tracked command to finish, including commands that
    /// outlive the configured shutdown timeout. Use this before external cleanup that must not
    /// race effects already started by the actor.
    /// </summary>
    let stopAndDrain (actor: Actor<'Msg, 'Model, 'Cmd, 'ExtMsg>) : Task =
        stop actor

        task {
            let mutable completionFailure = None

            try
                do! actor.Completion
            with ex ->
                completionFailure <- Some ex

            let tasks = actor.InFlight.Keys |> Seq.toArray

            if tasks.Length > 0 then
                try
                    do! Task.WhenAll(tasks)
                with _ ->
                    // Command failures have already been observed and routed by the actor.
                    ()

            actor.InFlight.Clear()

            match completionFailure with
            | Some ex -> return raise ex
            | None -> return ()
        }
