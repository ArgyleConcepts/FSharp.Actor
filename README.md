# ArgyleConcepts.FSharp.Actor

[![NuGet](https://img.shields.io/nuget/v/ArgyleConcepts.FSharp.Actor.svg)](https://www.nuget.org/packages/ArgyleConcepts.FSharp.Actor)

An Elm-style (Model–View–Update) actor for F# on .NET, built on `System.Threading.Channels` and `task { }`.

A single loop owns the model and processes one message at a time. `Update` is a pure function from a
message and the current model to a new model, the commands to start, and the external messages to emit.
Side effects run as commands or subscriptions, never inside `Update`, so the loop never blocks on I/O.

- **Commands** start in order but are not awaited by the loop. Each returns the messages it produced,
  which are queued when it completes. Commands can therefore finish, and report back, out of order.
- **Subscriptions** are long-running sources, keyed by name, that the model calls for. After every update
  the actor starts sources whose key is new and cancels sources whose key has gone.
- **External messages** are emitted on an `IAsyncEnumerable` returned by `Actor.start`. The stream
  completes when the actor stops, or faults with the exception when the actor fails.
- **Status** decides from the model whether the actor keeps running, stops cleanly, or fails.
- **Shutdown** is graceful: `Actor.stop` closes the mailbox, already-queued messages are still processed,
  and in-flight work is given `ShutdownTimeout` to finish before the actor's token is cancelled.
  `Actor.stopAndDrain` also waits for work that outlives that timeout.

## Install

```sh
dotnet add package ArgyleConcepts.FSharp.Actor
```

Targets .NET 8 and .NET 10.

## Example

```fsharp
open System
open System.Threading
open System.Threading.Tasks
open ArgyleConcepts.FSharp.Actor

type Msg =
    | Increment
    | Saved of int
    | SaveFailed of exn
    | Tick

type Cmd = Save of int

type Event =
    | CountChanged of int
    | Persisted of int

type Model = { Count: int; Saved: int; Ticking: bool }

let update msg model =
    match msg with
    | Increment ->
        let count = model.Count + 1
        { model with Count = count }, [ Save count ], [ CountChanged count ]
    | Saved count -> { model with Saved = model.Saved + 1 }, [], [ Persisted count ]
    | SaveFailed _ -> model, [], []
    | Tick -> model, [], []

let execute (ct: CancellationToken) cmd : Task<Msg list> =
    task {
        match cmd with
        | Save count ->
            do! Task.Delay(10, ct)
            return [ Saved count ]
    }

let ticker: Subscription<Msg> =
    { Key = "ticker"
      Start =
        fun post ct ->
            task {
                while not ct.IsCancellationRequested && post Tick do
                    do! Task.Delay(TimeSpan.FromSeconds 1.0, ct)
            } }

let actor, events =
    Actor.start
        CancellationToken.None
        { Init = { Count = 0; Saved = 0; Ticking = true }, [], []
          Update = update
          Execute = execute
          OnCommandFailure = SaveFailed
          Subscribe = fun model -> if model.Ticking then [ ticker ] else []
          Status = fun model -> if model.Saved = 3 then ActorStatus.Stopped else ActorStatus.Running
          ShutdownTimeout = Actor.defaultShutdownTimeout }

for _ in 1..3 do
    Actor.post actor Increment |> ignore

task {
    let enumerator = events.GetAsyncEnumerator()
    let mutable more = true

    while more do
        let! next = enumerator.MoveNextAsync()

        if next then printfn "%A" enumerator.Current else more <- false

    do! Actor.completion actor
}
|> _.Wait()
```

## API

| Function | Description |
| --- | --- |
| `Actor.start ct config` | Starts an actor and returns its handle and stream of external messages. Cancelling `ct` aborts the actor. |
| `Actor.post actor msg` | Queues a message. Returns `false` once the mailbox has closed. |
| `Actor.model actor` | Reads a consistent snapshot of the current model from any thread. |
| `Actor.stop actor` | Closes the mailbox and shuts down gracefully once queued messages are processed. |
| `Actor.stopAndDrain actor` | Stops the actor and waits for every started command, even past the shutdown timeout. |
| `Actor.completion actor` | A task that completes when the loop has exited and cleanup has finished. |

## Contributing and support

Bug reports, documentation improvements, tests, and feature proposals are welcome. Start with [CONTRIBUTING.md](https://github.com/ArgyleConcepts/FSharp.Actor/blob/main/CONTRIBUTING.md), then open an [issue](https://github.com/ArgyleConcepts/FSharp.Actor/issues) or a pull request against `main`. You do not need access to our internal issue tracker or Azure DevOps organization to contribute.

Please follow our [Code of Conduct](https://github.com/ArgyleConcepts/FSharp.Actor/blob/main/CODE_OF_CONDUCT.md). Report vulnerabilities privately using the process in [SECURITY.md](https://github.com/ArgyleConcepts/FSharp.Actor/blob/main/SECURITY.md). Maintainers can find CI and release administration in the [maintainer guide](https://github.com/ArgyleConcepts/FSharp.Actor/blob/main/docs/MAINTAINING.md).

## License

[MIT](https://github.com/ArgyleConcepts/FSharp.Actor/blob/main/LICENSE)
