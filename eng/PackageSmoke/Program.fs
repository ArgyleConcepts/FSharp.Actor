open System
open System.Threading
open System.Threading.Tasks
open ArgyleConcepts.FSharp.Actor

type Msg =
    | Ping
    | Ponged

let actor, events =
    Actor.start
        CancellationToken.None
        { Init = 0, [], []
          Update =
            fun msg count ->
                match msg with
                | Ping -> count, [ () ], []
                | Ponged -> count + 1, [], [ "pong" ]
          Execute = fun _ () -> Task.FromResult [ Ponged ]
          OnCommandFailure = raise
          Subscribe = fun _ -> []
          Status =
            fun count ->
                if count = 1 then
                    ActorStatus.Stopped
                else
                    ActorStatus.Running
          ShutdownTimeout = Actor.defaultShutdownTimeout }

if not (Actor.post actor Ping) then
    failwith "The packed actor rejected its first message."

let received =
    task {
        let messages = ResizeArray()
        let enumerator = events.GetAsyncEnumerator()
        let mutable more = true

        while more do
            let! next = enumerator.MoveNextAsync()

            if next then
                messages.Add enumerator.Current
            else
                more <- false

        do! Actor.completion actor
        return List.ofSeq messages
    }

if not (received.Wait(TimeSpan.FromSeconds 10.0)) then
    failwith "The packed actor did not stop."

if received.Result <> [ "pong" ] then
    failwith $"Unexpected external messages: %A{received.Result}"

printfn "Packed actor started, ran a command, emitted a message and stopped."
