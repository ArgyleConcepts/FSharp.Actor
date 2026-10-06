open System
open System.Collections.Generic
open System.Diagnostics
open System.IO
open System.Text.Json
open System.Text.RegularExpressions

if fsi.CommandLineArgs |> Array.exists ((=) "--help") then
    printfn "Usage: dotnet fsi eng/fsharp-analysis/Run.fsx"
    printfn "Runs broad informational F# analysis, the repository correctness gate, and curated FSharpLint rules."
    printfn "Set FSHARP_ANALYSIS_OUTPUT_DIRECTORY to choose where logs and SARIF reports are written."
    Environment.Exit(0)

let private scriptDirectory = __SOURCE_DIRECTORY__

let private repositoryRoot =
    Path.GetFullPath(Path.Combine(scriptDirectory, "..", ".."))

let private outputDirectory =
    match Environment.GetEnvironmentVariable("FSHARP_ANALYSIS_OUTPUT_DIRECTORY") with
    | value when not (String.IsNullOrWhiteSpace(value)) -> Path.GetFullPath(value)
    | _ -> Path.Combine(repositoryRoot, "fsharp-analysis-results")

Directory.CreateDirectory(outputDirectory) |> ignore

let private solutionPath =
    Path.Combine(repositoryRoot, "ArgyleConcepts.FSharp.Actor.slnx")

// Projects outside the solution build. AnalyzerPackages only restores the F# analyzer packages below, and
// PackageSmoke restores against locally packed packages in eng/verify-packages.sh.
let private toolingProjects =
    [ Path.Combine(repositoryRoot, "eng", "fsharp-analysis", "AnalyzerPackages.csproj")
      Path.Combine(repositoryRoot, "eng", "PackageSmoke", "PackageSmoke.fsproj") ]

let private startProcess (executable: string) (arguments: string list) (redirectOutput: bool) =
    let startInfo = ProcessStartInfo(executable)
    startInfo.WorkingDirectory <- repositoryRoot
    startInfo.UseShellExecute <- false
    startInfo.RedirectStandardOutput <- redirectOutput
    startInfo.RedirectStandardError <- redirectOutput

    for argument in arguments do
        startInfo.ArgumentList.Add(argument)

    let childProcess = new Process(StartInfo = startInfo)

    if not (childProcess.Start()) then
        failwith $"Failed to start {executable}."

    childProcess

let private run (executable: string) (arguments: string list) =
    use childProcess = startProcess executable arguments false
    childProcess.WaitForExit()

    if childProcess.ExitCode <> 0 then
        failwith $"{executable} exited with code {childProcess.ExitCode}."

let private runCaptured (executable: string) (arguments: string list) =
    use childProcess = startProcess executable arguments true
    let standardOutput = childProcess.StandardOutput.ReadToEndAsync()
    let standardError = childProcess.StandardError.ReadToEndAsync()
    childProcess.WaitForExit()
    childProcess.ExitCode, standardOutput.Result, standardError.Result

let private writeLog (name: string) (standardOutput: string) (standardError: string) =
    let path = Path.Combine(outputDirectory, name)

    use writer = new StreamWriter(path, false)
    writer.Write(standardOutput)

    if not (String.IsNullOrWhiteSpace(standardError)) then
        writer.WriteLine()
        writer.Write(standardError)

    path

let private runLogged (name: string) (executable: string) (arguments: string list) =
    let exitCode, standardOutput, standardError = runCaptured executable arguments
    let logPath = writeLog name standardOutput standardError

    if exitCode <> 0 then
        Console.Error.WriteLine(File.ReadAllText(logPath))

    exitCode

let private excludedSourceDirectories =
    HashSet<string>(
        [ ".git"; ".packages"; ".worktrees"; "bin"; "node_modules"; "obj" ],
        StringComparer.OrdinalIgnoreCase
    )

let private archivedSourceFiles (pattern: string) =
    let rec enumerate directory =
        seq {
            yield! Directory.EnumerateFiles(directory, pattern)

            for subdirectory in Directory.EnumerateDirectories(directory) do
                if not (excludedSourceDirectories.Contains(Path.GetFileName(subdirectory))) then
                    yield! enumerate subdirectory
        }

    enumerate repositoryRoot |> Seq.sort |> Seq.toArray

let private sourceFiles (pattern: string) =
    let gitDirectory = Path.Combine(repositoryRoot, ".git")

    if Directory.Exists(gitDirectory) || File.Exists(gitDirectory) then
        let exitCode, standardOutput, standardError =
            runCaptured "git" [ "ls-files"; pattern ]

        if exitCode <> 0 then
            failwith $"git ls-files failed: {standardError}"

        standardOutput.Split([| '\r'; '\n' |], StringSplitOptions.RemoveEmptyEntries)
        |> Array.map (fun path -> Path.GetFullPath(Path.Combine(repositoryRoot, path)))
    else
        archivedSourceFiles pattern

let private appendOption (option: string) (values: string array) (arguments: string list) =
    arguments @ [ option ] @ (values |> Array.toList)

let private verifyProjectCoverage () =
    let graphPath = Path.Combine(outputDirectory, "project-restore-graph.json")

    run
        "dotnet"
        [ "msbuild"
          solutionPath
          "-target:GenerateRestoreGraphFile"
          $"-property:RestoreGraphOutputPath={graphPath}"
          "-property:Configuration=Release" ]

    use graph = JsonDocument.Parse(File.ReadAllText(graphPath))

    let coveredProjects = HashSet<string>(StringComparer.OrdinalIgnoreCase)

    for project in graph.RootElement.GetProperty("restore").EnumerateObject() do
        coveredProjects.Add(Path.GetFullPath(project.Name)) |> ignore

    let intentionallyExcludedProjects =
        HashSet<string>(toolingProjects, StringComparer.OrdinalIgnoreCase)

    let trackedProjects = Array.append (sourceFiles "*.csproj") (sourceFiles "*.fsproj")

    let uncoveredProjects =
        trackedProjects
        |> Array.filter (fun path ->
            not (coveredProjects.Contains(path))
            && not (intentionallyExcludedProjects.Contains(path)))
        |> Array.map (fun path -> Path.GetRelativePath(repositoryRoot, path))
        |> Array.sort

    if uncoveredProjects.Length > 0 then
        uncoveredProjects
        |> Array.iter (fun path ->
            Console.Error.WriteLine($"Project is not covered by the Release solution build: {path}"))

        failwith "Tracked application projects must be solution members or transitive project references."

    printfn
        "Verified Release-build coverage for %d tracked projects (%d solution or transitive, %d intentionally tooling-only)."
        trackedProjects.Length
        coveredProjects.Count
        intentionallyExcludedProjects.Count

let private analyzerArguments sources reportPath =
    let excluded = HashSet<string>(toolingProjects, StringComparer.OrdinalIgnoreCase)

    let projects =
        sourceFiles "*.fsproj"
        |> Array.filter (fun path -> not (excluded.Contains(path)))

    let packageRoot = Path.Combine(scriptDirectory, ".packages")

    let analyzerPaths =
        [| Path.Combine(packageRoot, "ionide.analyzers", "0.19.0", "analyzers", "dotnet", "fs")
           Path.Combine(packageRoot, "g-research.fsharp.analyzers", "0.25.0", "analyzers", "dotnet", "fs")
           Path.Combine(packageRoot, "woofware.fsharpanalyzers", "0.2.18", "analyzers", "dotnet", "fs") |]

    for path in analyzerPaths do
        if not (Directory.Exists(path)) then
            failwith $"Analyzer package directory was not restored: {path}"

    [ "tool"; "run"; "fsharp-analyzers"; "--"; "--configuration"; "Release" ]
    |> appendOption "--project" projects
    |> appendOption "--analyzers-path" analyzerPaths
    |> appendOption "--include-files" sources
    |> fun arguments ->
        arguments
        @ [ "--exclude-analyzers"
            "StructDiscriminatedUnionAnalyzer"
            "--treat-as-info"
            "*"
            "--code-root"
            repositoryRoot + string Path.DirectorySeparatorChar
            "--report"
            reportPath ]

let private failIfNonZero description exitCode =
    if exitCode <> 0 then
        failwith $"{description} failed with exit code {exitCode}."

let private printSarifSummary path =
    use document = JsonDocument.Parse(File.ReadAllText(path))

    let findings =
        document.RootElement.GetProperty("runs").EnumerateArray()
        |> Seq.collect (fun run -> run.GetProperty("results").EnumerateArray())
        |> Seq.map (fun result -> result.GetProperty("ruleId").GetString())
        |> Seq.choose Option.ofObj
        |> Seq.countBy id
        |> Seq.sortByDescending snd
        |> Seq.toArray

    printfn "Informational findings: %d" (findings |> Array.sumBy snd)

    findings |> Array.iter (fun (rule, count) -> printfn "  %s: %d" rule count)

printfn "Verifying tracked project coverage..."
verifyProjectCoverage ()

printfn "Restoring pinned F# analysis tools and analyzer packages..."
run "dotnet" [ "tool"; "restore" ]

run
    "dotnet"
    [ "restore"
      Path.Combine(scriptDirectory, "AnalyzerPackages.csproj")
      "--locked-mode" ]

let allSources = sourceFiles "*.fs"

// FSharpLint's typed noPartialFunctions rule is incompatible with the current F# compiler AST.
// Keep the high-value collection/option checks as a deterministic source gate until that rule is compatible again.
let forbiddenPartialFunctionPatterns =
    [| Regex(
           @"\b(?:List|Array|Seq)\.(?:head|tail|last|item|find|findBack|pick|reduce|reduceBack|max|min|maxBy|minBy|average|averageBy|exactlyOne)\b",
           RegexOptions.Compiled
       )
       Regex(@"\b(?:Option|ValueOption)\.get\b", RegexOptions.Compiled)
       Regex(@"\bMap\.find\b", RegexOptions.Compiled) |]

let forbiddenPartialFunctionUsages =
    allSources
    |> Array.collect (fun path ->
        File.ReadLines(path)
        |> Seq.mapi (fun index line -> index + 1, line)
        |> Seq.filter (fun (_, line) ->
            let trimmed = line.TrimStart()

            not (trimmed.StartsWith("//", StringComparison.Ordinal))
            && (forbiddenPartialFunctionPatterns
                |> Array.exists (fun pattern -> pattern.IsMatch(line))))
        |> Seq.map (fun (lineNumber, line) ->
            $"{Path.GetRelativePath(repositoryRoot, path)}:{lineNumber}: {line.Trim()}")
        |> Seq.toArray)

if forbiddenPartialFunctionUsages.Length > 0 then
    forbiddenPartialFunctionUsages |> Array.iter Console.Error.WriteLine

    failwith
        "Partial collection and option functions are prohibited. Use try-based APIs or exhaustive pattern matching."

let gatedRules =
    [| "WOOF-EARLY-RETURN"
       "WOOF-BLOCKING"
       "WOOF-MISSING-CT"
       "WOOF-REFEQUALS"
       "WOOF-STREAM-READ"
       "WOOF-SUPPRESS-THROWING-GENERIC"
       "WOOF-TCS-ASYNC"
       "WOOF-THROWING-DISPOSE"
       "WOOF-VALUETASK-AWAIT"
       "WOOF-RETURN-BANG-ONLY"
       "GRA-DISPBEFOREASYNC-001"
       "GRA-IMMUTABLECOLLECTIONEQUALITY-001"
       "GRA-INTERPOLATED-001"
       "GRA-JSONOPTS-001"
       "GRA-LOGARGFUNCFULLAPP-001"
       "GRA-LOGTEMPLMISSVALS-001"
       "GRA-STRING-001"
       "GRA-STRING-002"
       "GRA-STRING-003"
       "GRA-STRING-004"
       "GRA-TYPE-ANNOTATE-001"
       "GRA-UNIONCASE-001"
       "GRA-VIRTUALCALL-001"
       "IONIDE-001"
       "IONIDE-002"
       "IONIDE-003"
       "IONIDE-004"
       "IONIDE-005"
       "IONIDE-006"
       "IONIDE-007"
       "IONIDE-008"
       "IONIDE-009"
       "IONIDE-010"
       "IONIDE-011"
       // IONIDE-012 changes discriminated-union runtime representation and remains advisory.
       "IONIDE-013"
       "IONIDE-014" |]

printfn "Running F# analysis and the repository correctness gate across %d tracked source files..." allSources.Length

let analysisReport = Path.Combine(outputDirectory, "fsharp-analysis.sarif")

analyzerArguments allSources analysisReport
|> appendOption "--treat-as-error" gatedRules
|> runLogged "fsharp-analysis.log" "dotnet"
|> failIfNonZero "F# analyzer correctness gate"

printSarifSummary analysisReport

printfn "Running curated FSharpLint correctness checks..."

[ "tool"
  "run"
  "dotnet-fsharplint"
  "--"
  "lint"
  solutionPath
  "--lint-config"
  Path.Combine(scriptDirectory, "fsharplint.json") ]
|> runLogged "fsharplint.log" "dotnet"
|> failIfNonZero "FSharpLint correctness gate"

printfn "F# analysis passed. Reports are available in %s." outputDirectory
