# Contributing

Thank you for helping improve ArgyleConcepts.FSharp.Actor. Contributions to code, tests, examples, and documentation are welcome.

## Discuss a change

Search the [issues](https://github.com/ArgyleConcepts/FSharp.Actor/issues) and open pull requests before starting. For a bug, include a small reproduction, expected and actual behavior, the package version, and `dotnet --info`. For a larger feature or breaking API change, open an issue first to discuss the design. Questions can also be opened as issues. No internal ticket or company account is required.

Do not include credentials, personal data, or private application code. Follow [SECURITY.md](SECURITY.md) for vulnerabilities and our [Code of Conduct](CODE_OF_CONDUCT.md) in all project spaces.

## AI-assisted contributions

AI-assisted contributions are welcome. You are responsible for everything you submit, whether you wrote it yourself or used an AI tool.

Before submitting, read and understand the entire change, review it for correctness, and run the relevant checks. Be prepared to explain how it works, why the approach fits the project, and what the tests demonstrate. Verify generated claims, examples, and references, and make sure you have the right to contribute any included material under the project's license.

Only submit changes you understand and can support through review. You remain responsible for answering reviewer questions and addressing problems in your contribution; AI assistance does not transfer that responsibility to the tool or the maintainers.

## Set up locally

Install Git and the .NET 10 SDK selected by [global.json](global.json). Bash and Python 3 are needed for package verification; on Windows, use Git Bash or WSL for that script.

Fork the repository on GitHub, then clone your fork and branch from `main`:

```sh
git clone https://github.com/YOUR-USERNAME/FSharp.Actor.git
cd FSharp.Actor
git remote add upstream https://github.com/ArgyleConcepts/FSharp.Actor.git
git fetch upstream
git switch -c describe-your-change upstream/main
dotnet restore ArgyleConcepts.FSharp.Actor.slnx
dotnet tool restore
```

Use a short branch name describing the change. `main` is the default branch and PR target. Do not push changes directly to it.

## Validate your changes

Run the same checks as Azure PR validation from the repository root:

```sh
dotnet build ArgyleConcepts.FSharp.Actor.slnx --configuration Release --no-restore
dotnet fsi eng/fsharp-analysis/Run.fsx
dotnet test --solution ArgyleConcepts.FSharp.Actor.slnx --configuration Release --no-build --no-restore
dotnet fantomas check .
bash eng/verify-packages.sh
```

Run `dotnet fantomas .` to apply formatting. The tests use xUnit v3 with Microsoft Testing Platform; keep the `--solution` option when specifying the solution. `eng/verify-packages.sh` packs the library, checks the package metadata, symbols and SourceLink mappings against the current commit, and runs a fresh consumer against the packed package.

The repository contains the library (`src/ArgyleConcepts.FSharp.Actor`), its tests (`tests/ArgyleConcepts.FSharp.Actor.Tests`), and build tooling under `eng`.

For behavior changes, add a regression test that demonstrates the problem or new behavior. Concurrency tests must be deterministic: wait on explicit signals rather than fixed delays. Update the README for public API changes. Follow existing F# conventions and `.editorconfig`; keep unrelated refactoring out of a focused fix.

## Open a pull request

Push your branch to your fork and open a PR against **ArgyleConcepts/FSharp.Actor:main**. Describe the problem, the resulting behavior, and the validation performed. Link related public issues and call out breaking changes. Draft PRs are welcome for early feedback.

The branch must be up to date, the **FSharp.Actor PR Validation** check must pass, and review conversations must be resolved before merging. Approval from the designated code owner is required; there is no additional numeric reviewer quota. Ownership is defined in [.github/CODEOWNERS](.github/CODEOWNERS). Maintainers may request changes before merging.

Fork PRs may wait for a maintainer to inspect the changes and authorize Azure validation. Contributors do not need Azure credentials. If validation does not start, mention it in the PR; maintainers should follow the [maintainer guide](docs/MAINTAINING.md).

## License and releases

Contributions are made under the project's existing [MIT license](LICENSE). Package publishing is a separate maintainer decision; PR validation builds and tests packages locally and never publishes them.
