# Maintainer guide

## Branches and merging

- `main` is the default branch, the PR target, and the only branch the release pipeline publishes from.
- `main` requires PRs and resolved review conversations, disallows force pushes and deletion, and applies protections to administrators. Code-owner approval is required, with no additional numeric reviewer quota.
- `main` requires the **FSharp.Actor PR Validation** check from the Azure Pipelines GitHub App (app ID `9426`) with an up-to-date branch.

Squash or merge focused contribution PRs as appropriate. Remove topic branches after verifying their work is merged and no open PR depends on them.

## Code ownership

[.github/CODEOWNERS](../.github/CODEOWNERS) assigns all files, including the ownership policy itself, to `@david-cyman-argyle`. `main` enables required code-owner reviews with the generic approval count set to zero. GitHub reads ownership from the PR's base branch, so ownership takes effect once this file is on `main`.

GitHub does not allow PR authors to approve their own PRs. `@david-cyman-argyle` has an explicit pull-request review bypass on `main` so he can merge his own PRs. This is a per-user exception, not a blanket admin exemption: required CI, an up-to-date branch, conversation resolution, and force-push/deletion protections still apply. Other contributors require code-owner approval. Keep using PRs for all changes.

## Access

Repository access is managed in GitHub Settings → Collaborators and teams. Public contributors use forks and need no write access. Use Triage for issue triage, Write for trusted code contributors, Maintain for routine repository administration, and Admin only for people responsible for permissions and security settings. Review access periodically and remove it when no longer needed.

GitHub Actions has read-only default token permissions and cannot approve PRs. Grant additional workflow permissions only where needed. Keep publishing credentials out of PR validation and source control.

## Azure PR validation

[FSharp.Actor PR Validation](https://dev.azure.com/ArgyleConceptsLLC/Argyle%20Converge/_build?definitionId=38) uses `azure-pipelines.yml` and the ArgyleConcepts GitHub App service connection in the **ArgyleConceptsLLC** Azure DevOps organization. Its default branch is `main`. The YAML validates PRs targeting `main`; it has no push or publishing trigger. It installs the SDK selected by `global.json`, restores the solution and local tools, builds in Release with warnings as errors, runs the F# analyzers, runs the tests with a minimum expected test count, checks formatting with Fantomas, and verifies the locally packed package.

Fork builds must use Microsoft-hosted agents, have no secrets, and have no full-access job token. Require a maintainer comment before building fork PRs. Inspect changes, including build scripts and YAML, before authorizing them with `/AzurePipelines run`. This comment must be posted by a maintainer with Write access or above.

Check both pipeline Triggers and organization/project Pipelines Settings if fork validation does not start: centralized settings can override the pipeline. Do not resolve a blocked fork build by granting secrets or broad repository access. Verify the first real external contribution end to end; a same-repository PR does not prove fork authorization works. See [Azure's GitHub integration documentation](https://learn.microsoft.com/en-us/azure/devops/pipelines/repos/github#contributions-from-forks).

When adding or removing tests, update `--minimum-expected-tests` in both `azure-pipelines.yml` and `azure-release.yml`.

## Security and dependency maintenance

Keep private vulnerability reporting, Dependabot alerts/security updates, secret scanning, and push protection enabled. Triage security reports through private advisories. Dependency update PRs target `main` and follow the same CI and review rules as other changes.

## Releasing

`ArgylePackageVersion` in [`Directory.Build.props`](../Directory.Build.props) is the package version.

1. Open a PR that changes `ArgylePackageVersion`, and run the validation commands in [CONTRIBUTING.md](../CONTRIBUTING.md), including `bash eng/verify-packages.sh`.
2. After it merges, run [FSharp.Actor Release](https://dev.azure.com/ArgyleConceptsLLC/Argyle%20Converge/_build?definitionId=39) manually on `main` with publishing disabled. Inspect the `.nupkg`, `.snupkg`, version, and source links in its retained `packages` artifact.
3. Run it again on `main` with **Publish verified packages to NuGet.org** enabled. The publish stage downloads the validated artifact without checking out or rebuilding source, and pushes the package and symbols with `--skip-duplicate` so a partial publish can be retried. Confirm the NuGet listing shows the new version and organization ownership.
4. Create a GitHub release tagged with the version at the packaged commit, mark prereleases as such, and attach the verified artifacts.

### Azure NuGet release setup

`azure-release.yml` has no push or PR triggers. Its default run validates and retains the package and symbols without publishing or loading the secret group. Publishing is compiled in only when `publishPackages=true`, and the stage refuses to run outside a manual run on `main`.

The release job shares the Argyle Concepts NuGet release resources with FSharp.MinimalApi in the **Argyle Converge** Azure DevOps project:

- The `fsharp-minimalapi-nuget` environment has a maintainer approval check. Authorize only the release pipelines that use it.
- The `fsharp-minimalapi-nuget-release` Library variable group holds `NUGET_API_KEY` as a secret. Authorize only the release pipelines; do not enable access for all pipelines.
- The NuGet API key must be scoped to the Argyle Concepts organization as Package Owner, with permission to push new packages and versions, and with a package glob that covers `ArgyleConcepts.FSharp.Actor`. Exclude unlisting permission and record its expiration for renewal.

Never place the NuGet key in chat, a YAML parameter, a source file, or the PR validation pipeline.
