#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
package_dir="$(mktemp -d)"
trap 'rm -rf "$package_dir"' EXIT

dotnet pack "$repo_root/src/ArgyleConcepts.FSharp.Actor/ArgyleConcepts.FSharp.Actor.fsproj" --configuration Release --output "$package_dir"

python3 - "$repo_root" "$package_dir" <<'PY'
from pathlib import Path
import json
import subprocess
import sys
import xml.etree.ElementTree as ET
import zipfile

repo_root = Path(sys.argv[1])
package_dir = Path(sys.argv[2])
commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo_root, text=True).strip()
version = ET.parse(repo_root / "Directory.Build.props").findtext("./PropertyGroup/ArgylePackageVersion")
package_id = "ArgyleConcepts.FSharp.Actor"
target_frameworks = ["net8.0", "net10.0"]
repository_url = "https://github.com/ArgyleConcepts/FSharp.Actor"
packages = list(package_dir.glob("*.nupkg"))
assert len(packages) == 1, f"Expected one package, found: {packages}"
package = packages[0]

with zipfile.ZipFile(package) as archive:
    files = set(archive.namelist())
    nuspec_name = next(name for name in files if name.endswith(".nuspec"))
    root = ET.fromstring(archive.read(nuspec_name))
    ns = {"n": "http://schemas.microsoft.com/packaging/2013/05/nuspec.xsd"}
    metadata = root.find("n:metadata", ns)
    assert metadata.findtext("n:id", namespaces=ns) == package_id
    assert metadata.findtext("n:version", namespaces=ns) == version
    assert metadata.findtext("n:projectUrl", namespaces=ns) == repository_url
    repository = metadata.find("n:repository", ns).attrib
    assert repository["url"] == repository_url
    assert repository["type"] == "git"
    assert repository["commit"] == commit
    assert metadata.findtext("n:readme", namespaces=ns) == "README.md"
    assert metadata.findtext("n:license", namespaces=ns) == "MIT"
    assert {"LICENSE", "README.md"} <= files, files
    dependencies = {node.attrib["id"] for node in metadata.findall(".//n:dependency", ns)}
    assert dependencies == {"FSharp.Core"}, dependencies

    for tfm in target_frameworks:
        assert f"lib/{tfm}/{package_id}.dll" in files, files
        assert f"lib/{tfm}/{package_id}.xml" in files, files

with zipfile.ZipFile(package.with_suffix(".snupkg")) as archive:
    symbols = set(archive.namelist())

expected_url = f"https://raw.githubusercontent.com/ArgyleConcepts/FSharp.Actor/{commit}/*"

for tfm in target_frameworks:
    assert f"lib/{tfm}/{package_id}.pdb" in symbols, symbols
    source_link = repo_root / "src" / package_id / "obj/Release" / tfm / f"{package_id}.sourcelink.json"
    documents = json.loads(source_link.read_text())["documents"]
    assert documents, source_link
    assert all(url == expected_url for url in documents.values()), documents

print("Package contents, metadata, symbols and generated SourceLink mappings verified.")
PY

export NUGET_PACKAGES="$package_dir/cache"
export ArgylePackageVersion="$(dotnet msbuild "$repo_root/src/ArgyleConcepts.FSharp.Actor/ArgyleConcepts.FSharp.Actor.fsproj" -getProperty:ArgylePackageVersion)"
consumer_dir="$package_dir/consumer"
mkdir "$consumer_dir"
cp "$repo_root/eng/PackageSmoke/PackageSmoke.fsproj" "$repo_root/eng/PackageSmoke/Program.fs" "$consumer_dir/"
dotnet restore "$consumer_dir/PackageSmoke.fsproj" --source "$package_dir" --source https://api.nuget.org/v3/index.json
dotnet run --project "$consumer_dir/PackageSmoke.fsproj" --configuration Release --no-restore

# Retain only verified release artifacts when requested by the release pipeline.
if [[ -n "${VERIFY_PACKAGE_OUTPUT_DIRECTORY:-}" ]]; then
    mkdir -p "$VERIFY_PACKAGE_OUTPUT_DIRECTORY"
    cp "$package_dir"/*.nupkg "$package_dir"/*.snupkg "$VERIFY_PACKAGE_OUTPUT_DIRECTORY/"
fi
