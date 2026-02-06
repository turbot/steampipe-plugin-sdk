# Release PR conventions

When creating PRs for a release from a version branch (e.g. `v5.13.x`):

1. Commit message for changelog updates should be the release version number (e.g. `v5.13.2`)
2. Create PR against `develop` — title: `Merge branch '<branchname>' into develop`
3. Create PR against `main` — title: `Release steampipe-postgres-fdw v<version>`
