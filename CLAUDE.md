# Releases

The SDK releases through Shipyard. Nobody types a version, cuts a branch or makes a tag by hand.

1. **Every PR into `develop` carries a `Changelog:` line** in its body (the PR template has it). It is one or two accurate sentences a plugin author can read. A PR that changes only files the release does not ship (`notShipped` in `.github/release.json`, such as tests, `design/`, `docs/` and `.github/`) may leave the line out.
2. **Never edit `CHANGELOG.md` in a PR.** The release writes its section from the `Changelog:` lines of the PRs it ships. A PR that adds a section there makes the cut refuse the release.
3. **The weekly start** (`.github/workflows/release-start.yml`, Tuesday 09:00 UTC, or run by hand on `develop`) files a prep job when a shipped change is waiting. The cut, and the fleet's release-PR hook from its artifact, open a release PR into `main`; `Release PR` tests the SDK and builds a consumer against it and records SHIP or HOLD.
4. **The approval ask posts in #pipes.** Merging the approved release PR makes the tag (`ship.yml`), which is the publish: the Go proxy serves the version once the tag exists.
5. **Do not edit `.github/workflows/shipyard-pr.yml`.** It is the same in every repo.
