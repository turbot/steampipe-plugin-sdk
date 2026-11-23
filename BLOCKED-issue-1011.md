# BLOCKED: issue #1011

## Kind: design

Everything the issue asks for in v6 is already on `develop` (bf923de, #1030, unreleased v6.3.0):

- All four settings (`HydrateConfig.MaxConcurrency`, `GetConfig.MaxConcurrency`, `Plugin.DefaultConcurrency`, `DefaultConcurrencyConfig`) carry a `Deprecated:` paragraph naming `MaxConcurrency` on a `rate_limiter.Definition`; a test guards the form.
- The doc lines and `MaxConcurrency: 50` examples are gone; the docs point to rate limiters.
- CHANGELOG v6.3.0 lists the four under Deprecations.

What is left is the v7 removal (the four settings and the `plugin/table.go` copy, plus a breaking-change CHANGELOG line). That breaks the build of any plugin that sets them, so it cannot go on the v6 `develop` line. No v7 branch exists on origin.

Next step: when v7 work starts, a person re-arms this issue on the v7 base so a run does the removal. Until then there is nothing to build, and closing the issue would drop the v7 half.
