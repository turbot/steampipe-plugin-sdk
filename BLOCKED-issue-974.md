## Kind: environment

# BLOCKED: issue #974 (add-to-project is red on develop)

## What I checked

- `.github/workflows/add-issues-to-pipeling-issue-tracker.yaml` in this repo is only a caller. It runs
  `turbot/steampipe-workflows/.github/workflows/assign-issue-to-pipeling-issue-tracker.yml@main` with
  `issue_number` and `repository`, and `secrets: inherit`. It passes no `fields` input and no project URL.
- The `fields: '{"Status": "Todo"}'` input, the project URL `https://github.com/orgs/turbot/projects/31` and the
  token (`secrets.GH_PROJECT_PAT`) all live in the called workflow in `turbot/steampipe-workflows`
  (last changed 2026-03-24, "Specify permissions in all workflows"). The log's `Uses:` line confirms it ran
  steampipe-workflows `main` at 5ed608b.
- So problem 1 (unexpected `fields` input) is real but only a warning, and is fixed in steampipe-workflows, not here.
- Problem 2 (`Could not resolve to a ProjectV2 with the number 31`) is the actual failure. It means project 31
  in org `turbot` is deleted, renumbered, or not visible to `GH_PROJECT_PAT` (expired or lacking `project`
  scope or org access). My own token also gets the same error from `gh project view 31 --owner turbot`, but that
  token may simply lack project access, so I cannot tell which of the three it is.

## Why I stopped

No change to this repo can make the check pass. The only edits possible here would be to delete the workflow
or point it at a different project, which is a product decision about where issues are tracked and is not
mine to make. Editing `turbot/steampipe-workflows` is outside this issue's repo.

## Next step for a person

1. Confirm in the turbot org whether the "pipeling issue tracker" project still exists and what its number is,
   and whether `GH_PROJECT_PAT` is valid with project access.
2. Fix `turbot/steampipe-workflows/.github/workflows/assign-issue-to-pipeling-issue-tracker.yml`: update the
   project URL or the secret, and drop the unsupported `fields` input (actions/add-to-project v1.0.2 does not
   take it; set Status with a project workflow or a separate step instead).
3. Every repo calling that workflow is red the same way, so the fix belongs in one place. If the tracker is
   retired, this repo's caller workflow should be deleted instead (should this issue be closed as won't fix here?).
