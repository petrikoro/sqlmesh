# GitLab CI/CD Bot

The SQLMesh GitLab CI/CD bot provides merge request feedback for GitLab CI pipelines. It can:

* Run the SQLMesh linter for merge requests
* Run unit tests for merge requests
* Create or update a merge request environment
* Maintain a single sticky merge request note with pipeline status, affected models, and a production plan preview

Unlike the [GitHub Actions bot](github.md), the GitLab integration is currently focused on merge request feedback. It does not yet support deploy commands, approval-triggered deploys, auto-merging merge requests, or GitLab status/check-run equivalents.

## Initial setup
1. Install SQLMesh. The optional GitLab extra is available as `pip install "sqlmesh[gitlab]"`.
2. Configure the bot in your SQLMesh project:

=== "YAML"

    ```yaml linenums="1"
    cicd_bot:
      type: gitlab
    ```

=== "Python"

    ```python linenums="1"
    from sqlmesh.core.config import Config
    from sqlmesh.integrations.gitlab.cicd.config import GitLabCICDBotConfig

    config = Config(
        cicd_bot=GitLabCICDBotConfig(),
    )
    ```

3. Add a GitLab CI job that runs the bot for merge request pipelines:

```yaml
sqlmesh:
  image: python:3.11
  stage: test
  resource_group: "sqlmesh-$CI_MERGE_REQUEST_IID"
  rules:
    - if: $CI_PIPELINE_SOURCE == "merge_request_event"
  variables:
    GITLAB_TOKEN: $SQLMESH_GITLAB_TOKEN
  script:
    - pip install -r requirements.txt
    - sqlmesh_cicd -p "$CI_PROJECT_DIR" gitlab --token "$GITLAB_TOKEN" run-all
```

The bot uses GitLab CI merge request variables to resolve the current project, merge request, source branch, and target branch.
Use a merge-request-scoped `resource_group` to serialize note updates when multiple pipelines, reruns, or split stage jobs target the same merge request.

## Merge request note workflow
The GitLab bot centers around one sticky merge request note. Every pipeline run updates the same note in place instead of creating a new comment each time.

The note tracks these stages:

* `Linter`
* `Unit Tests`
* `MR Environment`
* `Prod Plan Preview`

The simplest way to keep all four stages current is to run `sqlmesh_cicd gitlab ... run-all`. If you prefer separate CI jobs, run the stage commands individually in order: `run-linter`, `run-tests`, `update-mr-environment`, and `gen-prod-plan`.

If you split stages across jobs, keep all of those jobs under the same merge-request-scoped `resource_group` and run them in order. Without both serialization and stage ordering, later jobs can overwrite the sticky note with stale stage state.

When the merge request environment is built successfully, the note also includes:

* Affected models grouped by `Added`, `Removed`, `Directly Modified`, `Indirectly Modified`, and `Metadata Updated`
* Loaded and missing intervals for incremental models
* A production plan preview rendered from the same SQLMesh planning logic used by the GitHub bot

## GitLab.com and self-managed GitLab
The bot supports both GitLab.com and self-managed GitLab.

By default, SQLMesh derives the GitLab API and server URLs from GitLab CI environment variables such as `CI_API_V4_URL` and `CI_SERVER_URL`. If your setup needs explicit overrides, you can configure them in the bot config.

=== "YAML"

    ```yaml linenums="1"
    cicd_bot:
      type: gitlab
      api_v4_url: https://gitlab.example.com/api/v4
      server_url: https://gitlab.example.com
      note_header: ":robot: **Data Bot** :robot:"
    ```

=== "Python"

    ```python linenums="1"
    from sqlmesh.core.config import Config
    from sqlmesh.integrations.gitlab.cicd.config import GitLabCICDBotConfig

    config = Config(
        cicd_bot=GitLabCICDBotConfig(
            api_v4_url="https://gitlab.example.com/api/v4",
            server_url="https://gitlab.example.com",
            note_header=":robot: **Data Bot** :robot:",
        ),
    )
    ```

## Bot configuration
The GitLab bot reuses the same planning-oriented configuration concepts as the GitHub bot. Common options include:

* `auto_categorize_changes`
* `default_pr_start`
* `skip_pr_backfill`
* `pr_include_unmodified`
* `pr_environment_name`
* `pr_min_intervals`
* `forward_only_branch_suffix`
* `run_on_deploy_to_prod`

GitLab-specific options are:

* `api_v4_url`
* `server_url`
* `note_header`

## Commands
Run `sqlmesh_cicd gitlab --help` to see the full command list. The primary commands are:

* `run-all`: run linting, unit tests, merge request environment update, and prod plan preview while updating the sticky note
* `run-linter`: run the SQLMesh linter and update the sticky note's `Linter` stage
* `run-tests`: run SQLMesh unit tests
* `update-mr-environment`: create or update the merge request environment and update the `MR Environment` note section
* `gen-prod-plan`: generate the production plan preview and update the `Prod Plan Preview` note section

## Current limitations
The GitLab integration reuses the same SQLMesh planning and environment logic as the GitHub bot, but its v1 scope is intentionally narrower.

It does not currently:

* Deploy to production
* Trigger deploys from approvals or comments
* Auto-merge merge requests
* Publish GitLab pipeline status widgets or check-run equivalents

The merge request note is the primary user interface for the GitLab bot.
