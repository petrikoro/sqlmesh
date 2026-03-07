# GitLab CI/CD Bot

The SQLMesh GitLab CI/CD bot provides merge request feedback for GitLab CI pipelines. It can:

* Run the SQLMesh linter for merge requests
* Run unit tests for merge requests
* Create or update a merge request environment
* Maintain four sticky SQLMesh merge request notes, one for each GitLab bot command

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

3. Create a CI/CD variable backed by a GitLab personal access token or project access token with the `api` scope.

4. Add a GitLab CI job that runs the bot for merge request pipelines:

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
The GitLab bot maintains up to four sticky SQLMesh merge request notes, one per command. Each pipeline updates the matching note in place instead of adding a new comment.

The notes are:

* `Run Linter`: tracks the `Linter` stage and includes captured warnings or failures
* `Run Tests`: tracks the `Unit Tests` stage and preserves the rendered SQLMesh test summary
* `Update MR Environment`: tracks the `MR Environment` stage and includes the MR environment summary, affected models, and loaded or missing intervals
* `Generate Prod Plan`: tracks the `Prod Plan Preview` stage and reuses the same markdown-rich diff and backfill preview shown by the GitHub bot for what would change in `prod`

The visible body of each note now follows the GitHub bot's shared-stage output as closely as possible: a GitHub-aligned title plus the same SQLMesh markdown summary content, while preserving GitLab terminology such as `MR` and `merge request`. GitLab-specific chrome like the visible bot header, merge-request metadata table, stage-status table, and explicit `Summary` / `Details` wrappers is no longer shown. Instead, SQLMesh stores the sticky-note state in hidden machine-readable metadata so notes can still be updated safely in place. The MR environment and prod plan notes continue to reuse the same underlying SQLMesh diff and backfill summaries as the GitHub integration, so the two bots stay aligned on the substantive preview information as well as the visible rendering style.

The simplest way to keep all four notes current is to run `sqlmesh_cicd gitlab ... run-all`. `run-all` just runs the same command behaviors in order: `run-linter`, `run-tests`, `update-mr-environment`, and `gen-prod-plan`. It does not create a separate overview note or pre-seed downstream note state.
Each split command updates only its matching note. `run-linter` updates only `Run Linter`, `run-tests` updates only `Run Tests`, `update-mr-environment` updates only `Update MR Environment`, and `gen-prod-plan` updates only `Generate Prod Plan`.
The one shared-state safeguard that remains is the MR environment apply itself: if a newer pipeline has already posted SQLMesh note activity for the merge request, an older `update-mr-environment` run skips applying stale state. If a newer `Update MR Environment` note already exists, that note remains unchanged.

If you split stages across jobs, keep all of those jobs under the same merge-request-scoped `resource_group` and run them in order. That serialization still matters so older pipelines do not overwrite newer versions of the same note, but earlier commands no longer queue, skip, or otherwise mutate downstream notes on behalf of later commands.

## GitLab.com and self-managed GitLab
The bot supports both GitLab.com and self-managed GitLab.

By default, SQLMesh derives the GitLab API and server URLs from GitLab CI environment variables such as `CI_API_V4_URL` and `CI_SERVER_URL`.

=== "YAML"

    ```yaml linenums="1"
    cicd_bot:
      type: gitlab
      api_v4_url: https://gitlab.example.com/api/v4
      server_url: https://gitlab.example.com
    ```

=== "Python"

    ```python linenums="1"
    from sqlmesh.core.config import Config
    from sqlmesh.integrations.gitlab.cicd.config import GitLabCICDBotConfig

    config = Config(
        cicd_bot=GitLabCICDBotConfig(
            api_v4_url="https://gitlab.example.com/api/v4",
            server_url="https://gitlab.example.com",
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
* `prod_branch_name`
* `forward_only_branch_suffix`
* `run_on_deploy_to_prod`

GitLab-specific options are:

* `api_v4_url`
* `server_url`

## Commands
Run `sqlmesh_cicd gitlab --help` to see the full command list. The primary commands are:

* `run-all`: run linting, unit tests, merge request environment update, and prod plan preview by invoking the same per-command behaviors in order
* `run-linter`: run the SQLMesh linter and update only the `Run Linter` note
* `run-tests`: run SQLMesh unit tests and update only the `Run Tests` note
* `update-mr-environment`: create or update the merge request environment and update only the `Update MR Environment` note
* `gen-prod-plan`: generate the production plan preview and update the `Generate Prod Plan` note

## Current limitations
The GitLab integration reuses the same SQLMesh planning and environment logic as the GitHub bot, but its v1 scope is intentionally narrower.

It does not currently:

* Deploy to production
* Trigger deploys from approvals or comments
* Auto-merge merge requests
* Publish GitLab pipeline status widgets or check-run equivalents

The SQLMesh merge request notes are the primary user interface for the GitLab bot.
