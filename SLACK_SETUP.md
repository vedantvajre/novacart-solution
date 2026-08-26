# Slack App Setup Guide

Follow these steps once to create the bot that posts daily digests.

---

## 1 — Create a Slack App

1. Go to **https://api.slack.com/apps** and click **Create New App**.
2. Choose **From scratch**.
3. Give it a name (e.g. `NovaCart ETL`) and select your workspace.
4. Click **Create App**.

---

## 2 — Add Bot Token Scopes

1. In the left sidebar, go to **OAuth & Permissions**.
2. Scroll to **Scopes → Bot Token Scopes** and click **Add an OAuth Scope**.
3. Add the following scopes:
   - `chat:write` — post messages to channels
   - `chat:write.public` — post to public channels without being invited (optional; remove if you prefer to invite the bot manually)

---

## 3 — Install the App to Your Workspace

1. Still on **OAuth & Permissions**, scroll up and click **Install to Workspace**.
2. Review and click **Allow**.
3. Copy the **Bot User OAuth Token** (starts with `xoxb-`).
4. Save it as the `SLACK_BOT_TOKEN` environment variable (see `.env.example`).

---

## 4 — Find or Create the Target Channel

1. In Slack, open or create the channel where digests should be posted
   (e.g. `#novacart-pipeline`).
2. Right-click the channel name → **View channel details** → copy the
   **Channel ID** at the bottom (looks like `C0123ABCDEF`).
3. Set `slack_channel` in `config/pipeline.yaml` to that Channel ID.
   > Using the ID is more reliable than using the `#name`, which can break if
   > the channel is renamed.

4. If you did **not** add the `chat:write.public` scope, invite the bot to the
   channel by typing `/invite @NovaCart ETL` inside it.

---

## 5 — Set Environment Variables

```bash
export SLACK_BOT_TOKEN=xoxb-...
export WATSONX_API_KEY=...
export WATSONX_PROJECT_ID=...
```

Or copy `.env.example` to `.env`, fill in the values, and load it:

```bash
cp .env.example .env
# edit .env
source .env          # or use python-dotenv / direnv
```

---

## 6 — Configure the Pipeline

In `config/pipeline.yaml`, set:

```yaml
notify:
  slack_channel: "C0123ABCDEF"          # your channel ID
  watsonx_model_id: "ibm/granite-13b-instruct-v2"
  watsonx_project_id: ""                # leave blank — read from WATSONX_PROJECT_ID env var
```

---

## 7 — Test the Integration

Run the pipeline for any date with data:

```bash
python -m src.pipeline --date 2025-11-07
```

You should see a digest message appear in the configured Slack channel within a
few seconds of the run completing.

---

## What the Digest Contains

| Section | Always shown | Only when relevant |
|---------|-------------|-------------------|
| Run status (SUCCESS / FAIL) | ✅ | |
| Stage-by-stage breakdown | ✅ | |
| Quarantine row counts + reasons | | ✅ when rows quarantined |
| AI diagnosis (watsonx.ai) | | ✅ when rows quarantined |
| Pipeline error message | | ✅ on FAIL |

The digest is sent **once per processed date**. When `--backfill N` is used,
one message is posted per date.

---

## Troubleshooting

| Error | Likely cause | Fix |
|-------|-------------|-----|
| `not_in_channel` | Bot not invited | `/invite @NovaCart ETL` in the channel |
| `invalid_auth` | Wrong token | Check `SLACK_BOT_TOKEN` value |
| `channel_not_found` | Wrong channel ID | Use the Channel ID, not `#name` |
| `WATSONX_API_KEY not set` | Missing env var | Export the variable before running |
| Digest sent but no LLM text | `WATSONX_PROJECT_ID` empty | Set the env var or fill in `pipeline.yaml` |
