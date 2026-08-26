"""Send a pipeline daily-digest as a Slack Block Kit message.

Requires:
    SLACK_BOT_TOKEN  — xoxb-... token from the Slack app OAuth page
"""
from __future__ import annotations

import os
from typing import Any


def _stage_emoji(status: str) -> str:
    return "✅" if status == "OK" else "❌"


def _status_emoji(status: str) -> str:
    return "✅" if status == "SUCCESS" else "❌"


def _build_blocks(digest: dict) -> list[dict[str, Any]]:
    """Construct Slack Block Kit blocks from a digest dict."""
    date = digest["date"]
    status = digest["status"]
    duration = digest["duration_sec"]
    stages: list[dict] = digest.get("stages", [])
    quarantine: dict = digest.get("quarantine", {})
    llm_diagnosis: str | None = digest.get("llm_diagnosis")
    error: str | None = digest.get("error")

    blocks: list[dict[str, Any]] = []

    # ── Header ────────────────────────────────────────────────────────────────
    blocks.append({
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": f"{_status_emoji(status)}  NovaCart ETL — Daily Digest  {date}",
        },
    })
    blocks.append({"type": "divider"})

    # ── Run summary ───────────────────────────────────────────────────────────
    summary_lines = [
        f"*Status:* {_status_emoji(status)} {status}",
        f"*Duration:* {duration:.1f}s",
    ]
    if error:
        summary_lines.append(f"*Pipeline error:* `{error}`")

    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join(summary_lines)},
    })

    # ── Stage breakdown ───────────────────────────────────────────────────────
    if stages:
        stage_lines = ["*Stage breakdown:*"]
        for s in stages:
            icon = _stage_emoji(s["status"])
            line = f"{icon} `{s['stage']}` — {s['duration_sec']:.2f}s"
            if s.get("error"):
                line += f"  _(error: {s['error']})_"
            stage_lines.append(line)
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(stage_lines)},
        })
        blocks.append({"type": "divider"})

    # ── Quarantine section ────────────────────────────────────────────────────
    if quarantine:
        q_lines = ["*Quarantined rows:*"]
        for source, info in quarantine.items():
            q_lines.append(f"  • *{source}*: {info['count']} row(s)")
            for reason, count in info["reasons"].items():
                # Truncate very long validation messages
                short = reason[:120] + "…" if len(reason) > 120 else reason
                q_lines.append(f"    ‣ {count}×  _{short}_")

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(q_lines)},
        })

        # LLM diagnosis
        if llm_diagnosis:
            blocks.append({"type": "divider"})
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*🤖 AI Diagnosis:*\n{llm_diagnosis}",
                },
            })
    else:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "✅ No rows quarantined."},
        })

    blocks.append({"type": "divider"})
    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": "NovaCart ETL pipeline · automated digest"}],
    })

    return blocks


def send_digest(digest: dict, channel: str) -> None:
    """Post the digest to Slack. Raises RuntimeError if the post fails."""
    try:
        from slack_sdk import WebClient
        from slack_sdk.errors import SlackApiError
    except ImportError as exc:
        raise RuntimeError(
            "slack-sdk is not installed. Run: pip install slack-sdk"
        ) from exc

    token = os.environ.get("SLACK_BOT_TOKEN")
    if not token:
        raise RuntimeError(
            "SLACK_BOT_TOKEN environment variable is not set. "
            "See SLACK_SETUP.md for instructions."
        )

    client = WebClient(token=token)
    date = digest["date"]
    status = digest["status"]

    try:
        client.chat_postMessage(
            channel=channel,
            text=f"NovaCart ETL digest for {date}: {status}",  # fallback for notifications
            blocks=_build_blocks(digest),
        )
    except SlackApiError as exc:
        raise RuntimeError(
            f"Slack API error while posting digest for {date}: {exc.response['error']}"
        ) from exc
