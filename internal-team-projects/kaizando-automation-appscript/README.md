# kaizando-automation-appscript

Google Apps Script automation for the LUU **Kaizen** (continuous-improvement)
idea programme. Ideas arrive via a Form into a spreadsheet; this script
translates them, notifies management, and keeps contributors engaged — removing
the manual translation and daily spreadsheet-checking that used to gate the flow.

Three things run off the intake spreadsheet:

1. **Translation** — new entries (EN/PL) are normalised to German.
2. **Notification** — each new idea posts a Google Chat card (problem, proposed
   solution, and a link to the row) to the management channel.
3. **Gamification** — a monthly job emails contributors their reward-point
   balance, greeted in their language.

## Project layout

```
kaizando-automation-appscript/
├── scripts/
│   ├── translation.js     # LanguageApp translation of new entries → German
│   ├── notifications.js    # Chat Card V2 webhook on new submissions
│   └── gamification.js     # monthly HTML reward-point emails
└── images/                 # chat card + email previews
```

## How it works

- **Translation.** `translation.js` detects non-empty source cells and calls
  `LanguageApp.translate(text, "", "de")`, writing the German text into the
  master tab.
- **Notifications.** `notifications.js` walks new rows, builds a Chat
  `cardsV2` payload (decoratedText + button), posts it via `UrlFetchApp`, and
  marks the row `Sent` so it isn't re-notified. Triggered on edit/form submit.
- **Gamification.** `gamification.js` builds a per-recipient HTML email from a
  template and sends it via `GmailApp`, on a monthly time-based trigger.

## Setup

This is bound Apps Script, not a standalone repo — the code lives in the
container spreadsheet's script project. To configure:

1. **Webhook secret.** In *Project Settings → Script Properties*, add
   `CHAT_WEBHOOK_URL` with the Google Chat space's webhook URL.
   `notifications.js` reads it via `PropertiesService` — do **not** paste the URL
   into the source.
2. **Triggers.** Add an on-submit/on-edit trigger for the translation +
   notification flow, and a monthly time-based trigger for the email job.
3. **Sheet/tab names.** The scripts reference specific tab names
   (e.g. `Translated_Master_Response_sheet`); update them to match your sheet.

> The webhook URL embeds a key and a token — treat it as a credential. It belongs
> in Script Properties only.

## Notes

`UrlFetchApp` quota and `LanguageApp` limits apply; the notification loop sleeps
briefly between sends to stay within rate limits.
