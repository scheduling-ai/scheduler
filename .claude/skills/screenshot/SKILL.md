---
name: screenshot
description: Take a screenshot of a local web page using Playwright to verify visual changes. Works with any local dev server and any frontend framework.
---

Take a screenshot of a local web page using headless Chromium via Playwright.

## Inputs

The user may provide:
- **URL** — page to screenshot (default: `http://localhost:8000`)
- **Interaction steps** — clicks, typing, or navigation before capturing
- **CSS selector** — screenshot a specific element instead of the full page
- **Viewport** — width x height (default: 1280x800)

## Steps

1. Determine the target URL. If the user doesn't specify one, check whether a dev server is already listening (e.g. `curl -s -o /dev/null http://localhost:8000`). If nothing is running, start the appropriate dev server for the project in the background and poll until it responds.

2. Write a Python script to `$TMPDIR/screenshot_task.py` that:
   - Uses `playwright.sync_api` to launch headless Chromium
   - Sets the viewport size
   - Navigates to the URL, waits for `networkidle`
   - Performs any requested interactions, using `wait_for_selector` between steps (avoid fixed sleeps)
   - Takes a screenshot to `$TMPDIR/screenshot.png` (full page, or scoped to an element via CSS selector)
   - Closes the browser

3. Run it: `uv run python $TMPDIR/screenshot_task.py`

4. Read `$TMPDIR/screenshot.png` with the Read tool to inspect the result.

## Mobile testing

When verifying responsive or mobile changes, always take screenshots at both desktop (1280x800) and mobile (390x844) viewports. Navigate to multiple views (e.g. home screen, main app view, different tabs) — don't just screenshot the landing page.

When reviewing mobile screenshots, check critically:
- Are cards/sections proportional to the viewport, or do they waste scroll depth?
- Is padding/spacing appropriate for the screen size?
- Are key actions visible without excessive scrolling?

"Content fits without clipping" is not the same as "usable on a phone."

## Notes

- Playwright must be installed: `uv run python -m playwright install chromium`
- Use `dangerouslyDisableSandbox: true` on the Bash call since the server binds a port
- Prefer `wait_for_selector` or `wait_for_load_state("networkidle")` over `time.sleep`
- Do not hard-code selectors, element IDs, or interaction sequences from past UI versions — read the current HTML/source to determine the correct selectors
