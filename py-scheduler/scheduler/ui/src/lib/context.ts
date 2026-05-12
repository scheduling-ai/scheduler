// Each top-level app instantiates exactly one state class and pushes
// it into Svelte context here.  Shared components read it back via
// `getPlayback()` (typed as the PlaybackCore base, so they can't
// accidentally reach into mode-specific fields).
//
// Apps that need their full state class — e.g. the replay header
// wants `replayRunSolver` from ReplayState — should hold the
// reference returned by their own `set*State()` call rather than
// re-fetching from context, which avoids the awkward downcast.

import { getContext, setContext } from "svelte";
import type { PlaybackCore } from "./playback.svelte";
import type { LiveState } from "./live.svelte";
import type { ReplayState } from "./replay.svelte";

const PLAYBACK_KEY = Symbol("playback");

export function setPlaybackContext<T extends PlaybackCore>(state: T): T {
  setContext(PLAYBACK_KEY, state);
  return state;
}

export function getPlayback(): PlaybackCore {
  const ctx = getContext<PlaybackCore | undefined>(PLAYBACK_KEY);
  if (!ctx) {
    throw new Error(
      "getPlayback() called outside a state provider — the top-level app must call setPlaybackContext(state) before mounting components.",
    );
  }
  return ctx;
}

/** Narrow accessor for live-mode pages.  Throws if the mounted state
 *  isn't actually a LiveState — catches mount-order bugs early. */
export function getLive(): LiveState {
  const state = getPlayback();
  if (!("liveSource" in state)) {
    throw new Error("getLive() called from a non-live app shell");
  }
  return state as LiveState;
}

/** Narrow accessor for replay pages. */
export function getReplay(): ReplayState {
  const state = getPlayback();
  if (!("replayRunSolver" in state)) {
    throw new Error("getReplay() called from a non-replay app shell");
  }
  return state as ReplayState;
}
