// Entry point for the github-core-clone-01 forge.
//
// Lifecycle (see the kit's RULES.txt):
//   1. Read the suite's base URL from MAKEFUN_ENV_URL.
//   2. Start the forge on a kernel-chosen port.
//   3. Tell the suite which port we bound, retrying until it accepts.
//   4. Keep the forge up while polling the suite for "done".
//   5. Once the suite reports done, shut down and exit 0.
//
// Anything fatal along the way is reported on stderr and exits 1.

import { createApp } from './app.ts';

// How long a single HTTP call to the suite may take before it is abandoned.
const CALL_BUDGET_MS = 30_000;

// Backoff for retries: first pause, growth factor, and ceiling.
const FIRST_PAUSE_MS = 250;
const PAUSE_GROWTH = 2;
const PAUSE_CEILING_MS = 2000;

// HTTP statuses from /register that mean the suite accepted us.
const REGISTER_OK = new Set([200, 409]);

// HTTP statuses from /register that mean we will never be accepted.
const REGISTER_FATAL = new Set([400, 405]);

function die(message: string): never {
  console.error(`main: ${message}`);
  process.exit(1);
}

// Wait `ms` milliseconds without blocking the event loop.
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// A retry loop's pause state: starts short, doubles, never exceeds the cap.
function makePauseSchedule(): { next: () => Promise<void> } {
  let current = FIRST_PAUSE_MS;
  return {
    async next() {
      await sleep(current);
      current = Math.min(current * PAUSE_GROWTH, PAUSE_CEILING_MS);
    },
  };
}

// The suite URL from the environment, with any trailing slashes removed so
// that joining "/register" or "/done" never produces a double slash.
function suiteBaseUrl(): string {
  const raw = process.env.MAKEFUN_ENV_URL ?? '';
  return raw.replace(/\/+$/, '');
}

// The running forge, kept so the main flow can close it at the end.
let forgeServer: ReturnType<typeof createApp> | null = null;

// Start the forge and return the port the kernel assigned to it.
// The promise rejects if the socket cannot be bound at all.
function bindForge(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createApp();
    server.once('error', (err: Error) => {
      reject(new Error(`could not bind forge: ${err.message}`));
    });
    server.listen(0, () => {
      const address = server.address();
      if (address === null || typeof address === 'string') {
        reject(new Error('forge bound but has no port'));
        return;
      }
      resolve(address.port);
    });
    // Keep a handle so the process can close it once the suite is done.
    forgeServer = server;
  });
}

// POST {"port": <port>} to the suite's /register until it answers 200 or 409.
// A 400 or 405 is fatal; anything else (other status, network failure,
// timeout) is retried with backoff.
async function announcePort(origin: string, port: number): Promise<void> {
  const pauses = makePauseSchedule();
  for (;;) {
    let response: Response | null = null;
    try {
      response = await fetch(`${origin}/register`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ port }),
        signal: AbortSignal.timeout(CALL_BUDGET_MS),
      });
    } catch (err) {
      const reason = err instanceof Error ? err.message : String(err);
      console.error(`main: register attempt failed (${reason}); retrying`);
      await pauses.next();
      continue;
    }
    if (REGISTER_OK.has(response.status)) return;
    if (REGISTER_FATAL.has(response.status)) {
      die(`register rejected with status ${response.status}`);
    }
    console.error(`main: register answered ${response.status}; retrying`);
    await pauses.next();
  }
}

// GET the suite's /done until its JSON body is {done: true}.
// Any failure or non-done answer is retried with backoff.
async function awaitSuiteDone(origin: string): Promise<void> {
  const pauses = makePauseSchedule();
  for (;;) {
    let body: unknown = null;
    try {
      const response = await fetch(`${origin}/done`, {
        signal: AbortSignal.timeout(CALL_BUDGET_MS),
      });
      body = await response.json();
    } catch (err) {
      const reason = err instanceof Error ? err.message : String(err);
      console.error(`main: done poll failed (${reason}); retrying`);
      await pauses.next();
      continue;
    }
    if (
      body !== null &&
      typeof body === 'object' &&
      (body as { done?: unknown }).done === true
    ) {
      return;
    }
    await pauses.next();
  }
}

// Close the forge's listening socket. Never throws.
async function shutdownForge(): Promise<void> {
  if (forgeServer === null) return;
  await new Promise<void>((resolve) => {
    forgeServer!.close(() => resolve());
  });
  forgeServer = null;
}

const origin = suiteBaseUrl();
if (origin === '') {
  die('MAKEFUN_ENV_URL is not set');
}

let port: number;
try {
  port = await bindForge();
} catch (err) {
  const reason = err instanceof Error ? err.message : String(err);
  die(reason);
}

await announcePort(origin, port);
await awaitSuiteDone(origin);
await shutdownForge();
process.exit(0);
