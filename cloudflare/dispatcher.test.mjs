import assert from "node:assert/strict";
import dispatcher from "./dispatcher.mjs";

const originalFetch = globalThis.fetch;
try {
  let request;
  globalThis.fetch = async (...args) => {
    request = args;
    return new Response(null, { status: 204 });
  };
  await dispatcher.scheduled({}, { GITHUB_TOKEN: "test-token" });
  assert.match(request[0], /snapshot\.yml\/dispatches$/);
  assert.equal(request[1].method, "POST");
  assert.deepEqual(JSON.parse(request[1].body), { ref: "master" });
  assert.equal(request[1].headers.Authorization, "Bearer test-token");

  globalThis.fetch = async () => new Response(null, { status: 403 });
  await assert.rejects(() => dispatcher.scheduled({}, { GITHUB_TOKEN: "test-token" }), /403/);
} finally {
  globalThis.fetch = originalFetch;
}

console.log("dispatcher test: OK");
