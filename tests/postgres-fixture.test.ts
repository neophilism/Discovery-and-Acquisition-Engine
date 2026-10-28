import test from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
test("PostgreSQL fixture rejects absent approval, absent URL, remote and query targets before connection", () => {
  for (const [flag, url] of [
    ["", "postgresql://postgres:fixture@127.0.0.1/discovery_ci"],
    ["1", ""],
    ["1", "postgresql://postgres:fixture@example.invalid/discovery_ci"],
    [
      "1",
      "postgresql://postgres:fixture@127.0.0.1/discovery_ci?sslmode=disable",
    ],
  ]) {
    const result = spawnSync(
      process.execPath,
      ["integration/postgres-scheduler.test.ts"],
      {
        cwd: new URL("..", import.meta.url),
        env: {
          ...process.env,
          DAE_TEST_DISPOSABLE_POSTGRES: flag!,
          DAE_TEST_ADMIN_URL: url!,
        },
        encoding: "utf8",
        timeout: 10000,
      },
    );
    assert.equal(result.error, undefined);
    assert.notEqual(result.status, 0);
    assert.match(
      result.stdout + result.stderr,
      /Explicit disposable|Only the designated empty loopback/,
    );
  }
});
