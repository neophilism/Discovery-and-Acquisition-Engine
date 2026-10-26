import test from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:https";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
const execute = promisify(execFile);
/** A local ephemeral test certificate is trusted only by the child process. No verification bypass. */
test("native pinned HTTPS preserves hostname verification and enforces wire response limits", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "dae-tls-test-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const key = join(directory, "key.pem"),
    cert = join(directory, "cert.pem");
  await execute(
    "/usr/bin/openssl",
    [
      "req",
      "-x509",
      "-newkey",
      "ec",
      "-pkeyopt",
      "ec_paramgen_curve:P-256",
      "-nodes",
      "-keyout",
      key,
      "-out",
      cert,
      "-days",
      "2",
      "-subj",
      "/CN=archive.example",
      "-addext",
      "subjectAltName=DNS:archive.example",
    ],
    { timeout: 5000, maxBuffer: 65536 },
  );
  const requests: { host: string | undefined; path: string | undefined }[] = [];
  const server = createServer(
    { key: await readFile(key), cert: await readFile(cert) },
    (request, response) => {
      requests.push({ host: request.headers.host, path: request.url });
      response.setHeader("Content-Type", "text/plain");
      if (request.url === "/compressed") {
        response.setHeader("Content-Encoding", "gzip");
        response.end("encoded");
      } else if (request.url === "/large") response.end("too many bytes");
      else if (request.url === "/hang") {
        request.on("close", () => response.destroy());
      } else response.end("Fictional TLS source");
    },
  );
  server.on("tlsClientError", () => {});
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(
    () =>
      new Promise<void>((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
        server.closeAllConnections();
      }),
  );
  const port = (server.address() as { port: number }).port;
  const script = `import {pinnedHttpsTransport} from ${JSON.stringify(new URL("../src/index.ts", import.meta.url).href)};try{const r=await pinnedHttpsTransport(new URL(process.argv[1]),{address:'127.0.0.1',maximumBytes:Number(process.argv[2]),userAgent:'Fictional-TLS-test',signal:AbortSignal.timeout(Number(process.argv[3]))});process.stdout.write(JSON.stringify({status:r.status,text:Buffer.from(r.bytes).toString('utf8')}));}catch(e){process.stdout.write(JSON.stringify({code:e.code??null,message:e.message}));}`;
  async function request(
    host: string,
    path: string,
    maximumBytes = 1000,
    timeout = 2000,
  ) {
    const result = await execute(
      process.execPath,
      [
        "--input-type=module",
        "-e",
        script,
        `https://${host}:${port}${path}`,
        String(maximumBytes),
        String(timeout),
      ],
      {
        env: { NODE_EXTRA_CA_CERTS: cert, LANG: "C.UTF-8" },
        timeout: 4000,
        maxBuffer: 65536,
      },
    );
    return JSON.parse(result.stdout);
  }
  const valid = await request("archive.example", "/item");
  assert.deepEqual(valid, { status: 200, text: "Fictional TLS source" });
  assert.deepEqual(requests[0], {
    host: `archive.example:${port}`,
    path: "/item",
  });
  const count = requests.length,
    wrong = await request("other.example", "/item");
  assert.equal(wrong.code, "ERR_TLS_CERT_ALTNAME_INVALID");
  assert.equal(requests.length, count);
  assert.equal(
    (await request("archive.example", "/large", 4)).message,
    "capture_too_large",
  );
  assert.equal(
    (await request("archive.example", "/compressed")).message,
    "unsupported_encoding",
  );
  assert.equal(
    (await request("archive.example", "/hang", 1000, 100)).code,
    "ABORT_ERR",
  );
});
