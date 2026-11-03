import fs from "node:fs/promises";

const condition = process.argv[2];
if (!["A", "B", "C"].includes(condition)) {
  throw new Error("usage: node scripts/capture-browser-condition.mjs A|B|C");
}

const outputDirectory = new URL("../docs/assessment-3/raw/", import.meta.url);
await fs.mkdir(outputDirectory, { recursive: true });
const target = await fetch(
  `http://127.0.0.1:9222/json/new?http://127.0.0.1:5500/frontend/experiments/assessment3-runner.html?condition=${condition}`,
  { method: "PUT" },
).then((response) => response.json());

const socket = new WebSocket(target.webSocketDebuggerUrl);
let requestID = 0;
const results = [];
const rawLines = [];
const completion = Promise.withResolvers();

socket.addEventListener("open", () => {
  socket.send(JSON.stringify({ id: ++requestID, method: "Runtime.enable" }));
});

socket.addEventListener("message", (message) => {
  const payload = JSON.parse(message.data);
  if (payload.method === "Runtime.exceptionThrown") {
    completion.reject(
      new Error(payload.params.exceptionDetails.exception?.description ?? "browser exception"),
    );
    return;
  }
  if (payload.method !== "Runtime.consoleAPICalled") return;
  const values = payload.params.args.map((argument) => argument.value);
  const line = values.map(String).join(" ");
  rawLines.push(`${new Date().toISOString()} ${line}`);
  if (values[0] === "IMAGELAB_RUN_RESULT") {
    results.push(JSON.parse(values[1]));
  }
  if (values[0] === "IMAGELAB_CONDITION_COMPLETE") {
    completion.resolve();
  }
});

socket.addEventListener("error", (event) => completion.reject(event));
await Promise.race([
  completion.promise,
  new Promise((_, reject) =>
    setTimeout(() => reject(new Error("condition capture timed out")), 8 * 60 * 1000),
  ),
]);

await fs.writeFile(
  new URL(`client-condition-${condition}.log`, outputDirectory),
  `${rawLines.join("\n")}\n`,
);
await fs.writeFile(
  new URL(`condition-${condition}.json`, outputDirectory),
  `${JSON.stringify(results, null, 2)}\n`,
);
console.log(JSON.stringify({ condition, runs: results.length }));
socket.close();
