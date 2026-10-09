/**
 * Module resolve hook: redirect `@xmtp/node-sdk` to the local stub so the
 * REAL service code (src/*) boots against a simulated network layer.
 * Used only by the sim smoke test — never in production.
 */
import { dirname } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const stubUrl = pathToFileURL(`${dirname(fileURLToPath(import.meta.url))}/stub-sdk.mjs`).href;

export async function resolve(specifier, context, next) {
  if (specifier === "@xmtp/node-sdk") {
    return { url: stubUrl, shortCircuit: true };
  }
  return next(specifier, context);
}
