/**
 * EOA signer for @xmtp/node-sdk, implemented on @noble/curves + @noble/hashes
 * (no viem dependency).
 *
 * Matches Ethereum `personal_sign` semantics exactly:
 *   sig = secp256k1(keccak256("\x19Ethereum Signed Message:\n" + len + message))
 * returned as 65-byte {r, s, v} with v in {27, 28}.
 */
import { secp256k1 } from "@noble/curves/secp256k1.js";
import { keccak_256 } from "@noble/hashes/sha3.js";

const ETHEREUM = 0; // IdentifierKind.Ethereum in @xmtp/node-sdk

export function privateKeyToAddress(privBytes) {
  const pub = secp256k1.getPublicKey(privBytes, false); // uncompressed, 65 bytes
  const hash = keccak_256(pub.slice(1));
  return "0x" + Buffer.from(hash.slice(-20)).toString("hex");
}

function ethSignMessage(message, privBytes) {
  const m = Buffer.from(message, "utf8");
  const prefix = Buffer.from(`\x19Ethereum Signed Message:\n${m.length}`, "utf8");
  const hash = keccak_256(Buffer.concat([prefix, m]));
  // noble v2: 'recovered' format => [recovery: 1 byte, r: 32, s: 32].
  // prehash:false because we already hashed with keccak256 (noble defaults to sha256 prehash).
  const sig65 = secp256k1.sign(hash, privBytes, { format: "recovered", prehash: false });
  const recovery = sig65[0];
  if (recovery !== 0 && recovery !== 1) throw new Error("unexpected recovery bit");
  // Ethereum {r, s, v}, v = recovery + 27
  return Uint8Array.from([...sig65.subarray(1), recovery + 27]);
}

/**
 * @param {string} privHex 64 hex chars, no 0x prefix
 * @returns {{ signer: object, address: string }}
 */
export function createEoaSigner(privHex) {
  if (!/^[0-9a-fA-F]{64}$/.test(privHex)) {
    throw new Error("private key must be 64 hex chars (no 0x prefix)");
  }
  const privBytes = Uint8Array.from(Buffer.from(privHex, "hex"));
  const pubBytes = secp256k1.getPublicKey(privBytes, false);
  const address = privateKeyToAddress(privBytes);
  const identifier = { identifier: address, identifierKind: ETHEREUM };
  const signer = {
    type: "EOA",
    getIdentifier: () => identifier,
    signMessage: async (message) => ethSignMessage(message, privBytes),
  };
  return { signer, address };
}

export function isValidEthAddress(s) {
  return typeof s === "string" && /^0x[0-9a-fA-F]{40}$/.test(s);
}
