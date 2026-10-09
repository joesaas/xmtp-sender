/**
 * Simulated @xmtp/node-sdk for offline smoke tests.
 *
 * Implements ONLY the surface xmtp-sender uses (Client.create, inboxId,
 * installationIdBytes, isRegistered, conversations.create/fetchDm,
 * dm.sendText, revokeInstallations, close) with in-memory fakes — no network.
 * Every interaction is logged to stdout with a `[sim-sdk]` prefix so the
 * smoke transcript doubles as evidence.
 *
 * Failure injection: SIM_FAIL_CREATION=<n> makes the n-th Client.create
 * call (counting from 1, in call order) throw once — used to prove that one
 * client's failed registration neither blocks boot nor stays broken.
 */
import { createHash } from "node:crypto";

let createCount = 0;
let msgCount = 0;
const failCreation = Number(process.env.SIM_FAIL_CREATION ?? 0);

const bytes32 = (s) => new Uint8Array(createHash("sha256").update(s).digest());

function fakeDm(peerAddress) {
  return {
    id: `dm-${peerAddress.toLowerCase()}`,
    async sendText(text) {
      msgCount++;
      console.log(`[sim-sdk] sendText to=${peerAddress} len=${text.length} -> msg-${msgCount}`);
      return `msg-${msgCount}`;
    },
  };
}

export class Client {
  static async create(signer, options = {}) {
    createCount++;
    const n = createCount;
    const { identifier } = await signer.getIdentifier();
    if (n === failCreation) {
      console.log(`[sim-sdk] Client.create #${n} INJECTED FAILURE for ${identifier}`);
      throw new Error(`simulated registration failure (creation #${n})`);
    }
    const inboxId = `inbox-${identifier.slice(2, 12)}`;
    const installationIdBytes = bytes32(`${identifier}#${n}`);
    console.log(`[sim-sdk] Client.create #${n} ok address=${identifier} inbox=${inboxId} env=${options.env}`);
    return {
      inboxId,
      installationIdBytes,
      isRegistered: true,
      conversations: {
        async createDmWithIdentifier(id, _opts) {
          console.log(`[sim-sdk] createDmWithIdentifier ${id.identifier}`);
          return fakeDm(id.identifier);
        },
        async fetchDmByIdentifier(id) {
          console.log(`[sim-sdk] fetchDmByIdentifier ${id.identifier}`);
          return fakeDm(id.identifier);
        },
      },
      async revokeInstallations(ids) {
        const hexes = ids.map((b) => Buffer.from(b).toString("hex").slice(0, 12));
        console.log(`[sim-sdk] revokeInstallations [${hexes.join(", ")}] (inbox=${inboxId})`);
      },
      async close() {
        console.log(`[sim-sdk] client.close (inbox=${inboxId})`);
      },
    };
  }
}
