/** Registers the sim loader hook. Boot the service with:
 *   node --import ./sim/register.mjs src/index.js
 */
import { register } from "node:module";

register("./loader.mjs", import.meta.url);
