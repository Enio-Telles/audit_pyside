import { StitchProxy } from "@google/stitch-sdk";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";

const apiKey = process.env.STITCH_API_KEY;

if (!apiKey) {
  console.error("STITCH_API_KEY não definida");
  process.exit(1);
}

const proxy = new StitchProxy({ apiKey });
const transport = new StdioServerTransport();

await proxy.start(transport);
