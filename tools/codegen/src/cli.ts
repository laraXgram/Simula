import { buildBotApiSpec } from "./botapi";

const USAGE = `usage: bun run codegen <command> [--offline]

commands:
  botapi    scrape the Bot API docs into spec/botapi/api.json
`;

const [command, ...args] = process.argv.slice(2);
const offline = args.includes("--offline");

switch (command) {
  case "botapi": {
    const spec = await buildBotApiSpec({ offline });
    console.log(
      `Bot API ${spec.release.version} (${spec.release.date}): ${spec.methods.length} methods, ${spec.types.length} types`,
    );
    break;
  }
  default:
    console.error(USAGE);
    process.exit(command ? 1 : 0);
}
