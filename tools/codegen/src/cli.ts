import { buildBotApiSpec } from "./botapi";
import { buildTlSchemas, importTl } from "./tl";

const USAGE = `usage: bun run codegen <command> [options]

commands:
  botapi [--offline]     scrape the Bot API docs into spec/botapi/api.json
  tl                     parse spec/tl/*.tl into spec/tl/*.json
  tl:import <tdlib-dir>  vendor telegram_api.tl / mtproto_api.tl from a tdlib checkout, then parse
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
  case "tl:import": {
    const dir = args.find((arg) => !arg.startsWith("--"));
    if (!dir) {
      console.error(USAGE);
      process.exit(1);
    }
    const source = await importTl(dir);
    console.log(
      `imported layer ${source.layer} from tdlib ${source.tdlib_commit} (${source.tdlib_date})`,
    );
    await printTl();
    break;
  }
  case "tl":
    await printTl();
    break;
  default:
    console.error(USAGE);
    process.exit(command ? 1 : 0);
}

async function printTl() {
  for (const schema of Object.values(await buildTlSchemas())) {
    const layer = schema.layer ? ` (layer ${schema.layer})` : "";
    console.log(
      `${schema.file}${layer}: ${schema.constructors.length} constructors, ${schema.functions.length} functions`,
    );
  }
}
