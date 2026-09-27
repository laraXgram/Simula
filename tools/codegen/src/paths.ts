import { join } from "node:path";

export const ROOT = join(import.meta.dir, "..", "..", "..");

export const paths = {
  cache: join(ROOT, ".cache"),
  botapi: {
    html: join(ROOT, ".cache", "botapi.html"),
    spec: join(ROOT, "spec", "botapi", "api.json"),
    overrides: join(ROOT, "spec", "botapi", "overrides.json"),
    names: join(ROOT, "spec", "botapi", "names.json"),
  },
  tl: {
    dir: join(ROOT, "spec", "tl"),
    source: join(ROOT, "spec", "tl", "source.json"),
    files: ["telegram_api", "mtproto_api"] as const,
  },
} as const;
