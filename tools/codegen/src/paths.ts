import { join } from "node:path";

export const ROOT = join(import.meta.dir, "..", "..", "..");

export const paths = {
  cache: join(ROOT, ".cache"),
  botapi: {
    html: join(ROOT, ".cache", "botapi.html"),
    spec: join(ROOT, "spec", "botapi", "api.json"),
    overrides: join(ROOT, "spec", "botapi", "overrides.json"),
  },
} as const;
