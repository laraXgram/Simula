# Optional Tauri Desktop Packaging

Desktop packaging is optional and isolated from the default web workflow.

## Prerequisites

- Rust toolchain
- Node.js 18+
- System dependencies required by Tauri for your OS

## Bootstrap

```bash
cd client
npm install
npm run tauri:init
```

## Development

```bash
npm run tauri:dev
```

## Build

```bash
npm run tauri:build
```

## Notes

- Tauri commands run through npx @tauri-apps/cli scripts in package.json.
- If Tauri is not needed, web and docker workflows remain unaffected.
