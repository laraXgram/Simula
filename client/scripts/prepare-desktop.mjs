import { execSync } from 'node:child_process';
import { chmodSync, copyFileSync, existsSync, mkdirSync, rmSync } from 'node:fs';
import { join } from 'node:path';

const clientDir = process.cwd();
const repoRoot = join(clientDir, '..');
const resourcesDir = join(clientDir, 'src-tauri', 'resources');

execSync('npm run build', { stdio: 'inherit' });

const binaryName = process.platform === 'win32' ? 'simula-api-server.exe' : 'simula-api-server';
const envBinarySource = process.env.SIMULA_API_BINARY_SOURCE;
let binarySource = envBinarySource || '';

if (!binarySource) {
  execSync('cargo build --manifest-path ../api-server/Cargo.toml --release', {
    stdio: 'inherit',
  });
  binarySource = join(repoRoot, 'api-server', 'target', 'release', binaryName);
}

if (!existsSync(binarySource)) {
  throw new Error(`API binary was not found at ${binarySource}`);
}

mkdirSync(resourcesDir, { recursive: true });
rmSync(join(resourcesDir, 'simula-api-server'), { force: true });
rmSync(join(resourcesDir, 'simula-api-server.exe'), { force: true });

const destination = join(resourcesDir, binaryName);
copyFileSync(binarySource, destination);

if (process.platform !== 'win32') {
  chmodSync(destination, 0o755);
}

console.log(`Prepared desktop resource: ${binaryName}`);
