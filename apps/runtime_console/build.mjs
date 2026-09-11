import {build} from 'esbuild';
import {mkdir,copyFile} from 'node:fs/promises';
await mkdir('dist/assets',{recursive:true});
await build({entryPoints:['src/main.tsx'],outdir:'dist/assets',bundle:true,minify:true,format:'esm',target:'es2022',
  define:{'process.env.NODE_ENV':'"production"'}});
await copyFile('index.html','dist/index.html');
