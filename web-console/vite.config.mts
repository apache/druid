/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { readFileSync } from 'fs';
import postcssPresetEnv from 'postcss-preset-env';
import * as sass from 'sass';
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import cssInjectedByJsPlugin from 'vite-plugin-css-injected-by-js';

const version = JSON.parse(readFileSync('./package.json', 'utf-8')).version;

export default defineConfig(({ mode }) => {
  let druidUrl = process.env.druid_host || 'localhost';
  if (!druidUrl.startsWith('http')) {
    druidUrl = (druidUrl.endsWith(':9088') ? 'https://' : 'http://') + druidUrl;
  }
  if (!/:\d+$/.test(druidUrl)) {
    druidUrl += druidUrl.startsWith('https://') ? ':9088' : ':8888';
  }

  console.log(`Vite running in ${mode} mode.`);

  return {
    publicDir: false,

    build: {
      outDir: 'public',
      emptyOutDir: true,
      sourcemap: mode === 'development' ? 'inline' : false,
      target: 'es2016',
      cssCodeSplit: false,
      chunkSizeWarningLimit: Infinity,
      rollupOptions: {
        input: 'src/entry.tsx',
        output: {
          entryFileNames: `web-console-${version}.js`,
          // @ts-expect-error -- Rolldown option, not in Rollup types
          codeSplitting: false,
          assetFileNames: (assetInfo) => {
            if (/\.(woff2?|ttf|eot)$/.test(assetInfo.name || '')) {
              return 'fonts/[name][extname]';
            }
            return '[name]-[hash][extname]';
          },
        },
      },
    },

    resolve: {
      alias: {
        // ./node_modules/@blueprintjs/core/src/common/_mixins.scss imports color definitions
        // from the "lib" folder in @blueprintjs/colors but we need to import it from the "src"
        // folder. The "src" version includes "!default" in variable definitions, which allows
        // us to override color variables, but the "lib" version does not.
        '@blueprintjs/colors/lib/scss/colors': '@blueprintjs/colors/src/_colors',
      },
      extensions: ['.tsx', '.ts', '.js', '.scss', '.css'],
    },

    define: {
      global: '({})',
    },

    css: {
      preprocessorOptions: {
        scss: {
          // Blueprint's usage of SCSS is dependent on 'node-sass', but we use Dart Sass for
          // broader compatibility across CPU architectures. Blueprint's build process substitutes
          // these 'svg-icon' functions with actual icons but we don't have access to them at this
          // point. None of the components that use svg icons via CSS are themselves being used by
          // the web console, so we can safely omit the icons.
          //
          // TODO: Re-evaluate after upgrading to Blueprint v6
          silenceDeprecations: ['import', 'if-function', 'global-builtin'],
          functions: {
            'svg-icon($_icon, $_path)': () => new sass.SassString('transparent'),
          },
        },
      },
      postcss: {
        plugins: [
          postcssPresetEnv({
            autoprefixer: { grid: 'no-autoplace' },
          }),
        ],
      },
    },

    server: {
      host: '0.0.0.0',
      port: 18081,
      open: '/index.html',
      proxy: {
        '/status': { target: druidUrl, secure: false },
        '/druid': { target: druidUrl, secure: false },
        '/proxy': { target: druidUrl, secure: false },
      },
    },

    plugins: [
      react(),
      cssInjectedByJsPlugin(),
    ],

  };
});
