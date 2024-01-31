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

const process = require('process');
const path = require('path');
const { SassString } = require('sass');
const BundleAnalyzerPlugin = require('webpack-bundle-analyzer').BundleAnalyzerPlugin;
const webpack = require('webpack');

const { version } = require('./package.json');

function friendlyErrorFormatter(e) {
  return `${e.severity}: ${e.content} [TS${e.code}]\n    at (${e.file}:${e.line}:${e.character})`;
}

module.exports = env => {
  let druidUrl = (env || {}).druid_host || process.env.druid_host || 'localhost';
  if (!druidUrl.startsWith('http')) druidUrl = 'http://' + druidUrl;
  if (!/:\d+$/.test(druidUrl)) druidUrl += ':8888';

  const proxyTarget = {
    target: druidUrl,
    secure: false,
  };

  const mode = process.env.NODE_ENV === 'production' ? 'production' : 'development';
  console.log(`Webpack running in ${mode} mode.`);

  const plugins = [
    new webpack.DefinePlugin({
      'process.env': JSON.stringify({ NODE_ENV: mode }),
      'global': {},
      'NODE_ENV': JSON.stringify(mode),
    }),
  ];

  return {
    mode: mode,
    devtool: mode === 'production' ? undefined : 'eval-cheap-module-source-map',
    entry: {
      'web-console': './src/entry.ts',
    },
    output: {
      path: path.resolve(__dirname, './public'),
      filename: `[name]-${version}.js`,
      chunkFilename: `[name]-${version}.js`,
      publicPath: 'public/',
    },
    target: 'web',
    resolve: {
      alias: {
        // ./node_modules/@blueprintjs/core/src/common/_mixins.scss imports color definitions
        // from the "lib" folder in @blueprintjs/colors but we need to import it from the "src"
        // folder. The "src" version includes "!default" in variable definitions, which allows
        // us to override color variables, but the "lib" version does not.
        //
        // Maps './node_modules/@blueprintjs/colors/lib/scss/colors.scss' to './node_modules/@blueprintjs/colors/src/_colors.scss'
        '@blueprintjs/colors/lib/scss/colors': '@blueprintjs/colors/src/_colors',
      },
      extensions: ['.tsx', '.ts', '.js', '.scss', '.css'],
      fallback: {
        os: false,
      },
    },
    devServer: {
      host: '0.0.0.0',
      port: 18081,
      hot: true,
      static: {
        directory: __dirname,
      },
      devMiddleware: {
        publicPath: '/public',
      },
      open: 'unified-console.html',
      proxy: {
        '/status': proxyTarget,
        '/druid': proxyTarget,
        '/proxy': proxyTarget,
      },
    },
    module: {
      rules: [
        {
          test: /\.tsx?$/,
          exclude: /node_modules/,
          use: [
            {
              loader: 'ts-loader',
              options: {
                errorFormatter: friendlyErrorFormatter,
              },
            },
          ],
        },
        {
          test: /\.s?css$/,
          use: [
            { loader: 'style-loader' }, // creates style nodes from JS strings
            { loader: 'css-loader' }, // translates CSS into CommonJS
            {
              loader: 'postcss-loader',
              options: {
                postcssOptions: {
                  plugins: {
                    'postcss-preset-env': {
                      autoprefixer: { grid: 'no-autoplace' },
                    },
                  },
                },
              },
            },
            {
              // compiles Sass to CSS, using Dart Sass by default
              loader: 'sass-loader',
              options: {
                sassOptions: {
                  functions: {
                    // Blueprint's usage of SCSS is dependent on 'node-sass', but we use Dart
                    // Sass for broader compatibility across CPU architectures. Blueprint's build
                    // process substitutes these 'svg-icon' functions with actual icons but we don't
                    // have access to them at this point. None of the components that use svg icons
                    // via CSS are themselves being used by the web console, so we can safely omit the icons.
                    //
                    // TODO: Re-evaluate after upgrading to Blueprint v5
                    'svg-icon($_icon, $_path)': () => new SassString('transparent'),
                  },
                },
              },
            },
          ],
        },
        {
          test: /\.(woff|woff2|ttf|eot)$/,
          use: {
            loader: 'file-loader',
            options: {
              name: '[name].[ext]',
            },
          },
        },
      ],
    },
    performance: {
      hints: false,
    },
    plugins:
      process.env.BUNDLE_ANALYZER_PLUGIN === 'TRUE'
        ? [...plugins, new BundleAnalyzerPlugin()]
        : plugins,
  };
};
