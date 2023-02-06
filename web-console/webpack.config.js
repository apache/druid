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
  const useBabel = process.env.babel || mode === 'production';
  console.log(`Webpack running in ${mode} mode. ${useBabel ? 'Will' : "Won't"} use babel.`);

  const plugins = [
    new webpack.DefinePlugin({
      'process.env': JSON.stringify({ NODE_ENV: mode }),
      'global': {},
      'NODE_ENV': JSON.stringify(mode),
    }),
  ];

  function babelTest(s) {
    // https://github.com/zloirock/core-js/issues/514
    if (s.includes('/node_modules/core-js/')) return false;
    return /\.m?js$/.test(s);
  }

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
      publicPath: '/public',
      index: './index.html',
      openPage: 'unified-console.html',
      host: '0.0.0.0',
      port: 18081,
      proxy: {
        '/status': proxyTarget,
        '/druid': proxyTarget,
        '/proxy': proxyTarget,
      },
      transportMode: 'ws',
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
          test: useBabel ? babelTest : /^xxx_nothing_will_match_$/,
          use: {
            loader: 'babel-loader',
          },
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
            { loader: 'sass-loader' }, // compiles Sass to CSS, using Node Sass by default
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
