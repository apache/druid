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
const postcssPresetEnv = require('postcss-preset-env');
const BundleAnalyzerPlugin = require('webpack-bundle-analyzer').BundleAnalyzerPlugin;

const ALWAYS_BABEL = false;

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
  console.log(`Webpack running in ${mode} mode`);
  return {
    mode: mode,
    devtool: 'hidden-source-map',
    entry: {
      'web-console': './src/entry.ts',
    },
    output: {
      path: path.resolve(__dirname, './public'),
      filename: `[name]-${version}.js`,
      chunkFilename: `[name]-${version}.js`,
      publicPath: '/public',
    },
    target: 'web',
    resolve: {
      extensions: ['.tsx', '.ts', '.html', '.js', '.json', '.scss', '.css'],
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
    },
    module: {
      rules: [
        {
          test: /\.tsx?$/,
          enforce: 'pre',
          use: [
            {
              loader: 'tslint-loader',
              options: {
                configFile: 'tslint.json',
                emitErrors: true,
                fix: false, // Set this to true to auto fix errors
              },
            },
          ],
        },
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
          test: ALWAYS_BABEL || mode === 'production' ? /\.m?js$/ : /^xxx$/,
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
                ident: 'postcss',
                plugins: () => [
                  postcssPresetEnv({
                    browsers: ['> 1%', 'last 3 versions', 'Firefox ESR', 'Opera 12.1'],
                  }),
                ],
              },
            },
            { loader: 'sass-loader' }, // compiles Sass to CSS, using Node Sass by default
          ],
        },
      ],
    },
    performance: {
      hints: false,
    },
    plugins: process.env.BUNDLE_ANALYZER_PLUGIN === 'TRUE' ? [new BundleAnalyzerPlugin()] : [],
  };
};
