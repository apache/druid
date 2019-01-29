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

const { version } = require('./package.json');

module.exports = (env) => {
  const druidUrl = 'http://' + ((env || {}).druid_host || process.env.druid_host || 'localhost:8888');
  return {
    mode: process.env.NODE_ENV || 'development',
    entry: {
      'web-console': './src/entry.ts'
    },
    output: {
      path: path.resolve(__dirname, './public'),
      filename: `[name]-${version}.js`,
      chunkFilename: `[name]-${version}.js`,
      publicPath: '/public'
    },
    target: 'web',
    resolve: {
      extensions: ['.tsx', '.ts', '.html', '.js', '.json', '.scss', '.css']
    },
    devServer: {
      publicPath: '/public',
      index: './index.html',
      port: 18081,
      proxy: {
        '/status': druidUrl,
        '/druid': druidUrl
      }
    },
    module: {
      rules: [
        {
          test: /\.tsx?$/,
          use: 'ts-loader',
          exclude: /node_modules/
        },
        {
          test: /\.s?css$/,
          use: [
            {loader: 'style-loader'}, // creates style nodes from JS strings
            {loader: 'css-loader'}, // translates CSS into CommonJS
            {
              loader: 'postcss-loader',
              options: {
                ident: 'postcss',
                plugins: () => [
                  postcssPresetEnv({
                    browsers: ['> 1%', 'last 3 versions', 'Firefox ESR', 'Opera 12.1']
                  })
                ]
              }
            },
            {loader: 'sass-loader'} // compiles Sass to CSS, using Node Sass by default
          ]
        }
      ]
    }
  };
};
