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

const replace = require('replace-in-file');

if (process.argv.length !== 4) {
  console.log('Usage: node fix-path.js latest 0.17.0');
  process.exit(1);
}

var urlVersion = process.argv[2];
var druidVersion = process.argv[3];

try {
  // Fix doc paths
  replace.sync({
    files: './build/ApacheDruid/docs/**/*.html',
    from: /"\/docs\//g,
    to: `"/docs/${urlVersion}/`,
  });

  // Interpolate {{DRUIDVERSION}}
  replace.sync({
    files: './build/ApacheDruid/docs/**/*.html',
    from: /\{\{DRUIDVERSION\}\}/g,
    to: druidVersion,
  });

  // Add canonical header
  replace.sync({
    files: './build/ApacheDruid/docs/**/*.html',
    from: /<meta name="generator" content="Docusaurus"\/>/g,
    to: (match, fullText, b, filename) => {
      const path = filename.replace('./build/ApacheDruid/docs/', '');
      return `<link rel="canonical" href="https://druid.apache.org/docs/${urlVersion}/${path}"/><meta name="generator" content="Docusaurus"/>`;
    },
  });

  // Add docusearch version meta
  // ref: https://community.algolia.com/docsearch/required-configuration.html#introduces-global-information-as-meta-tags
  replace.sync({
    files: './build/ApacheDruid/docs/**/*.html',
    from: /<meta name="docsearch:language"[^>]+\/>/g,
    to: (match, fullText) => {
      return match + `<meta name="docsearch:version" content="${druidVersion}" />`;
    },
  });
  replace.sync({
    files: './build/ApacheDruid/docs/**/*.html',
    from: /"version:druidVersion"/g,
    to: (match, fullText) => {
      return `"version:${druidVersion}"`;
    },
  });

  console.log('Fixed versions');

} catch (error) {
  console.error('Error occurred:', error);
  process.exit(1);
}
