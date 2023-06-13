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

try {
  // Force upgrade to docusearch 2 as 1 appers to have stopped being supported
  replace.sync({
    files: './build/ApacheDruid/docs/**/*.html',
    from: 'https://cdn.jsdelivr.net/docsearch.js/1/docsearch.min.js',
    to: 'https://cdn.jsdelivr.net/npm/docsearch.js@2/dist/cdn/docsearch.min.js',
  });
  replace.sync({
    files: './build/ApacheDruid/docs/**/*.html',
    from: 'https://cdn.jsdelivr.net/docsearch.js/1/docsearch.min.css',
    to: 'https://cdn.jsdelivr.net/npm/docsearch.js@2/dist/cdn/docsearch.min.css',
  });
  replace.sync({
    files: './build/ApacheDruid/css/main.css',
    from: 'ul li a',
    to: 'ul li > a',
  });

  console.log(`Fixed docsearch`);

} catch (error) {
  console.error('Error occurred:', error);
  process.exit(1);
}
