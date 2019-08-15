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

if (process.argv.length !== 3) {
  console.error(`Incorrect number of arguments`);
  console.error(`Run as: node do-redirects.js <path-of-docs>`);
  process.exit(1);
}

const fs = require('fs-extra');
const path = require('path');

const dst = process.argv[2];
const strict = false;

function resolveTarget(source, target) {
  return path.resolve(path.dirname('/' + source), target).substr(1);
}

const redirects = fs.readFileSync('./redirects.json', 'utf-8')
  .trim()
  .split('\n')
  .map((line) => JSON.parse(line));

const issues = [];
const validRedirects = redirects.filter((redirect, i) => {
  const lineNumber = String(i + 1);
  let source = redirect.source;

  let valid = true;
  if (fs.pathExistsSync(dst + source)) {
    issues.push(`On line ${lineNumber} source ${source} already exists`);
    valid = false;
  }

  const cleanTarget = redirect.target.replace(/#.+$/, '');
  let resolvedTarget = resolveTarget(source, cleanTarget);
  if (!redirect.target.startsWith('/') && !fs.pathExistsSync(dst + resolvedTarget)) {
    issues.push(`On line ${lineNumber} target ${resolvedTarget} does not exist`);
    valid = false;
  }

  return valid
});

if (issues.length) {
  issues.push(`There are ${issues.length} issues with the redirects`);
  console.error(issues.join('\n'));
  if (strict) process.exit(1);
}

validRedirects.forEach((redirect) => {
  let source = redirect.source;
  let target = redirect.target;

  fs.ensureDirSync(path.dirname(dst + source));
  fs.writeFileSync(dst + source, `---
layout: redirect_page
redirect_target: ${target}
---`);
});
console.log(`Written ${validRedirects.length} redirects`);

