#!/usr/bin/env node

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

/* eslint-disable no-undef */

import fs from 'fs';
import https from 'https';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const DOCS_BASE_URL = 'https://druid.apache.org/docs/latest';
const SRC_DIR = path.join(__dirname, '../src');

// Regex to find ${getLink('DOCS')}/path patterns
const DOC_LINK_REGEX = /\$\{getLink\(['"]DOCS['"]\)\}\/([^\s`'"}\]]+)/g;

function getAllTsFiles(dir) {
  const files = [];

  function traverse(currentPath) {
    const entries = fs.readdirSync(currentPath, { withFileTypes: true });

    for (const entry of entries) {
      const fullPath = path.join(currentPath, entry.name);

      if (entry.isDirectory()) {
        traverse(fullPath);
      } else if (entry.isFile() && (entry.name.endsWith('.ts') || entry.name.endsWith('.tsx'))) {
        files.push(fullPath);
      }
    }
  }

  traverse(dir);
  return files;
}

function extractDocLinks(filePath) {
  const content = fs.readFileSync(filePath, 'utf-8');
  const links = [];
  let match;

  // Reset regex state
  DOC_LINK_REGEX.lastIndex = 0;

  while ((match = DOC_LINK_REGEX.exec(content)) !== null) {
    links.push({
      file: path.relative(path.join(__dirname, '..'), filePath),
      path: match[1],
      fullUrl: `${DOCS_BASE_URL}/${match[1]}`
    });
  }

  return links;
}

function checkUrl(url) {
  return new Promise((resolve) => {
    https.get(url, { method: 'HEAD' }, (res) => {
      resolve({
        url,
        status: res.statusCode,
        exists: res.statusCode >= 200 && res.statusCode < 400
      });
    }).on('error', (err) => {
      resolve({
        url,
        status: `ERROR: ${err.message}`,
        exists: false
      });
    });
  });
}

async function main() {
  console.log('Searching for doc links in TypeScript files...\n');

  const tsFiles = getAllTsFiles(SRC_DIR);
  console.log(`Found ${tsFiles.length} TypeScript files\n`);

  const allLinks = [];
  for (const file of tsFiles) {
    const links = extractDocLinks(file);
    allLinks.push(...links);
  }

  console.log(`Found ${allLinks.length} doc links\n`);

  if (allLinks.length === 0) {
    console.log('No doc links found.');
    return;
  }

  console.log('Checking if links exist...\n');

  const results = await Promise.all(
    allLinks.map(link => checkUrl(link.fullUrl))
  );

  let hasErrors = false;
  for (let i = 0; i < allLinks.length; i++) {
    const link = allLinks[i];
    const result = results[i];

    if (!result.exists) {
      hasErrors = true;
      console.log(`❌ BROKEN: ${link.file}`);
      console.log(`   URL: ${link.fullUrl}`);
      console.log(`   Status: ${result.status}\n`);
    }
  }

  if (hasErrors) {
    console.log('⚠️  Some links are broken!');
    process.exit(1);
  } else {
    console.log('✅ All links are valid!');
    process.exit(0);
  }
}

main().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
