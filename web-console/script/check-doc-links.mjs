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
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const SRC_DIR = path.join(__dirname, '../src');
const DOCS_DIR = path.join(__dirname, '../../docs');

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
    const fullPath = match[1];
    // Remove anchor if present
    let pathWithoutAnchor = fullPath.split('#')[0];

    // Remove trailing slash if present
    pathWithoutAnchor = pathWithoutAnchor.replace(/\/$/, '');

    // Try path.md first, fallback to path/index.md
    let docFilePath = path.join(DOCS_DIR, `${pathWithoutAnchor}.md`);
    if (!fs.existsSync(docFilePath)) {
      docFilePath = path.join(DOCS_DIR, pathWithoutAnchor, 'index.md');
    }

    links.push({
      file: path.relative(path.join(__dirname, '..'), filePath),
      docPath: fullPath,
      docFilePath: docFilePath
    });
  }

  return links;
}

function checkDocFile(docFilePath) {
  return {
    docFilePath,
    exists: fs.existsSync(docFilePath)
  };
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

  console.log('Checking if doc files exist...\n');

  let hasErrors = false;
  for (const link of allLinks) {
    const result = checkDocFile(link.docFilePath);

    if (!result.exists) {
      hasErrors = true;
      console.log(`❌ BROKEN: ${link.file}`);
      console.log(`   Doc path: ${link.docPath}`);
      console.log(`   Expected file: ${path.relative(path.join(__dirname, '../..'), link.docFilePath)}\n`);
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
