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

const path = require('path');
const fg = require('fast-glob');
const fs = require('fs-extra');

const entries = fg.sync(['./build/docs/**/*.html']);

function hasAnchor(html, anchor) {
  anchor = anchor.replace('#', '');
  return html.includes(`name="${anchor}"`) || html.includes(`id="${anchor}"`);
}

const issues = [];
entries.forEach((entry) => {
  const cnt = fs.readFileSync(entry, 'utf-8');
  const links = cnt.match(/href="([^"#]+)?(#[^"]+)?"/g);
  if (!links) return;

  links.forEach(link => {
    if (link === `href=""`) return;
    const match = link.match(/^href="([^"#]+)?(#[^"]+)?"$/);
    if (!match) throw new Error(`something went wrong for: ${link}`);

    const url = match[1];
    const anchor = match[2];

    if (url) {
      // Ignore external links
      if (url.includes('://')) return;

      // Ignore external doc links
      if (url.startsWith('/') && !url.startsWith('/docs/')) return;

      // Ignore mailto links
      if (url.startsWith('mailto:')) return;

      // This one will get created externally
      if (url === '/docs/latest') return;

      let target = url.startsWith('/')
        ? './build' + url
        : path.resolve(path.dirname(entry), url);

      if (target.endsWith('/')) {
        target += 'index.html';
      } else {
        target += '/index.html';
      }


      let targetHtml;
      try {
        targetHtml = fs.readFileSync(target, 'utf-8');
      } catch (e) {
        issues.push(`Could not find '${url}' linked from '${entry}' [${target}]`);
        return;
      }

      if (anchor && !hasAnchor(targetHtml, anchor)) {
        issues.push(`Could not find anchor '${anchor}' in '${url}' linked from '${entry}'`)
      }
    } else {
      if (anchor) {
        if (!hasAnchor(cnt, anchor)) {
          issues.push(`Could not find self anchor '${anchor}' in '${entry}'`)
        }
      } else {
        throw new Error(`should not get here with: ${link} in '${entry}'`);
      }
    }
  });
});

if (issues.length) {
  issues.push(`There are ${issues.length} issues`);
  console.error(issues.join('\n'));
  process.exit(1);
} else {
  console.log('No link-lint issues found');
}
