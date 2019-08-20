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

const entries = fg.sync(['./build/ApacheDruid/docs/**/*.html'])

const issues = [];
entries.forEach((entry) => {
  const cnt = fs.readFileSync(entry, 'utf-8');
  const links = cnt.match(/href="([./][^"]+|)#[^"]+"/g);
  if (!links) return;

  links.forEach(link => {
    const match = link.match(/^href="([./][^"]+|)#([^"]+)"$/);
    if (!match) throw new Error(`something went wrong for: ${link}`);

    const url = match[1];
    const anchor = match[2];

    if (url) {
      const target = url.startsWith('/')
        ? './build/ApacheDruid/docs' + url
        : path.resolve(path.dirname(entry), url);


      let targetHtml;
      try {
        targetHtml = fs.readFileSync(target, 'utf-8');
      } catch {
        //issues.push(`Could not find '${url}' linked from '${entry}'`);
        return;
      }

      if (!targetHtml.includes(`name="${anchor}"`) && !targetHtml.includes(`id="${anchor}"`)) {
        issues.push(`Could not find anchor '${anchor}' in '${url}' linked from '${entry}'`)
      }
    } else {
      if (!cnt.includes(`name="${anchor}"`) && !cnt.includes(`id="${anchor}"`)) {
        issues.push(`Could not find self anchor '${anchor}' in '${entry}'`)
      }
    }
  });
});

if (issues.length) {
  issues.push(`There are ${issues.length} issues`);
  console.error(issues.join('\n'));
  process.exit(1);
}


