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

const fs = require('fs-extra');

const readfile = '../docs/content/querying/sql.md';
const writefile = 'lib/sql-function-doc.ts';

const heading = `/*
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

// This file is auto generated and should not be modified

export interface FunctionDescription {
  syntax: string;
  description: string;
}

/* tslint:disable:quotemark */

export const SQLFunctionDoc: FunctionDescription[] = `;

const readDoc = async () => {
  try {
    const data = await fs.readFile(readfile, 'utf-8');
    const sections = data.split("##");

    let entries = [];
    sections.forEach((section) => {
      if (!/^#.*function/.test(section)) return;

      entries = entries.concat(
        section.split('\n').map(line => {
          if (line.startsWith('|`')) {
            const rawSyntax = line.match(/\|`(.*)`\|/);
            if (rawSyntax == null) return null;
            const syntax = rawSyntax[1]
              .replace(/\\/g,'')
              .replace(/&#124;/g,'|');

            // Must have an uppercase letter
            if (!/[A-Z]/.test(syntax)) return null;

            const rawDescription = line.match(/`\|(.*)\|/);
            if (rawDescription == null) return null;
            const description = rawDescription[1];

            return {
              syntax: syntax,
              description: description
            };
          }
        }).filter(Boolean)
      );
    });

    // Make sure there are at least 10 functions for sanity
    if (entries.length < 10) {
      throw new Error(`Did not find any entries did the structure of '${readfile}' change?`);
    }

    const content = heading + JSON.stringify(entries, null, 2) + ';\n';

    try {
      await fs.writeFile(writefile, content, 'utf-8');
    } catch (e) {
      console.log(`Error when writing to ${writefile}: `, e);
    }

  } catch (e) {
    console.log(`Error when reading ${readfile}: `, e);
  }
}

readDoc();
