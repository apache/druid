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
const readfile='../docs/content/querying/sql.md'
const writefile='lib/sql-function-doc.ts'

const license = '/*\n' +
  ' * Licensed to the Apache Software Foundation (ASF) under one\n' +
  ' * or more contributor license agreements.  See the NOTICE file\n' +
  ' * distributed with this work for additional information\n' +
  ' * regarding copyright ownership.  The ASF licenses this file\n' +
  ' * to you under the Apache License, Version 2.0 (the\n' +
  ' * "License"); you may not use this file except in compliance\n' +
  ' * with the License.  You may obtain a copy of the License at\n' +
  ' *\n' +
  ' *     http://www.apache.org/licenses/LICENSE-2.0\n' +
  ' *\n' +
  ' * Unless required by applicable law or agreed to in writing, software\n' +
  ' * distributed under the License is distributed on an "AS IS" BASIS,\n' +
  ' * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n' +
  ' * See the License for the specific language governing permissions and\n' +
  ' * limitations under the License.\n' +
  ' */\n\n';

const comment = '// This file is auto generated and should not be modified\n\n';

const interfaceStr = 'export interface FunctionDescription {\n' +
  '  syntax: string;\n' +
  '  description: string;\n' +
  '}\n\n';

const disableTSlint = '/* tslint:disable */\n\n';

const readDoc = async () => {
  try {

    let content = license + comment + interfaceStr + disableTSlint + "export const SQLFunctionDoc: FunctionDescription[] = [\n";

    const data = await fs.readFile(readfile, 'utf-8');
    const sections = data.toString().split("##");

    sections.forEach((section) => {

      if (/^#.*functions/.test(section)) {

        section.split('\n').forEach(line => {
          if (line.startsWith('|`')) {
            const rawSyntax = line.match(/\|`.*`\|/g);
            if (rawSyntax == null) return;
            const syntax = rawSyntax[0].slice(2, -2);

            const rawDescription = line.match(/`\|.*\|/g);
            if (rawDescription == null) return;
            const description = rawDescription[0].slice(2,-1);

            const json = {
              syntax: syntax,
              description: description
            }

            const prettyJson = JSON.stringify(json, null, 4)
              .replace('{', '  {')
              .replace('}', '  }')
              .replace(/\"([^(\")"]+)\":/g,"$1:");
            content += prettyJson + ',\n';
          }
        });

      }
    });

    content = content.slice(0, -2);
    content += '\n]\n';

    try {
      fs.writeFile(writefile, content, 'utf-8');
    } catch (e) {
      console.log(`Error when writing to ${writefile}: `, e);
    }

  } catch (e) {
    console.log(`Error when reading ${readfile}: `, e);
  }
}

readDoc();
