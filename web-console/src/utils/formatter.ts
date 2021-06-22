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

export interface Formatter<T> {
  stringify: (thing: T) => string;
  parse: (str: string) => T;
}

const JSON_ESCAPES: Record<string, string> = {
  '"': '"',
  '\\': '\\',
  '/': '/',
  'b': '\b',
  'f': '\f',
  'n': '\n',
  'r': '\r',
  't': '\t',
};

// The stringifier is just JSON minus the double quotes, the parser is much more forgiving
export const JSON_STRING_FORMATTER: Formatter<string> = {
  stringify: (str: string) => {
    if (typeof str !== 'string') throw new TypeError(`must be a string`);

    const json = JSON.stringify(str);
    return json.substr(1, json.length - 2);
  },
  parse: (str: string) => {
    const n = str.length;
    let i = 0;
    let parsed = '';
    while (i < n) {
      const ch = str[i];
      if (ch === '\\') {
        const nextCh = str[i + 1];
        if (nextCh === 'u' && /^[0-9a-f]{4}$/i.test(str.substr(i + 2, 4))) {
          parsed += String.fromCharCode(parseInt(str.substr(i + 2, 4), 16));
          i += 6;
        } else if (JSON_ESCAPES[nextCh]) {
          parsed += JSON_ESCAPES[nextCh];
          i += 2;
        } else {
          parsed += ch;
          i++;
        }
      } else {
        parsed += ch;
        i++;
      }
    }
    return parsed;
  },
};
