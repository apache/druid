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

export function escapeControlCharacters(str: string): string {
  if (typeof str !== 'string') throw new TypeError(`must be a string`);

  const n = str.length;
  let trailingSlash = false;
  if ((n - str.replace(/\\+$/, '').length) % 2 === 1) {
    // String ends with an odd number of '\' on the end
    trailingSlash = true;
    str = str.substr(0, str.length - 1);
  }

  const json = JSON.stringify(str);
  let escaped = json.substr(1, json.length - 2);
  if (trailingSlash) escaped += '\\';

  return escaped;
}

export function unescapeControlCharacters(str: string): string {
  // Replace every " (and \") with \" so that parsing `"hi" there` would not fail as a string
  str = str.replace(/\\?"/g, '\\"');

  // Replace \ at the end with \\
  str = str.replace(/\\$/, '\\\\');

  return JSON.parse('"' + str + '"');
}
