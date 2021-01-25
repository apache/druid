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

export function escapeAsciiControlCharacters(str: string): string {
  return str.replace(
    /[\x00-\x1f]/g,
    c => `\\x${(256 + c.charCodeAt(0)).toString(16).substr(1, 2)}`,
  );
}

export function unescapeAsciiControlCharacters(str: string): string {
  return str.replace(/\\x[0-1][0-9a-f]/gi, c => String.fromCharCode(parseInt(c.substr(2, 2), 16)));
}
