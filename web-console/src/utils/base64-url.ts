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

export function base64UrlEncode(input: string): string {
  return base64Encode(input) // Encode to base64
    .replace(/\+/g, '-') // Replace '+' with '-'
    .replace(/\//g, '_') // Replace '/' with '_'
    .replace(/=+$/, ''); // Remove any trailing '='
}

export function base64UrlDecode(input: string): string {
  return base64Decode(
    padEndWithEquals(
      input
        .replace(/-/g, '+') // Replace '-' with '+'
        .replace(/_/g, '/'), // Replace '_' with '/'
    ),
  );
}

function base64Encode(data: string): string {
  const bytes = new TextEncoder().encode(data);
  const binString = String.fromCodePoint(...bytes);
  return btoa(binString);
}

function base64Decode(base64: string): string {
  const binString = atob(base64);
  const bytes = Uint8Array.from(binString.split('').map(m => m.codePointAt(0)!));
  return new TextDecoder().decode(bytes);
}

function padEndWithEquals(input: string): string {
  return input.padEnd(input.length + ((4 - (input.length % 4)) % 4), '=');
}
