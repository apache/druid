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
  const base64 = btoa(input); // Encode to base64
  return base64
    .replace(/\+/g, '-') // Replace '+' with '-'
    .replace(/\//g, '_') // Replace '/' with '_'
    .replace(/=+$/, ''); // Remove any trailing '='
}

export function base64UrlDecode(input: string): string {
  const base64 = input
    .replace(/-/g, '+') // Replace '-' with '+'
    .replace(/_/g, '/'); // Replace '_' with '/'

  // Add padding '=' if necessary
  return atob(base64.padEnd(base64.length + ((4 - (base64.length % 4)) % 4), '=')); // Decode from base64
}
