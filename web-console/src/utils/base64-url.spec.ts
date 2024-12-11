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

import { base64UrlDecode, base64UrlEncode } from './base64-url';

describe('base64-url', () => {
  it('works in simple case', () => {
    const originalString = 'Hello, World!';
    const encoded = base64UrlEncode(originalString);
    expect(encoded).toEqual('SGVsbG8sIFdvcmxkIQ');

    const decoded = base64UrlDecode(encoded);
    expect(decoded).toEqual(originalString);
  });

  it('works for all ascii chars', () => {
    for (let c = 0; c < 256; c++) {
      const originalString = String.fromCharCode(c);
      const encoded = base64UrlEncode(originalString);
      const decoded = base64UrlDecode(encoded);
      expect(decoded).toEqual(originalString);
    }
  });
});
