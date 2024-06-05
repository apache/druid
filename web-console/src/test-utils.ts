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

import * as http from 'http';
import * as JSONBig from 'json-bigint-native';

export function nodePostJson(urlString: string, payload: any) {
  return new Promise((resolve, reject) => {
    const urlObj = new URL(urlString);
    const data = JSONBig.stringify(payload);

    const options = {
      hostname: urlObj.hostname,
      port: urlObj.port || 80,
      path: urlObj.pathname + urlObj.search,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': data.length,
      },
    };

    const req = http.request(options, res => {
      let responseBody = '';

      res.on('data', chunk => {
        responseBody += chunk;
      });

      res.on('end', () => {
        if (Number(res.statusCode) >= 200 && Number(res.statusCode) < 300) {
          try {
            const jsonResponse = JSONBig.parse(responseBody);
            resolve(jsonResponse);
          } catch (e) {
            reject(new Error('Invalid JSON response'));
          }
        } else {
          reject(new Error(`Request failed with status code ${res.statusCode}: ${responseBody}`));
        }
      });
    });

    req.on('error', e => {
      reject(e);
    });

    req.write(data);
    req.end();
  });
}
