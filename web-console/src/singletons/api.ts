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

import axios, { AxiosInstance, AxiosRequestConfig } from 'axios';
import * as JSONBig from 'json-bigint-native';

export class Api {
  static instance: AxiosInstance;

  static initialize(config?: AxiosRequestConfig): void {
    Api.instance = axios.create(config);
  }

  static getDefaultConfig(): AxiosRequestConfig {
    return {
      headers: {},

      transformResponse: [
        data => {
          if (typeof data === 'string') {
            try {
              data = JSONBig.parse(data);
            } catch (e) {
              /* Ignore */
            }
          }
          return data;
        },
      ],
    };
  }

  static encodePath(path: string): string {
    return path.replace(/[?#%;&'[\]\\]/g, c => '%' + c.charCodeAt(0).toString(16).toUpperCase());
  }

  static isNetworkError(e: Error): boolean {
    return e.message === 'Network Error' && !(e as any).response;
  }
}
