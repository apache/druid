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

import type { DartQueryEntry } from './dart-query-entry';

export const DART_QUERIES: DartQueryEntry[] = [
  {
    sqlQueryId: '77b2344c-0a1f-4aa0-b127-de6fbc0c2b57',
    dartQueryId: '99cdba0d-ed77-433d-9adc-0562d816e105',
    sql: 'SELECT\n  "URL",\n  COUNT(*)\nFROM "c"\nGROUP BY 1\nORDER BY 2 DESC\nLIMIT 50\n',
    authenticator: 'allowAll',
    identity: 'allowAll',
    startTime: '2024-09-28T07:41:21.194Z',
    state: 'RUNNING',
  },
  {
    sqlQueryId: '45441cf5-d8b7-46cb-b6d8-682334f056ef',
    dartQueryId: '25af9bff-004d-494e-b562-2752dc3779c8',
    sql: 'SELECT\n  "URL",\n  COUNT(*)\nFROM "c"\nGROUP BY 1\nORDER BY 2 DESC\nLIMIT 50\n',
    authenticator: 'allowAll',
    identity: 'allowAll',
    startTime: '2024-09-28T07:41:22.854Z',
    state: 'CANCELED',
  },
  {
    sqlQueryId: 'f7257c78-6bbe-439d-99ba-f4998b300770',
    dartQueryId: 'f7c2d644-9c40-4d61-9fdb-7b0e15219886',
    sql: 'SELECT\n  "URL",\n  COUNT(*)\nFROM "c"\nGROUP BY 1\nORDER BY 2 DESC\nLIMIT 50\n',
    authenticator: 'allowAll',
    identity: 'allowAll',
    startTime: '2024-09-28T07:41:24.425Z',
    state: 'ACCEPTED',
  },
];
