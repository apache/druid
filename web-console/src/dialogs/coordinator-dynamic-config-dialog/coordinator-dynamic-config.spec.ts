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

import { cloneCountSummary, serverCountSummary } from './coordinator-dynamic-config';

describe('serverCountSummary', () => {
  it('returns None for undefined', () => {
    expect(serverCountSummary(undefined)).toBe('None');
  });

  it('returns None for empty array', () => {
    expect(serverCountSummary([])).toBe('None');
  });

  it('returns None for non-array', () => {
    expect(serverCountSummary('not an array')).toBe('None');
  });

  it('returns singular for one server', () => {
    expect(serverCountSummary(['server1'])).toBe('1 server');
  });

  it('returns plural for multiple servers', () => {
    expect(serverCountSummary(['server1', 'server2', 'server3'])).toBe('3 servers');
  });
});

describe('cloneCountSummary', () => {
  it('returns None for undefined', () => {
    expect(cloneCountSummary(undefined)).toBe('None');
  });

  it('returns None for non-object', () => {
    expect(cloneCountSummary('not an object')).toBe('None');
  });

  it('returns None for empty object', () => {
    expect(cloneCountSummary({})).toBe('None');
  });

  it('returns singular for one mapping', () => {
    expect(cloneCountSummary({ target1: 'source1' })).toBe('1 mapping');
  });

  it('returns plural for multiple mappings', () => {
    expect(cloneCountSummary({ target1: 'source1', target2: 'source2' })).toBe('2 mappings');
  });
});
