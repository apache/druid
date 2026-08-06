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

import { buildTieredServers } from './tiered-servers';

describe('buildTieredServers', () => {
  it('returns empty structure for empty input', () => {
    const result = buildTieredServers([]);
    expect(result).toEqual({
      tiers: [],
      serversByTier: {},
      serverToTier: {},
      allServers: [],
    });
  });

  it('sorts tiers alphabetically', () => {
    const result = buildTieredServers([
      { server: 'host1:8083', tier: '_default_tier' },
      { server: 'host2:8083', tier: 'hot' },
      { server: 'host3:8083', tier: 'cold' },
    ]);
    expect(result.tiers).toEqual(['_default_tier', 'cold', 'hot']);
  });

  it('sorts servers within each tier', () => {
    const result = buildTieredServers([
      { server: 'host-c:8083', tier: 'hot' },
      { server: 'host-a:8083', tier: 'hot' },
      { server: 'host-b:8083', tier: 'hot' },
    ]);
    expect(result.serversByTier['hot']).toEqual(['host-a:8083', 'host-b:8083', 'host-c:8083']);
  });

  it('builds serverToTier map correctly', () => {
    const result = buildTieredServers([
      { server: 'host1:8083', tier: 'hot' },
      { server: 'host2:8083', tier: 'cold' },
    ]);
    expect(result.serverToTier).toEqual({
      'host1:8083': 'hot',
      'host2:8083': 'cold',
    });
  });

  it('builds allServers in tier-sorted order', () => {
    const result = buildTieredServers([
      { server: 'cold-host:8083', tier: 'cold' },
      { server: 'hot-host2:8083', tier: 'hot' },
      { server: 'hot-host1:8083', tier: 'hot' },
    ]);
    expect(result.allServers).toEqual(['cold-host:8083', 'hot-host1:8083', 'hot-host2:8083']);
  });
});
