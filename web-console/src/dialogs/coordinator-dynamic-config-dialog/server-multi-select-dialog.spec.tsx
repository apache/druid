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

import { shallow } from '../../utils/shallow-renderer';

import { ServerMultiSelectDialog } from './server-multi-select-dialog';
import type { TieredServers } from './tiered-servers';

const MOCK_SERVERS: TieredServers = {
  tiers: ['hot', 'cold'],
  serversByTier: {
    hot: ['hot-host1:8083', 'hot-host2:8083'],
    cold: ['cold-host1:8083'],
  },
  serverToTier: {
    'hot-host1:8083': 'hot',
    'hot-host2:8083': 'hot',
    'cold-host1:8083': 'cold',
  },
  allServers: ['hot-host1:8083', 'hot-host2:8083', 'cold-host1:8083'],
};

describe('ServerMultiSelectDialog', () => {
  it('matches snapshot with no selection', () => {
    const dialog = shallow(
      <ServerMultiSelectDialog
        title="Decommissioning nodes"
        servers={MOCK_SERVERS}
        selectedServers={[]}
        onSave={() => {}}
        onClose={() => {}}
      />,
    );
    expect(dialog).toMatchSnapshot();
  });

  it('matches snapshot with existing selection', () => {
    const dialog = shallow(
      <ServerMultiSelectDialog
        title="Turbo loading nodes"
        servers={MOCK_SERVERS}
        selectedServers={['hot-host1:8083']}
        onSave={() => {}}
        onClose={() => {}}
      />,
    );
    expect(dialog).toMatchSnapshot();
  });

  it('matches snapshot when servers are loading', () => {
    const dialog = shallow(
      <ServerMultiSelectDialog
        title="Decommissioning nodes"
        servers={undefined}
        selectedServers={[]}
        onSave={() => {}}
        onClose={() => {}}
      />,
    );
    expect(dialog).toMatchSnapshot();
  });
});
