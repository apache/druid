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

import { CloneServerMappingDialog } from './clone-server-mapping-dialog';
import type { TieredServers } from './tiered-servers';

const MOCK_SERVERS: TieredServers = {
  tiers: ['_default_tier'],
  serversByTier: {
    _default_tier: ['host1:8083', 'host2:8083', 'host3:8083'],
  },
  serverToTier: {
    'host1:8083': '_default_tier',
    'host2:8083': '_default_tier',
    'host3:8083': '_default_tier',
  },
  allServers: ['host1:8083', 'host2:8083', 'host3:8083'],
};

describe('CloneServerMappingDialog', () => {
  it('matches snapshot with no existing mappings', () => {
    const dialog = shallow(
      <CloneServerMappingDialog
        servers={MOCK_SERVERS}
        cloneServers={{}}
        onSave={() => {}}
        onClose={() => {}}
      />,
    );
    expect(dialog).toMatchSnapshot();
  });

  it('matches snapshot with existing mappings', () => {
    const dialog = shallow(
      <CloneServerMappingDialog
        servers={MOCK_SERVERS}
        cloneServers={{ 'host2:8083': 'host1:8083' }}
        onSave={() => {}}
        onClose={() => {}}
      />,
    );
    expect(dialog).toMatchSnapshot();
  });

  it('matches snapshot when servers are loading', () => {
    const dialog = shallow(
      <CloneServerMappingDialog
        servers={undefined}
        cloneServers={{}}
        onSave={() => {}}
        onClose={() => {}}
      />,
    );
    expect(dialog).toMatchSnapshot();
  });

  it('renders self-clone mapping (filtered on save by component logic)', () => {
    // Verify the dialog renders with a self-clone mapping present.
    // The component's handleSave filters out target === source mappings.
    const dialog = shallow(
      <CloneServerMappingDialog
        servers={MOCK_SERVERS}
        cloneServers={{ 'host1:8083': 'host1:8083', 'host2:8083': 'host1:8083' }}
        onSave={() => {}}
        onClose={() => {}}
      />,
    );
    expect(dialog).toMatchSnapshot();
  });
});
