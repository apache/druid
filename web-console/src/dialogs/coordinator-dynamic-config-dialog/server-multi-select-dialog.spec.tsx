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

import { fireEvent, render, screen } from '@testing-library/react';

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

const EMPTY_SERVERS: TieredServers = {
  tiers: [],
  serversByTier: {},
  serverToTier: {},
  allServers: [],
};

function clickCheckbox(label: string | RegExp) {
  const checkbox = screen.getByLabelText(label);
  fireEvent.click(checkbox);
}

function clickButton(name: string) {
  fireEvent.click(screen.getByText(name));
}

describe('ServerMultiSelectDialog behavior', () => {
  it('toggles individual server and saves', () => {
    const onSave = jest.fn();
    const onClose = jest.fn();
    render(
      <ServerMultiSelectDialog
        title="Test"
        servers={MOCK_SERVERS}
        selectedServers={[]}
        onSave={onSave}
        onClose={onClose}
      />,
    );

    clickCheckbox('cold-host1:8083');
    clickButton('Save');

    expect(onSave).toHaveBeenCalledWith(['cold-host1:8083']);
    expect(onClose).toHaveBeenCalled();
  });

  it('toggles tier to select all servers in that tier', () => {
    const onSave = jest.fn();
    render(
      <ServerMultiSelectDialog
        title="Test"
        servers={MOCK_SERVERS}
        selectedServers={[]}
        onSave={onSave}
        onClose={() => {}}
      />,
    );

    clickCheckbox('hot');
    clickButton('Save');

    expect(onSave).toHaveBeenCalledWith(['hot-host1:8083', 'hot-host2:8083']);
  });

  it('toggles tier to deselect all servers when all are selected', () => {
    const onSave = jest.fn();
    render(
      <ServerMultiSelectDialog
        title="Test"
        servers={MOCK_SERVERS}
        selectedServers={['hot-host1:8083', 'hot-host2:8083']}
        onSave={onSave}
        onClose={() => {}}
      />,
    );

    clickCheckbox('hot');
    clickButton('Save');

    // Only cold-host1 was never touched, hot tier was deselected
    expect(onSave).toHaveBeenCalledWith([]);
  });

  it('filters servers by search text', () => {
    render(
      <ServerMultiSelectDialog
        title="Test"
        servers={MOCK_SERVERS}
        selectedServers={[]}
        onSave={() => {}}
        onClose={() => {}}
      />,
    );

    fireEvent.change(screen.getByPlaceholderText('Search servers...'), {
      target: { value: 'hot-host1' },
    });

    expect(screen.getByLabelText('hot-host1:8083')).toBeTruthy();
    expect(screen.queryByLabelText('hot-host2:8083')).toBeNull();
    expect(screen.queryByLabelText('cold-host1:8083')).toBeNull();
  });

  it('tier toggle only affects filtered servers when search is active', () => {
    const onSave = jest.fn();
    render(
      <ServerMultiSelectDialog
        title="Test"
        servers={MOCK_SERVERS}
        selectedServers={[]}
        onSave={onSave}
        onClose={() => {}}
      />,
    );

    // Filter to just hot-host1
    fireEvent.change(screen.getByPlaceholderText('Search servers...'), {
      target: { value: 'hot-host1' },
    });

    // Toggle the hot tier (only hot-host1 is visible, label includes "hidden by filter")
    clickCheckbox(/^hot \(/);

    // Clear search
    fireEvent.change(screen.getByPlaceholderText('Search servers...'), {
      target: { value: '' },
    });

    // Save — only hot-host1 should be selected, not hot-host2
    clickButton('Save');
    expect(onSave).toHaveBeenCalledWith(['hot-host1:8083']);
  });

  it('shows stale server warning and removes them', () => {
    const onSave = jest.fn();
    render(
      <ServerMultiSelectDialog
        title="Test"
        servers={MOCK_SERVERS}
        selectedServers={['hot-host1:8083', 'gone-server:8083']}
        onSave={onSave}
        onClose={() => {}}
      />,
    );

    expect(screen.getByText(/no longer in the cluster/)).toBeTruthy();
    expect(screen.getByText(/gone-server:8083/)).toBeTruthy();

    clickButton('Remove');
    clickButton('Save');

    // gone-server removed, hot-host1 retained
    expect(onSave).toHaveBeenCalledWith(['hot-host1:8083']);
  });

  it('cancel calls onClose but not onSave', () => {
    const onSave = jest.fn();
    const onClose = jest.fn();
    render(
      <ServerMultiSelectDialog
        title="Test"
        servers={MOCK_SERVERS}
        selectedServers={[]}
        onSave={onSave}
        onClose={onClose}
      />,
    );

    clickButton('Cancel');

    expect(onClose).toHaveBeenCalled();
    expect(onSave).not.toHaveBeenCalled();
  });

  it('shows empty cluster message', () => {
    render(
      <ServerMultiSelectDialog
        title="Test"
        servers={EMPTY_SERVERS}
        selectedServers={[]}
        onSave={() => {}}
        onClose={() => {}}
      />,
    );

    expect(screen.getByText('No historical servers found in the cluster.')).toBeTruthy();
  });

  it('shows correct selected count with singular and plural', () => {
    render(
      <ServerMultiSelectDialog
        title="Test"
        servers={MOCK_SERVERS}
        selectedServers={['hot-host1:8083']}
        onSave={() => {}}
        onClose={() => {}}
      />,
    );

    expect(screen.getByText('1 server selected')).toBeTruthy();

    clickCheckbox('hot-host2:8083');

    expect(screen.getByText('2 servers selected')).toBeTruthy();
  });
});
