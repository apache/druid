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

import { render } from '@testing-library/react';

import { TieredReplicant } from './tiered-replicant';

describe('TieredReplicant', () => {
  it('matches snapshot with existing tier', () => {
    const tieredReplicant = (
      <TieredReplicant
        tier="test1"
        replication={2}
        tiers={['test1', 'test2', 'test3']}
        usedTiers={['test1']}
        disabled={false}
        onChangeTier={() => {}}
        onChangeReplication={() => {}}
        onRemove={() => {}}
      />
    );
    const { container } = render(tieredReplicant);
    expect(container.firstChild).toMatchSnapshot();
  });

  it('matches snapshot with non-existing tier', () => {
    const tieredReplicant = (
      <TieredReplicant
        tier="nonexist"
        replication={1}
        tiers={['test1', 'test2', 'test3']}
        usedTiers={['nonexist']}
        disabled={false}
        onChangeTier={() => {}}
        onChangeReplication={() => {}}
        onRemove={() => {}}
      />
    );
    const { container } = render(tieredReplicant);
    expect(container.firstChild).toMatchSnapshot();
  });

  it('matches snapshot when disabled', () => {
    const tieredReplicant = (
      <TieredReplicant
        tier="test1"
        replication={3}
        tiers={['test1', 'test2', 'test3']}
        usedTiers={['test1']}
        disabled
        onChangeTier={() => {}}
        onChangeReplication={() => {}}
        onRemove={() => {}}
      />
    );
    const { container } = render(tieredReplicant);
    expect(container.firstChild).toMatchSnapshot();
  });

  it('matches snapshot without remove button', () => {
    const tieredReplicant = (
      <TieredReplicant
        tier="test2"
        replication={1}
        tiers={['test1', 'test2', 'test3']}
        usedTiers={['test2']}
        disabled={false}
        onChangeTier={() => {}}
        onChangeReplication={() => {}}
        onRemove={undefined}
      />
    );
    const { container } = render(tieredReplicant);
    expect(container.firstChild).toMatchSnapshot();
  });

  it('matches snapshot with multiple used tiers', () => {
    const tieredReplicant = (
      <TieredReplicant
        tier="test3"
        replication={5}
        tiers={['test1', 'test2', 'test3']}
        usedTiers={['test1', 'test2']}
        disabled={false}
        onChangeTier={() => {}}
        onChangeReplication={() => {}}
        onRemove={() => {}}
      />
    );
    const { container } = render(tieredReplicant);
    expect(container.firstChild).toMatchSnapshot();
  });
});
