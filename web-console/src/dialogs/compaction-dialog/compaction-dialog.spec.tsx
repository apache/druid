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

import { shallow } from 'enzyme';
import React from 'react';

import { CompactionDialog } from './compaction-dialog';

describe('CompactionDialog', () => {
  it('matches snapshot without compactionConfig', () => {
    const compactionDialog = shallow(
      <CompactionDialog
        onClose={() => {}}
        onSave={() => {}}
        onDelete={() => {}}
        datasource="test1"
        compactionConfig={undefined}
      />,
    );
    expect(compactionDialog).toMatchSnapshot();
  });

  it('matches snapshot with compactionConfig (dynamic partitionsSpec)', () => {
    const compactionDialog = shallow(
      <CompactionDialog
        onClose={() => {}}
        onSave={() => {}}
        onDelete={() => {}}
        datasource="test1"
        compactionConfig={{
          dataSource: 'test1',
          tuningConfig: { partitionsSpec: { type: 'dynamic' } },
        }}
      />,
    );
    expect(compactionDialog).toMatchSnapshot();
  });

  it('matches snapshot with compactionConfig (hashed partitionsSpec)', () => {
    const compactionDialog = shallow(
      <CompactionDialog
        onClose={() => {}}
        onSave={() => {}}
        onDelete={() => {}}
        datasource="test1"
        compactionConfig={{
          dataSource: 'test1',
          tuningConfig: { partitionsSpec: { type: 'hashed' } },
        }}
      />,
    );
    expect(compactionDialog).toMatchSnapshot();
  });

  it('matches snapshot with compactionConfig (range partitionsSpec)', () => {
    const compactionDialog = shallow(
      <CompactionDialog
        onClose={() => {}}
        onSave={() => {}}
        onDelete={() => {}}
        datasource="test1"
        compactionConfig={{
          dataSource: 'test1',
          tuningConfig: { partitionsSpec: { type: 'range' } },
        }}
      />,
    );
    expect(compactionDialog).toMatchSnapshot();
  });
});
