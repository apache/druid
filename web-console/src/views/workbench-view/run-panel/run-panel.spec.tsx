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

import { HotkeysProvider } from '@blueprintjs/core';
import { render } from '@testing-library/react';

import { DEFAULT_SERVER_QUERY_CONTEXT, DRUID_ENGINES, WorkbenchQuery } from '../../../druid-models';

import { RunPanel } from './run-panel';

describe('RunPanel', () => {
  it('matches snapshot on native (auto) query', () => {
    const runPanel = (
      <HotkeysProvider>
        <RunPanel
          query={WorkbenchQuery.blank().changeQueryString(`SELECT * FROM wikipedia`)}
          onQueryChange={() => {}}
          running={false}
          onRun={() => {}}
          queryEngines={DRUID_ENGINES}
          clusterCapacity={9}
          defaultQueryContext={DEFAULT_SERVER_QUERY_CONTEXT}
        />
      </HotkeysProvider>
    );
    const { container } = render(runPanel);
    expect(container.firstChild).toMatchSnapshot();
  });

  it('matches snapshot on msq (auto) query', () => {
    const runPanel = (
      <HotkeysProvider>
        <RunPanel
          query={WorkbenchQuery.blank()
            .changeQueryString(`SELECT * FROM wikipedia`)
            .changeEngine('sql-msq-task')}
          onQueryChange={() => {}}
          running={false}
          onRun={() => {}}
          queryEngines={DRUID_ENGINES}
          clusterCapacity={9}
          defaultQueryContext={DEFAULT_SERVER_QUERY_CONTEXT}
        />
      </HotkeysProvider>
    );
    const { container } = render(runPanel);
    expect(container.firstChild).toMatchSnapshot();
  });
});
