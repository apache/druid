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

import { RuleProviderEditor } from './rule-provider-editor';

describe('RuleProviderEditor', () => {
  it('matches snapshot with empty provider', () => {
    const editor = shallow(
      <RuleProviderEditor onClose={() => {}} onSave={() => {}} ruleProvider={undefined} />,
    );
    expect(editor).toMatchSnapshot();
  });

  it('matches snapshot with rules', () => {
    const editor = shallow(
      <RuleProviderEditor
        onClose={() => {}}
        onSave={() => {}}
        ruleProvider={{
          type: 'inline',
          partitioningRules: [
            {
              id: 'daily-30d',
              olderThan: 'P30D',
              segmentGranularity: 'DAY',
              partitionsSpec: { type: 'dynamic', maxRowsPerSegment: 5000000 },
            },
          ],
          deletionRules: [
            {
              id: 'remove-bots',
              olderThan: 'P90D',
              deleteWhere: { type: 'equals', column: 'isRobot', matchValue: 'true' },
            },
          ],
        }}
      />,
    );
    expect(editor).toMatchSnapshot();
  });
});
