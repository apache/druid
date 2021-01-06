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

import { AutoForm } from './auto-form';

describe('AutoForm', () => {
  it('matches snapshot', () => {
    const autoForm = shallow(
      <AutoForm
        fields={[
          { name: 'testOne', type: 'number' },
          { name: 'testTwo', type: 'size-bytes' },
          { name: 'testThree', type: 'string' },
          { name: 'testFour', type: 'boolean' },
          { name: 'testFourWithDefault', type: 'boolean', defaultValue: false },
          { name: 'testFive', type: 'string-array' },
          { name: 'testSix', type: 'json' },
          { name: 'testSeven', type: 'json' },

          { name: 'testNotDefined', type: 'string', defined: false },
          { name: 'testAdvanced', type: 'string', hideInMore: true },
        ]}
        model={String}
        onChange={() => {}}
      />,
    );
    expect(autoForm).toMatchSnapshot();
  });
});
