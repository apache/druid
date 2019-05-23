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


import {Intent} from '@blueprintjs/core';
import * as React from 'react';
import { render } from 'react-testing-library';

import {SupervisorTableActionDialog} from './supervisor-table-action-dialog';

const basicAction = {title: 'test', onAction: () => null};
describe('describe supervisor table action dialog', () => {
  it('supervisor table action dialog snapshot', () => {
    const supervisorTableActionDialog =
      <SupervisorTableActionDialog
        supervisorId={'test'}
        actions={[basicAction, basicAction, basicAction, basicAction]}
        onClose={() => null}
        isOpen={true}
      />;
    const { container, getByText } = render(supervisorTableActionDialog, { container: document.body });
    expect(container.firstChild).toMatchSnapshot();
  });
});
