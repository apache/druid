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
import React from 'react';

import { LookupEditDialog } from './lookup-edit-dialog';

describe('lookup edit dialog', () => {
  it('matches snapshot', () => {
    const lookupEditDialog = (
      <LookupEditDialog
        onClose={() => {}}
        onSubmit={() => {}}
        onChange={() => {}}
        lookupName={'test'}
        lookupTier={'test'}
        lookupVersion={'test'}
        lookupSpec={'test'}
        isEdit={false}
        allLookupTiers={['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']}
      />
    );

    render(lookupEditDialog);
    expect(document.body.lastChild).toMatchSnapshot();
  });
});
