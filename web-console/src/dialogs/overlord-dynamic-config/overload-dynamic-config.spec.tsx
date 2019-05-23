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


import * as React from 'react';
import { render } from 'react-testing-library';

import {LookupEditDialog} from '..';

describe('describe overload dynamic config', () => {
  it('overload dynamic config snapshot', () => {
    const lookupEditDialog =
      <LookupEditDialog
        isOpen={true}
        onClose={() => null}
        onSubmit={() => null}
        onChange={() => null}
        lookupName={'test'}
        lookupTier={'test'}
        lookupVersion={'test'}
        lookupSpec={'test'}
        isEdit={false}
        allLookupTiers={['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']}
      />;
    const { container, getByText } = render(lookupEditDialog, { container: document.body });
    expect(container.firstChild).toMatchSnapshot();
  });
});
