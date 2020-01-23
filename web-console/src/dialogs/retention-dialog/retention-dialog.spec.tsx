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

import { reorderArray, RetentionDialog } from './retention-dialog';

describe('retention dialog', () => {
  it('matches snapshot', () => {
    const retentionDialog = (
      <RetentionDialog
        datasource={'test'}
        rules={[null]}
        tiers={['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']}
        onEditDefaults={() => {}}
        onCancel={() => {}}
        onSave={() => {}}
      />
    );
    render(retentionDialog);
    expect(document.body.lastChild).toMatchSnapshot();
  });

  describe('reorderArray', () => {
    it('works when nothing changes', () => {
      const array = ['a', 'b', 'c', 'd', 'e'];

      const newArray = reorderArray(array, 0, 0);

      expect(newArray).toEqual(['a', 'b', 'c', 'd', 'e']);
      expect(array).toEqual(['a', 'b', 'c', 'd', 'e']);
    });

    it('works upward', () => {
      const array = ['a', 'b', 'c', 'd', 'e'];

      let newArray = reorderArray(array, 2, 1);
      expect(newArray).toEqual(['a', 'c', 'b', 'd', 'e']);
      expect(array).toEqual(['a', 'b', 'c', 'd', 'e']);

      newArray = reorderArray(array, 2, 0);
      expect(newArray).toEqual(['c', 'a', 'b', 'd', 'e']);
      expect(array).toEqual(['a', 'b', 'c', 'd', 'e']);
    });

    it('works downward', () => {
      const array = ['a', 'b', 'c', 'd', 'e'];

      let newArray = reorderArray(array, 2, 3);
      expect(newArray).toEqual(['a', 'b', 'c', 'd', 'e']);
      expect(array).toEqual(['a', 'b', 'c', 'd', 'e']);

      newArray = reorderArray(array, 2, 4);
      expect(newArray).toEqual(['a', 'b', 'd', 'c', 'e']);
      expect(array).toEqual(['a', 'b', 'c', 'd', 'e']);
    });
  });
});
