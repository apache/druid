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

import { StatusDialog } from './status-dialog';

describe('status dialog', () => {
  it('matches snapshot', () => {
    const statusDialog = <StatusDialog onClose={() => {}} />;
    render(statusDialog);
    expect(document.body.lastChild).toMatchSnapshot();
  });

  it('filters data that contains input', () => {
    const data = [
      'org.apache.druid.common.gcp.GcpModule',
      'org.apache.druid.common.aws.AWSModule',
      'io.imply.druid.UtilityBeltModule',
    ];

    expect(StatusDialog.anywhereMatcher({ id: '0', value: 'common' }, data)).toEqual(true);
    expect(StatusDialog.anywhereMatcher({ id: '1', value: 'common' }, data)).toEqual(true);
    expect(StatusDialog.anywhereMatcher({ id: '0', value: 'org' }, data)).toEqual(true);
    expect(StatusDialog.anywhereMatcher({ id: '1', value: 'org' }, data)).toEqual(true);
    expect(StatusDialog.anywhereMatcher({ id: '2', value: 'common' }, data)).toEqual(false);
  });
});
