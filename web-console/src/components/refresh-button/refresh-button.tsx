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

import { IconNames } from '@blueprintjs/icons';
import React from 'react';

import { LocalStorageKeys } from '../../utils';
import { TimedButton } from '../timed-button/timed-button';

export interface RefreshButtonProps {
  onRefresh: (auto: boolean) => void;
  localStorageKey?: LocalStorageKeys;
}

export const RefreshButton = React.memo(function RefreshButton(props: RefreshButtonProps) {
  const { onRefresh, localStorageKey } = props;
  const intervals = [
    { label: '5 seconds', value: 5000 },
    { label: '10 seconds', value: 10000 },
    { label: '30 seconds', value: 30000 },
    { label: '1 minute', value: 60000 },
    { label: '2 minutes', value: 120000 },
    { label: 'None', value: 0 },
  ];

  return (
    <TimedButton
      defaultValue={30000}
      label="Auto refresh every:"
      intervals={intervals}
      icon={IconNames.REFRESH}
      text="Refresh"
      onRefresh={onRefresh}
      localStorageKey={localStorageKey}
    />
  );
});
