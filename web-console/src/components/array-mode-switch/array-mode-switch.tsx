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

import { Switch } from '@blueprintjs/core';
import React from 'react';

import type { ArrayMode } from '../../druid-models';
import { getLink } from '../../links';
import { ExternalLink } from '../external-link/external-link';
import { FormGroupWithInfo } from '../form-group-with-info/form-group-with-info';
import { PopoverText } from '../popover-text/popover-text';

export interface ArrayModeSwitchProps {
  arrayMode: ArrayMode;
  changeArrayMode(newArrayMode: ArrayMode): void;
}

export const ArrayModeSwitch = React.memo(function ArrayModeSwitch(props: ArrayModeSwitchProps) {
  const { arrayMode, changeArrayMode } = props;

  return (
    <FormGroupWithInfo
      inlineInfo
      info={
        <PopoverText>
          <p>
            Store arrays as multi-value string columns instead of arrays. Note that all detected
            array elements will be coerced to strings if you choose this option, and data will
            behave more like a string than an array at query time. See{' '}
            <ExternalLink href={`${getLink('DOCS')}/querying/arrays`}>array docs</ExternalLink> and{' '}
            <ExternalLink href={`${getLink('DOCS')}/querying/multi-value-dimensions`}>
              mvd docs
            </ExternalLink>{' '}
            for more details about the differences between arrays and multi-value strings.
          </p>
        </PopoverText>
      }
    >
      <Switch
        label="Store ARRAYs as MVDs"
        className="legacy-switch"
        checked={arrayMode === 'multi-values'}
        onChange={() => changeArrayMode(arrayMode === 'arrays' ? 'multi-values' : 'arrays')}
      />
    </FormGroupWithInfo>
  );
});
