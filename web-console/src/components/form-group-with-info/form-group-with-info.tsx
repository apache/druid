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

import { FormGroup, Icon } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import React from 'react';

import './form-group-with-info.scss';

export interface FormGroupWithInfoProps {
  label?: React.ReactNode;
  info?: JSX.Element | string;
  inlineInfo?: boolean;
  children?: React.ReactNode;
}

export const FormGroupWithInfo = React.memo(function FormGroupWithInfo(
  props: FormGroupWithInfoProps,
) {
  const { label, info, inlineInfo, children } = props;

  const popover = (
    <Popover2 className="info-popover" content={info} position="left-bottom">
      <Icon icon={IconNames.INFO_SIGN} size={14} />
    </Popover2>
  );

  return (
    <FormGroup
      className="form-group-with-info"
      label={label}
      labelInfo={info && !inlineInfo && popover}
    >
      {children}
      {info && inlineInfo && popover}
    </FormGroup>
  );
});
