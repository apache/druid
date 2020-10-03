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

import { Button, ButtonGroup, FormGroup } from '@blueprintjs/core';
import React from 'react';

export type FormJsonTabs = 'form' | 'json';

export interface FormJsonSelectorProps {
  tab: FormJsonTabs;
  onChange: (tab: FormJsonTabs) => void;
}

export const FormJsonSelector = React.memo(function FormJsonSelector(props: FormJsonSelectorProps) {
  const { tab, onChange } = props;

  return (
    <FormGroup className="form-json-selector">
      <ButtonGroup fill>
        <Button text="Form" active={tab === 'form'} onClick={() => onChange('form')} />
        <Button text="JSON" active={tab === 'json'} onClick={() => onChange('json')} />
      </ButtonGroup>
    </FormGroup>
  );
});
