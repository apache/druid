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

import {
  Button,
  ButtonGroup,
  FormGroup,
  InputGroup,
  Intent,
  Radio,
  RadioGroup,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Tooltip2 } from '@blueprintjs/popover2';
import classNames from 'classnames';
import React, { useState } from 'react';

import { DestinationInfo, DestinationMode } from '../../../druid-models';

import './destination-form.scss';

interface DestinationFormProps {
  className?: string;
  existingTables: string[];
  destinationInfo: DestinationInfo;
  changeDestinationInfo(destinationInfo: DestinationInfo): void;
}

export const DestinationForm = React.memo(function DestinationForm(props: DestinationFormProps) {
  const { className, existingTables, destinationInfo, changeDestinationInfo } = props;

  const [tableSearch, setTableSearch] = useState<string>('');

  function changeMode(mode: DestinationMode) {
    changeDestinationInfo({ ...destinationInfo, mode });
  }

  const { mode, table } = destinationInfo;
  return (
    <div className={classNames('destination-form', className)}>
      <FormGroup>
        <ButtonGroup fill>
          <Button
            text="New table"
            active={mode === 'new'}
            onClick={() => {
              changeMode('new');
            }}
          />
          <Button
            text="Append to table"
            active={mode === 'insert'}
            onClick={() => {
              changeMode('insert');
            }}
          />
          <Button
            text="Replace table"
            active={mode === 'replace'}
            onClick={() => {
              changeMode('replace');
            }}
          />
        </ButtonGroup>
      </FormGroup>
      {mode === 'new' ? (
        <FormGroup label="New table name">
          <InputGroup
            value={table}
            onChange={e => {
              changeDestinationInfo({ ...destinationInfo, table: (e.target as any).value });
            }}
            placeholder="Choose a name"
            rightElement={
              existingTables.includes(table) ? (
                <Tooltip2 content="Table name already exists">
                  <Button icon={IconNames.DELETE} intent={Intent.DANGER} minimal />
                </Tooltip2>
              ) : undefined
            }
          />
        </FormGroup>
      ) : (
        <>
          <FormGroup label="Choose a table">
            <InputGroup
              value={tableSearch}
              onChange={e => {
                setTableSearch(e.target.value);
              }}
              placeholder="Search"
            />
          </FormGroup>
          <RadioGroup
            className="table-radios"
            selectedValue={table}
            onChange={e => {
              changeDestinationInfo({ ...destinationInfo, table: (e.target as any).value });
            }}
          >
            {existingTables
              .filter(t => t.includes(tableSearch))
              .map(table => (
                <Radio key={table} value={table}>
                  {table}
                </Radio>
              ))}
          </RadioGroup>
        </>
      )}
    </div>
  );
});
