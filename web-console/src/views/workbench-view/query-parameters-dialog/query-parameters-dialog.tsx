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
  Classes,
  Code,
  ControlGroup,
  Dialog,
  FormGroup,
  InputGroup,
  Intent,
  Menu,
  MenuItem,
  Position,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import type { QueryParameter } from '@druid-toolkit/query';
import { isEmptyArray } from '@druid-toolkit/query';
import React, { useState } from 'react';

import { FancyNumericInput } from '../../../components/fancy-numeric-input/fancy-numeric-input';
import { deepSet, oneOf, tickIcon, without } from '../../../utils';

import './query-parameters-dialog.scss';

const TYPES = ['VARCHAR', 'TIMESTAMP', 'BIGINT', 'DOUBLE', 'FLOAT'];

interface QueryParametersDialogProps {
  queryParameters: QueryParameter[] | undefined;
  onQueryParametersChange(parameters: QueryParameter[] | undefined): void;
  onClose(): void;
}

export const QueryParametersDialog = React.memo(function QueryParametersDialog(
  props: QueryParametersDialogProps,
) {
  const { queryParameters, onQueryParametersChange, onClose } = props;
  const [currentQueryParameters, setCurrentQueryParameters] = useState(queryParameters || []);

  function onSave() {
    onQueryParametersChange(
      isEmptyArray(currentQueryParameters) ? undefined : currentQueryParameters,
    );
    onClose();
  }

  return (
    <Dialog
      className="query-parameters-dialog"
      isOpen
      onClose={onClose}
      title="Dynamic query parameters"
    >
      <div className={Classes.DIALOG_BODY}>
        <p>
          Druid SQL supports dynamic parameters using question mark <Code>?</Code> syntax, where
          parameters are bound positionally to ? placeholders at execution time.
        </p>
        {currentQueryParameters.map((queryParameter, i) => {
          const { type, value } = queryParameter;

          function onValueChange(v: string | number) {
            setCurrentQueryParameters(deepSet(currentQueryParameters, `${i}.value`, v));
          }

          return (
            <FormGroup key={i} label={`Parameter in position ${i + 1}`}>
              <ControlGroup fill>
                <Popover2
                  minimal
                  position={Position.BOTTOM_LEFT}
                  content={
                    <Menu>
                      {TYPES.map(t => (
                        <MenuItem
                          key={t}
                          icon={tickIcon(t === type)}
                          text={t}
                          onClick={() => {
                            setCurrentQueryParameters(
                              deepSet(currentQueryParameters, `${i}.type`, t),
                            );
                          }}
                        />
                      ))}
                    </Menu>
                  }
                >
                  <Button text={type} rightIcon={IconNames.CARET_DOWN} />
                </Popover2>
                {oneOf(type, 'BIGINT', 'DOUBLE', 'FLOAT') ? (
                  <FancyNumericInput
                    value={Number(value)}
                    onValueChange={onValueChange}
                    fill
                    arbitraryPrecision={type !== 'BIGINT'}
                  />
                ) : (
                  <InputGroup
                    value={String(value)}
                    onChange={(e: any) => onValueChange(e.target.value)}
                    placeholder={type === 'TIMESTAMP' ? '2022-01-01 00:00:00' : 'Parameter value'}
                    fill
                  />
                )}
                <Button
                  icon={IconNames.TRASH}
                  onClick={() => {
                    setCurrentQueryParameters(without(currentQueryParameters, queryParameter));
                  }}
                />
              </ControlGroup>
            </FormGroup>
          );
        })}
        <Button
          icon={IconNames.PLUS}
          text="Add parameter"
          intent={currentQueryParameters.length ? undefined : Intent.PRIMARY}
          onClick={() => {
            setCurrentQueryParameters(
              currentQueryParameters.concat({ type: 'VARCHAR', value: '' }),
            );
          }}
        />
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" onClick={onClose} />
          <Button text="Save" intent={Intent.PRIMARY} onClick={onSave} />
        </div>
      </div>
    </Dialog>
  );
});
