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
  InputGroup,
  Intent,
  Menu,
  MenuItem,
  NumericInput,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import type {
  ExpressionMeta,
  OptionValue,
  ParameterDefinition,
  RegisteredVisualModule,
} from '@druid-toolkit/visuals-core';
import { getPluginOptionLabel } from '@druid-toolkit/visuals-core';
import type { JSX } from 'react';
import React from 'react';

import { AutoForm, FormGroupWithInfo, PopoverText } from '../../../components';
import { AppToaster } from '../../../singletons';
import { ColumnPickerMenu } from '../column-picker-menu/column-picker-menu';
import { DroppableContainer } from '../droppable-container/droppable-container';

import { AggregateMenu } from './aggregate-menu';
import { ColumnsInput } from './columns-input';
import { getPossibleAggregateForColumn } from './helpers';
import { OptionsInput } from './options-input';

import './control-pane.scss';

export interface ControlPaneProps {
  columns: ExpressionMeta[];
  onUpdateParameterValues(params: Record<string, unknown>): void;
  parameterValues: Record<string, unknown>;
  visualModule: RegisteredVisualModule;
}

export const ControlPane = function ControlPane(props: ControlPaneProps) {
  const { columns, onUpdateParameterValues, parameterValues, visualModule } = props;

  function renderOptionsPropInput(
    parameter: ParameterDefinition,
    value: any,
    onValueChange: (value: any) => void,
  ): {
    element: JSX.Element;
    onDropColumn?: (column: ExpressionMeta) => void;
  } {
    switch (parameter.type) {
      case 'boolean': {
        return {
          element: (
            <ButtonGroup>
              <Button
                active={value === false}
                onClick={() => {
                  onValueChange(false);
                }}
              >
                False
              </Button>
              <Button
                active={value === true}
                onClick={() => {
                  onValueChange(true);
                }}
              >
                True
              </Button>
            </ButtonGroup>
          ),
        };
      }

      case 'number':
        return {
          element: (
            <NumericInput
              value={(value as string) ?? ''}
              onValueChange={v => onValueChange(v)}
              placeholder={parameter.control?.placeholder}
              fill
              min={parameter.min}
              max={parameter.max}
            />
          ),
        };

      case 'string':
        return {
          element: (
            <InputGroup
              value={(value as string) || ''}
              onChange={e => onValueChange(e.target.value)}
              placeholder={parameter.control?.placeholder}
              fill
            />
          ),
        };

      case 'option': {
        const controlOptions = parameter.options || [];
        const selectedOption: OptionValue | undefined = controlOptions.find(o => o === value);
        return {
          element: (
            <Popover2
              fill
              position="bottom-left"
              minimal
              content={
                <Menu>
                  {controlOptions.map((o, i) => (
                    <MenuItem
                      key={i}
                      text={getPluginOptionLabel(o, parameter)}
                      onClick={() => onValueChange(o)}
                    />
                  ))}
                </Menu>
              }
            >
              <InputGroup
                value={
                  typeof selectedOption === 'undefined'
                    ? String(value)
                    : getPluginOptionLabel(selectedOption, parameter)
                }
                readOnly
                fill
                rightElement={<Button icon={IconNames.CARET_DOWN} minimal />}
              />
            </Popover2>
          ),
        };
      }

      case 'options': {
        return {
          element: (
            <OptionsInput
              options={parameter.options || []}
              value={(value as OptionValue[]) || []}
              onValueChange={onValueChange}
              parameter={parameter}
            />
          ),
        };
      }

      case 'column':
        return {
          element: (
            <Popover2
              fill
              position="bottom-left"
              minimal
              content={
                <ColumnPickerMenu
                  columns={columns}
                  onSelectNone={
                    parameter.control?.required ? undefined : () => onValueChange(undefined)
                  }
                  onSelectColumn={onValueChange}
                />
              }
            >
              <InputGroup
                value={(value as ExpressionMeta)?.name || 'None'}
                readOnly
                fill
                rightElement={<Button icon={IconNames.CARET_DOWN} minimal />}
              />
            </Popover2>
          ),
          onDropColumn: onValueChange,
        };

      case 'columns': {
        return {
          element: (
            <ColumnsInput
              columns={columns}
              allowReordering
              value={(value as ExpressionMeta[]) || []}
              onValueChange={onValueChange}
              allowDuplicates={parameter.allowDuplicates}
            />
          ),
          onDropColumn: (column: ExpressionMeta) => {
            value = value || [];
            const columnName = column.name;
            if (
              !parameter.allowDuplicates &&
              value.find((v: ExpressionMeta) => v.name === columnName)
            ) {
              AppToaster.show({
                intent: Intent.WARNING,
                message: `"${columnName}" already selected`,
              });
              return;
            }
            onValueChange(value.concat(column));
          },
        };
      }

      case 'aggregate': {
        return {
          element: (
            <Popover2
              fill
              position="bottom-left"
              minimal
              content={
                <AggregateMenu
                  columns={columns}
                  onSelectAggregate={onValueChange}
                  onSelectNone={
                    parameter.control?.required ? undefined : () => onValueChange(undefined)
                  }
                />
              }
            >
              <InputGroup
                value={value ? (value as { name: string }).name : 'None'}
                readOnly
                fill
                rightElement={<Button icon={IconNames.CARET_DOWN} minimal />}
              />
            </Popover2>
          ),
          onDropColumn: column => {
            const aggregates = getPossibleAggregateForColumn(column);
            if (!aggregates.length) return;
            onValueChange(aggregates[0]);
          },
        };
      }

      case 'aggregates': {
        return {
          element: (
            <ColumnsInput
              columns={columns}
              value={(value as ExpressionMeta[]) || []}
              onValueChange={onValueChange}
              allowReordering
              pickerMenu={availableColumns => (
                <AggregateMenu
                  columns={availableColumns}
                  onSelectAggregate={c => onValueChange((value as ExpressionMeta[]).concat(c))}
                />
              )}
            />
          ),
          onDropColumn: column => {
            value = value || [];
            const aggregates = getPossibleAggregateForColumn(column).filter(
              p => !value.some((v: ExpressionMeta) => v.name === p.name),
            );
            if (!aggregates.length) return;
            onValueChange(value.concat(aggregates[0]));
          },
        };
      }

      default:
        return {
          element: (
            <Button
              icon={IconNames.ERROR}
              text={`Type not supported: ${(parameter as { type: string }).type}`}
              disabled
              fill
            />
          ),
        };
    }
  }

  const namedParameters = Object.entries(visualModule.parameterDefinitions ?? {});

  return (
    <div className="control-pane">
      {namedParameters.map(([name, parameter], i) => {
        const visible = parameter.control?.visible;
        if (
          visible === false ||
          (typeof visible === 'function' && !visible({ params: parameterValues }))
        ) {
          return;
        }

        const value = parameterValues[name];
        function onValueChange(newValue: unknown) {
          onUpdateParameterValues({ [name]: newValue });
        }

        const { element, onDropColumn } = renderOptionsPropInput(parameter, value, onValueChange);

        const formGroup = (
          <FormGroupWithInfo
            key={i}
            label={parameter.control?.label || AutoForm.makeLabelName(name)}
            info={
              parameter.control?.description ? (
                <PopoverText>{parameter.control.description}</PopoverText>
              ) : undefined
            }
          >
            {element}
          </FormGroupWithInfo>
        );

        if (!onDropColumn) {
          return formGroup;
        }

        return (
          <DroppableContainer key={i} onDropColumn={onDropColumn}>
            {formGroup}
          </DroppableContainer>
        );
      })}
    </div>
  );
};
