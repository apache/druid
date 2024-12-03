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
  InputGroup,
  Intent,
  Menu,
  MenuItem,
  Popover,
  SegmentedControl,
  Tag,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import type { Column, SqlExpression } from 'druid-query-toolkit';
import { SqlColumn } from 'druid-query-toolkit';
import type { JSX } from 'react';

import {
  AutoForm,
  FancyNumericInput,
  FormGroupWithInfo,
  PopoverText,
} from '../../../../components';
import { AppToaster } from '../../../../singletons';
import { filterMap } from '../../../../utils';
import type { OptionValue, ParameterDefinition, QuerySource } from '../../models';
import {
  effectiveParameterDefault,
  evaluateFunctor,
  ExpressionMeta,
  getModuleOptionLabel,
  Measure,
} from '../../models';
import { changeOrAdd } from '../../utils';
import { DroppableContainer } from '../droppable-container/droppable-container';

import { ExpressionMenu } from './expression-menu';
import { MeasureMenu } from './measure-menu';
import { NamedExpressionsInput } from './named-expressions-input';
import { OptionsInput } from './options-input';

import './control-pane.scss';

export interface ControlPaneProps {
  querySource: QuerySource | undefined;
  onUpdateParameterValues(params: Record<string, unknown>): void;
  parameters: Record<string, ParameterDefinition>;
  parameterValues: Record<string, unknown>;
  onAddToSourceQueryAsColumn?(expression: SqlExpression): void;
  onAddToSourceQueryAsMeasure?(measure: Measure): void;
}

export const ControlPane = function ControlPane(props: ControlPaneProps) {
  const {
    querySource,
    onUpdateParameterValues,
    parameters,
    parameterValues,
    onAddToSourceQueryAsColumn,
    onAddToSourceQueryAsMeasure,
  } = props;
  const columns = querySource?.columns || [];
  const measures = querySource?.measures || [];

  function renderOptionsPropInput(
    parameter: ParameterDefinition,
    value: any,
    onValueChange: (value: any) => void,
  ): {
    element: JSX.Element;
    onDropColumn?: (column: Column) => void;
    onDropMeasure?: (measure: Measure) => void;
  } {
    const effectiveValue = value ?? effectiveParameterDefault(parameter, querySource);
    const required = evaluateFunctor(parameter.required, parameterValues);
    switch (parameter.type) {
      case 'boolean': {
        return {
          element: (
            <SegmentedControl
              value={String(effectiveValue)}
              onValueChange={v => {
                onValueChange(v === 'true');
              }}
              options={[
                { value: 'false', label: 'False' },
                { value: 'true', label: 'True' },
              ]}
              small
            />
          ),
        };
      }

      case 'number':
        return {
          element: (
            <FancyNumericInput
              value={effectiveValue}
              onValueChange={onValueChange}
              placeholder={parameter.placeholder}
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
              value={(effectiveValue as string) || ''}
              onChange={e => onValueChange(e.target.value)}
              placeholder={parameter.placeholder}
              fill
            />
          ),
        };

      case 'option': {
        const controlOptions = parameter.options || [];
        const selectedOption: OptionValue | undefined = controlOptions.find(
          o => o === effectiveValue,
        );
        return {
          element: (
            <Popover
              fill
              position="bottom-left"
              minimal
              content={
                <Menu>
                  {controlOptions.map((o, i) => (
                    <MenuItem
                      key={i}
                      text={getModuleOptionLabel(o, parameter)}
                      onClick={() => onValueChange(o)}
                    />
                  ))}
                </Menu>
              }
            >
              <InputGroup
                value={
                  typeof selectedOption === 'undefined'
                    ? String(effectiveValue)
                    : getModuleOptionLabel(selectedOption, parameter)
                }
                readOnly
                fill
                rightElement={<Button icon={IconNames.CARET_DOWN} minimal />}
              />
            </Popover>
          ),
        };
      }

      case 'options': {
        return {
          element: (
            <OptionsInput
              options={parameter.options || []}
              value={(effectiveValue as OptionValue[]) || []}
              onValueChange={onValueChange}
              optionLabel={o => getModuleOptionLabel(o, parameter)}
              allowDuplicates={parameter.allowDuplicates}
              nonEmpty={parameter.nonEmpty}
            />
          ),
        };
      }

      case 'expression': {
        const handleSelectColumn = (c: Column) => {
          onValueChange(ExpressionMeta.fromColumn(c));
        };
        return {
          element: (
            <NamedExpressionsInput<ExpressionMeta>
              allowReordering
              values={effectiveValue ? [effectiveValue] : []}
              onValuesChange={vs => onValueChange(vs[0])}
              singleton
              nonEmpty={required}
              itemMenu={(initExpression, onClose) => (
                <ExpressionMenu
                  columns={columns}
                  initExpression={initExpression}
                  onSelectExpression={onValueChange}
                  onClose={onClose}
                  onAddToSourceQueryAsColumn={onAddToSourceQueryAsColumn}
                />
              )}
            />
          ),
          onDropColumn: handleSelectColumn,
        };
      }

      case 'expressions': {
        const disabledColumnNames = parameter.allowDuplicates
          ? []
          : filterMap(effectiveValue as ExpressionMeta[], ({ expression }) =>
              expression instanceof SqlColumn ? expression.getName() : undefined,
            );

        return {
          element: (
            <NamedExpressionsInput<ExpressionMeta>
              allowReordering
              values={effectiveValue as ExpressionMeta[]}
              onValuesChange={onValueChange}
              nonEmpty={parameter.nonEmpty}
              itemMenu={(initExpression, onClose) => (
                <ExpressionMenu
                  columns={columns}
                  initExpression={initExpression}
                  onSelectExpression={c =>
                    onValueChange(changeOrAdd(effectiveValue, initExpression, c))
                  }
                  disabledColumnNames={disabledColumnNames}
                  onClose={onClose}
                  onAddToSourceQueryAsColumn={onAddToSourceQueryAsColumn}
                />
              )}
            />
          ),
          onDropColumn: (column: Column) => {
            const columnName = column.name;
            if (
              !parameter.allowDuplicates &&
              effectiveValue.find((v: ExpressionMeta) => v.name === columnName)
            ) {
              AppToaster.show({
                intent: Intent.WARNING,
                message: (
                  <>
                    <Tag minimal>{columnName}</Tag> already selected
                  </>
                ),
              });
              return;
            }
            onValueChange(effectiveValue.concat(ExpressionMeta.fromColumn(column)));
          },
        };
      }

      case 'measure': {
        return {
          element: (
            <NamedExpressionsInput<Measure>
              values={effectiveValue ? [effectiveValue] : []}
              onValuesChange={vs => onValueChange(vs[0])}
              singleton
              nonEmpty={required}
              itemMenu={(initMeasure, onClose) => (
                <MeasureMenu
                  columns={columns}
                  measures={measures}
                  initMeasure={initMeasure}
                  onSelectMeasure={onValueChange}
                  onClose={onClose}
                  onAddToSourceQueryAsMeasure={onAddToSourceQueryAsMeasure}
                />
              )}
            />
          ),
          onDropColumn: column => {
            const candidateMeasures = Measure.getPossibleMeasuresForColumn(column).filter(
              p => !effectiveValue || effectiveValue.name !== p.name,
            );
            if (!candidateMeasures.length) return;
            onValueChange(candidateMeasures[0]);
          },
          onDropMeasure: onValueChange,
        };
      }

      case 'measures': {
        const disabledMeasureNames = parameter.allowDuplicates
          ? []
          : filterMap(effectiveValue as Measure[], measure => measure.getAggregateMeasureName());

        return {
          element: (
            <NamedExpressionsInput<Measure>
              values={effectiveValue}
              onValuesChange={onValueChange}
              allowReordering
              nonEmpty={parameter.nonEmpty}
              itemMenu={(initMeasure, onClose) => (
                <MeasureMenu
                  columns={columns}
                  measures={measures}
                  initMeasure={initMeasure}
                  disabledMeasureNames={disabledMeasureNames}
                  onSelectMeasure={m => onValueChange(changeOrAdd(effectiveValue, initMeasure, m))}
                  onClose={onClose}
                  onAddToSourceQueryAsMeasure={onAddToSourceQueryAsMeasure}
                />
              )}
            />
          ),
          onDropColumn: column => {
            const candidateMeasures = Measure.getPossibleMeasuresForColumn(column).filter(
              p => !effectiveValue.some((v: Measure) => v.name === p.name),
            );
            if (!candidateMeasures.length) return;
            onValueChange(effectiveValue.concat(candidateMeasures[0]));
          },
          onDropMeasure: measure => {
            onValueChange(effectiveValue.concat(measure));
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

  const namedParameters = Object.entries(parameters ?? {});

  return (
    <div className="control-pane">
      {namedParameters.map(([name, parameter], i) => {
        const visible = evaluateFunctor(parameter.visible, parameterValues);
        if (visible === false) return;

        const value = parameterValues[name];

        function onValueChange(newValue: unknown) {
          onUpdateParameterValues({ [name]: newValue });
        }

        const { element, onDropColumn, onDropMeasure } = renderOptionsPropInput(
          parameter,
          value,
          onValueChange,
        );

        const description = evaluateFunctor(parameter.description, parameterValues);
        const formGroup = (
          <FormGroupWithInfo
            key={i}
            label={
              evaluateFunctor(parameter.label, parameterValues) || AutoForm.makeLabelName(name)
            }
            info={description && <PopoverText>{description}</PopoverText>}
          >
            {element}
          </FormGroupWithInfo>
        );

        if (!onDropColumn) {
          return formGroup;
        }

        return (
          <DroppableContainer key={i} onDropColumn={onDropColumn} onDropMeasure={onDropMeasure}>
            {formGroup}
          </DroppableContainer>
        );
      })}
    </div>
  );
};
