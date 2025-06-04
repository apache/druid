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
  Icon,
  InputGroup,
  Intent,
  Menu,
  MenuItem,
  Popover,
  SegmentedControl,
  Tag,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import classNames from 'classnames';
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
import { evaluateFunctor, ExpressionMeta, getModuleOptionLabel, Measure } from '../../models';
import { changeOrAdd } from '../../utils';
import { DroppableContainer } from '../droppable-container/droppable-container';

import { ExpressionMenu } from './expression-menu';
import { MeasureMenu } from './measure-menu';
import { NamedExpressionsInput } from './named-expressions-input';
import { OptionsInput } from './options-input';

import './control-pane.scss';

export interface ControlPaneProps {
  querySource: QuerySource | undefined;
  where: SqlExpression;
  onUpdateParameterValues(params: Record<string, unknown>): void;
  parameters: Record<string, ParameterDefinition>;
  parameterValues: Record<string, unknown>;
  compact?: boolean;
  onAddToSourceQueryAsColumn?(expression: SqlExpression): void;
  onAddToSourceQueryAsMeasure?(measure: Measure): void;
}

export const ControlPane = function ControlPane(props: ControlPaneProps) {
  const {
    querySource,
    where,
    onUpdateParameterValues,
    parameters,
    parameterValues,
    compact,
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
    const required = evaluateFunctor(parameter.required, parameterValues, querySource, where);
    switch (parameter.type) {
      case 'boolean': {
        return {
          element: (
            <SegmentedControl
              value={String(value)}
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
              value={value}
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
              value={(value as string) || ''}
              onChange={e => onValueChange(e.target.value)}
              placeholder={parameter.placeholder}
              fill
            />
          ),
        };

      case 'option': {
        const controlOptions =
          evaluateFunctor(parameter.options, parameterValues, querySource, where) || [];
        const selectedOption: OptionValue | undefined = controlOptions.find(o => o === value);
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
                      labelElement={
                        o === selectedOption ? <Icon icon={IconNames.TICK} /> : undefined
                      }
                      onClick={() => onValueChange(o)}
                    />
                  ))}
                </Menu>
              }
            >
              <InputGroup
                value={
                  typeof selectedOption === 'undefined'
                    ? `Unknown option: ${value}`
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
        const controlOptions =
          evaluateFunctor(parameter.options, parameterValues, querySource, where) || [];
        return {
          element: (
            <OptionsInput
              options={controlOptions}
              value={(value as OptionValue[]) || []}
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
              values={value ? [value] : []}
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
          : filterMap(value as ExpressionMeta[], ({ expression }) =>
              expression instanceof SqlColumn ? expression.getName() : undefined,
            );

        return {
          element: (
            <NamedExpressionsInput<ExpressionMeta>
              allowReordering
              values={value as ExpressionMeta[]}
              onValuesChange={onValueChange}
              nonEmpty={parameter.nonEmpty}
              itemMenu={(initExpression, onClose) => (
                <ExpressionMenu
                  columns={columns}
                  initExpression={initExpression}
                  onSelectExpression={c => onValueChange(changeOrAdd(value, initExpression, c))}
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
              value.find((v: ExpressionMeta) => v.name === columnName)
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
            onValueChange(value.concat(ExpressionMeta.fromColumn(column)));
          },
        };
      }

      case 'measure': {
        return {
          element: (
            <NamedExpressionsInput<Measure>
              values={value ? [value] : []}
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
              p => !value || value.name !== p.name,
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
          : filterMap(value as Measure[], measure => measure.getAggregateMeasureName());

        return {
          element: (
            <NamedExpressionsInput<Measure>
              values={value}
              onValuesChange={onValueChange}
              allowReordering
              nonEmpty={parameter.nonEmpty}
              itemMenu={(initMeasure, onClose) => (
                <MeasureMenu
                  columns={columns}
                  measures={measures}
                  initMeasure={initMeasure}
                  disabledMeasureNames={disabledMeasureNames}
                  onSelectMeasure={m => onValueChange(changeOrAdd(value, initMeasure, m))}
                  onClose={onClose}
                  onAddToSourceQueryAsMeasure={onAddToSourceQueryAsMeasure}
                />
              )}
            />
          ),
          onDropColumn: column => {
            const candidateMeasures = Measure.getPossibleMeasuresForColumn(column).filter(
              p => !value.some((v: Measure) => v.name === p.name),
            );
            if (!candidateMeasures.length) return;
            onValueChange(value.concat(candidateMeasures[0]));
          },
          onDropMeasure: measure => {
            onValueChange(value.concat(measure));
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
    <div className={classNames('control-pane', { compact })}>
      {namedParameters.map(([name, parameter], i) => {
        const visible = evaluateFunctor(parameter.visible, parameterValues, querySource, where);
        const defined = evaluateFunctor(parameter.defined, parameterValues, querySource, where);
        if (visible === false || defined === false) return;
        if (compact && !parameter.important) return;

        const value = parameterValues[name];

        function onValueChange(newValue: unknown) {
          onUpdateParameterValues({ [name]: newValue });
        }

        const { element, onDropColumn, onDropMeasure } = renderOptionsPropInput(
          parameter,
          value,
          onValueChange,
        );

        const label =
          evaluateFunctor(parameter.label, parameterValues, querySource, where) ||
          AutoForm.makeLabelName(name);
        const description = compact
          ? undefined
          : evaluateFunctor(parameter.description, parameterValues, querySource, where);
        const formGroup = (
          <FormGroupWithInfo
            key={i}
            label={label}
            inline={compact}
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
