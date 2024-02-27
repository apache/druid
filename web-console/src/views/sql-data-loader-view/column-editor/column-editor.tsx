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
  FormGroup,
  InputGroup,
  Intent,
  Menu,
  MenuDivider,
  MenuItem,
  Position,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import type { QueryResult } from '@druid-toolkit/query';
import { F, SqlExpression, SqlFunction, SqlType } from '@druid-toolkit/query';
import type { JSX } from 'react';
import React, { useState } from 'react';

import { AppToaster } from '../../../singletons';
import { deepDelete, deleteKeys, tickIcon } from '../../../utils';
import { FlexibleQueryInput } from '../../workbench-view/flexible-query-input/flexible-query-input';

import './column-editor.scss';

function getTargetTypes(isArray: boolean): SqlType[] {
  return isArray
    ? [SqlType.VARCHAR_ARRAY, SqlType.BIGINT_ARRAY, SqlType.DOUBLE_ARRAY]
    : [SqlType.VARCHAR, SqlType.BIGINT, SqlType.DOUBLE];
}

interface CastBreakdown {
  formula: string;
  castType?: SqlType;
  forceMultiValue: boolean;
  outputName: string;
}

function expressionToCastBreakdown(expression: SqlExpression): CastBreakdown {
  const outputName = expression.getOutputName() || '';
  expression = expression.getUnderlyingExpression();

  if (expression instanceof SqlFunction) {
    const asType = expression.getCastType();
    const formula = String(expression.getArg(0));
    if (asType) {
      return {
        formula,
        castType: asType,
        forceMultiValue: false,
        outputName,
      };
    } else if (expression.getEffectiveFunctionName() === 'ARRAY_TO_MV') {
      return {
        formula,
        forceMultiValue: true,
        outputName,
      };
    }
  }

  return {
    formula: String(expression),
    forceMultiValue: false,
    outputName,
  };
}

function castBreakdownToExpression({
  formula,
  castType,
  forceMultiValue,
  outputName,
}: CastBreakdown): SqlExpression {
  let newExpression = SqlExpression.parse(formula);
  const defaultOutputName = newExpression.getOutputName();

  if (castType) {
    newExpression = newExpression.cast(castType);
  } else if (forceMultiValue) {
    newExpression = F('ARRAY_TO_MV', newExpression);
  }

  if (!defaultOutputName && !outputName) {
    throw new Error('Must explicitly define an output name');
  }

  if (newExpression.getOutputName() !== outputName) {
    newExpression = newExpression.as(outputName);
  }

  return newExpression;
}

function castBreakdownsEqual(a: CastBreakdown, b: CastBreakdown): boolean {
  return (
    a.formula === b.formula &&
    String(a.castType) === String(b.castType) &&
    a.forceMultiValue === b.forceMultiValue &&
    a.outputName === b.outputName
  );
}

interface ColumnEditorProps {
  initExpression?: SqlExpression;
  onApply(expression: SqlExpression | undefined): void;
  onCancel(): void;
  dirty(): void;

  queryResult: QueryResult | undefined;
  headerIndex: number;
}

export const ColumnEditor = React.memo(function ColumnEditor(props: ColumnEditorProps) {
  const { initExpression, onApply, onCancel, dirty, queryResult, headerIndex } = props;

  const [initBreakdown] = useState(
    initExpression ? expressionToCastBreakdown(initExpression) : undefined,
  );
  const [currentBreakdown, setCurrentBreakdown] = useState(
    initBreakdown || { formula: '', forceMultiValue: false, outputName: '' },
  );

  let typeButton: JSX.Element | undefined;
  const sqlQuery = queryResult?.sqlQuery;
  if (queryResult && sqlQuery && headerIndex !== -1) {
    const column = queryResult.header[headerIndex];
    const isArray = String(column.nativeType).toUpperCase().startsWith('ARRAY');

    const expression = queryResult.sqlQuery?.getSelectExpressionForIndex(headerIndex);

    if (expression && column.sqlType !== 'TIMESTAMP' && column.sqlType !== 'OTHER') {
      const selectExpression = sqlQuery.getSelectExpressionForIndex(headerIndex);
      const initCastType = initBreakdown?.castType;
      const initForceMultiValue = initBreakdown?.forceMultiValue;
      if (selectExpression) {
        const castToItems = (
          <>
            <MenuDivider title="Cast to..." />
            {getTargetTypes(isArray).map((targetType, i) => (
              <MenuItem
                key={i}
                icon={tickIcon(
                  targetType.equals(currentBreakdown.castType) && !currentBreakdown.forceMultiValue,
                )}
                text={targetType.value}
                label={targetType.getNativeType()}
                onClick={() => {
                  dirty();
                  setCurrentBreakdown({ ...currentBreakdown, castType: targetType });
                }}
              />
            ))}
            {isArray && (
              <>
                <MenuDivider />
                <MenuItem
                  icon={tickIcon(currentBreakdown.forceMultiValue)}
                  text="Make multi-value"
                  onClick={() => {
                    dirty();
                    setCurrentBreakdown({
                      ...deepDelete(currentBreakdown, 'castType'),
                      forceMultiValue: true,
                    });
                  }}
                />
              </>
            )}
          </>
        );

        typeButton = (
          <Popover2
            position={Position.BOTTOM_LEFT}
            minimal
            content={
              initCastType ? (
                <Menu>
                  <MenuItem
                    icon={tickIcon(!currentBreakdown.castType && !currentBreakdown.forceMultiValue)}
                    text="Remove explicit cast"
                    onClick={() => {
                      dirty();
                      setCurrentBreakdown(
                        deleteKeys(currentBreakdown, ['castType', 'forceMultiValue']),
                      );
                    }}
                  />
                  {castToItems}
                </Menu>
              ) : initForceMultiValue ? (
                <Menu>
                  <MenuItem
                    icon={tickIcon(!currentBreakdown.castType && currentBreakdown.forceMultiValue)}
                    text="VARCHAR (multi-value)"
                    onClick={() => {
                      dirty();
                      setCurrentBreakdown({ ...currentBreakdown, forceMultiValue: true });
                    }}
                  />
                  <MenuItem
                    icon={tickIcon(!currentBreakdown.castType && !currentBreakdown.forceMultiValue)}
                    text="Remove multi-value coercion"
                    onClick={() => {
                      dirty();
                      setCurrentBreakdown(
                        deleteKeys(currentBreakdown, ['castType', 'forceMultiValue']),
                      );
                    }}
                  />
                </Menu>
              ) : (
                <Menu>
                  <MenuItem
                    icon={tickIcon(!currentBreakdown.castType && !currentBreakdown.forceMultiValue)}
                    text={`implicit (${column.sqlType})`}
                    onClick={() => {
                      dirty();
                      setCurrentBreakdown(
                        deleteKeys(currentBreakdown, ['castType', 'forceMultiValue']),
                      );
                    }}
                  />
                  {castToItems}
                </Menu>
              )
            }
          >
            <Button
              text={`Type: ${
                currentBreakdown.castType ||
                (currentBreakdown.forceMultiValue
                  ? 'VARCHAR (multi-value)'
                  : `implicit (${column.sqlType})`)
              }`}
              rightIcon={IconNames.CARET_DOWN}
            />
          </Popover2>
        );
      }
    }
  }

  return (
    <div className="column-editor">
      <div className="title">{initExpression ? 'Edit column' : 'Add column'}</div>
      <FormGroup label="Name">
        <InputGroup
          value={currentBreakdown.outputName}
          onChange={e => {
            dirty();
            setCurrentBreakdown({ ...currentBreakdown, outputName: e.target.value });
          }}
        />
      </FormGroup>
      <FormGroup label="SQL expression">
        <FlexibleQueryInput
          showGutter={false}
          placeholder="expression"
          queryString={currentBreakdown.formula}
          onQueryStringChange={formula => {
            dirty();
            setCurrentBreakdown({ ...currentBreakdown, formula });
          }}
          columnMetadata={undefined}
        />
      </FormGroup>
      {typeButton && <FormGroup>{typeButton}</FormGroup>}
      <div className="apply-cancel-buttons">
        {initExpression && (
          <Button
            className="delete"
            icon={IconNames.TRASH}
            intent={Intent.DANGER}
            onClick={() => {
              onApply(undefined);
              onCancel();
            }}
          />
        )}
        <Button text="Cancel" onClick={onCancel} />
        <Button
          text="Apply"
          intent={Intent.PRIMARY}
          disabled={Boolean(initBreakdown && castBreakdownsEqual(initBreakdown, currentBreakdown))}
          onClick={() => {
            let newExpression: SqlExpression;
            try {
              newExpression = castBreakdownToExpression(currentBreakdown);
            } catch (e) {
              AppToaster.show({
                message: e.message,
                intent: Intent.DANGER,
              });
              return;
            }

            onApply(newExpression);
            onCancel();
          }}
        />
      </div>
    </div>
  );
});
