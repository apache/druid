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

import { Button, FormGroup, InputGroup, Intent, Menu, MenuItem, Position } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import { QueryResult, SqlExpression, SqlFunction } from 'druid-query-toolkit';
import React, { useState } from 'react';

import { AppToaster } from '../../../singletons';
import { tickIcon } from '../../../utils';
import { FlexibleQueryInput } from '../../workbench-view/flexible-query-input/flexible-query-input';

import './column-editor.scss';

const NATIVE_TYPES: (undefined | string)[] = ['', 'string', 'long', 'double'];

interface Breakdown {
  expression: SqlExpression;
  nativeType?: string;
  outputName: string;
}

function breakdownExpression(expression: SqlExpression): Breakdown {
  const outputName = expression.getOutputName() || '';
  expression = expression.getUnderlyingExpression();

  let nativeType: string | undefined;
  if (expression instanceof SqlFunction && expression.getEffectiveFunctionName() === 'CAST') {
    const asType = String(expression.getArgAsString(1)).toUpperCase();
    switch (asType) {
      case 'VARCHAR':
        nativeType = 'string';
        expression = expression.getArg(0)!;
        break;

      case 'BIGINT':
        nativeType = 'long';
        expression = expression.getArg(0)!;
        break;

      case 'DOUBLE':
        nativeType = 'double';
        expression = expression.getArg(0)!;
        break;
    }
  }

  return {
    expression,
    nativeType,
    outputName,
  };
}

function nativeTypeToSqlType(nativeType: string): string {
  switch (nativeType) {
    case 'string':
      return 'VARCHAR';
    case 'long':
      return 'BIGINT';
    case 'double':
      return 'DOUBLE';
    default:
      return 'VARCHAR';
  }
}

interface ColumnEditorProps {
  expression?: SqlExpression;
  onApply(expression: SqlExpression | undefined): void;
  onCancel(): void;
  dirty(): void;

  queryResult: QueryResult | undefined;
  headerIndex: number;
}

export const ColumnEditor = React.memo(function ColumnEditor(props: ColumnEditorProps) {
  const { expression, onApply, onCancel, dirty, queryResult, headerIndex } = props;

  const breakdown = expression ? breakdownExpression(expression) : undefined;

  const [outputName, setOutputName] = useState<string | undefined>();
  const [nativeType, setNativeType] = useState<string | undefined>();
  const [expressionString, setExpressionString] = useState<string | undefined>();

  const effectiveOutputName = outputName ?? (breakdown?.outputName || '');
  const effectiveNativeType = nativeType ?? breakdown?.nativeType;
  const effectiveExpressionString = expressionString ?? (breakdown?.expression.toString() || '');

  let typeButton: JSX.Element | undefined;

  const sqlQuery = queryResult?.sqlQuery;
  if (queryResult && sqlQuery && headerIndex !== -1) {
    const column = queryResult.header[headerIndex];

    const expression = queryResult.sqlQuery?.getSelectExpressionForIndex(headerIndex);

    if (expression && column.sqlType !== 'TIMESTAMP') {
      const implicitText =
        'implicit' + (breakdown?.nativeType ? '' : ` (${String(column.nativeType).toLowerCase()})`);
      const selectExpression = sqlQuery.getSelectExpressionForIndex(headerIndex);
      if (selectExpression) {
        typeButton = (
          <Popover2
            position={Position.BOTTOM_LEFT}
            minimal
            content={
              <Menu>
                {NATIVE_TYPES.map((type, i) => {
                  return (
                    <MenuItem
                      key={i}
                      icon={tickIcon(type === (effectiveNativeType || ''))}
                      text={type || implicitText}
                      onClick={() => setNativeType(type)}
                    />
                  );
                })}
              </Menu>
            }
          >
            <Button
              text={`Type: ${effectiveNativeType || implicitText}`}
              rightIcon={IconNames.CARET_DOWN}
            />
          </Popover2>
        );
      }
    }
  }

  return (
    <div className="column-editor">
      <div className="title">{expression ? 'Edit column' : 'Add column'}</div>
      <FormGroup label="Name">
        <InputGroup
          value={effectiveOutputName}
          onChange={e => {
            if (!outputName) dirty();
            setOutputName(e.target.value);
          }}
        />
      </FormGroup>
      <FormGroup label="SQL expression">
        <FlexibleQueryInput
          autoHeight={false}
          showGutter={false}
          placeholder="expression"
          queryString={effectiveExpressionString}
          onQueryStringChange={f => {
            if (!expressionString) dirty();
            setExpressionString(f);
          }}
          columnMetadata={undefined}
        />
      </FormGroup>
      {typeButton && <FormGroup>{typeButton}</FormGroup>}
      <div className="apply-cancel-buttons">
        {expression && (
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
          disabled={!outputName && !expressionString && typeof nativeType === 'undefined'}
          onClick={() => {
            let newExpression: SqlExpression;
            try {
              newExpression = SqlExpression.parse(effectiveExpressionString);
            } catch (e) {
              AppToaster.show({
                message: `Could not parse SQL expression: ${e.message}`,
                intent: Intent.DANGER,
              });
              return;
            }

            if (nativeType) {
              newExpression = newExpression.cast(nativeTypeToSqlType(nativeType));
            }

            if (newExpression.getOutputName() !== effectiveOutputName) {
              newExpression = newExpression.as(effectiveOutputName);
            }

            onApply(newExpression);
            onCancel();
          }}
        />
      </div>
    </div>
  );
});
