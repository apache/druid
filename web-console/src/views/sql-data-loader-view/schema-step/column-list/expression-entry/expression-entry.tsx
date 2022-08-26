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

import { Icon } from '@blueprintjs/core';
import classNames from 'classnames';
import { Column, QueryResult, SqlRef } from 'druid-query-toolkit';
import React from 'react';

import { columnToIcon } from '../../../../../utils';

import './expression-entry.scss';

interface ExpressionEntryProps {
  column: Column;
  headerIndex: number;
  queryResult: QueryResult;
  grouped?: boolean;
  selected: boolean;
  onEditColumn(columnIndex: number): void;
}

export const ExpressionEntry = function ExpressionEntry(props: ExpressionEntryProps) {
  const { column, headerIndex, queryResult, grouped, selected, onEditColumn } = props;

  const expression = queryResult.sqlQuery?.getSelectExpressionForIndex(headerIndex);
  if (!expression) return null;

  const icon = columnToIcon(column);
  return (
    <div
      className={
        grouped === false
          ? classNames('expression-entry', 'metric', { selected })
          : classNames(
              'expression-entry',
              column.isTimeColumn() ? 'timestamp' : 'dimension',
              column.sqlType?.toLowerCase(),
              { selected },
            )
      }
      onClick={() => onEditColumn(headerIndex)}
    >
      {icon && <Icon className="type-icon" icon={icon} size={14} />}
      <div className="output-name">
        {expression.getOutputName() || 'EXPR?'}
        <span className="type-name">{` :: ${column.nativeType}`}</span>
      </div>
      {!(expression instanceof SqlRef) && (
        <div className="expression">
          {expression.getUnderlyingExpression().prettify({ keywordCasing: 'preserve' }).toString()}
        </div>
      )}
    </div>
  );
};
