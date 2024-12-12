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

import { Button, Menu, MenuDivider, MenuItem, Popover, Position } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { SqlQuery, SqlTable } from 'druid-query-toolkit';
import type { JSX } from 'react';
import React from 'react';

import { useQueryManager } from '../../../../hooks';
import { queryDruidSql } from '../../../../utils';

import './source-pane.scss';

function formatQuerySource(source: SqlQuery | undefined): string | JSX.Element {
  if (!(source instanceof SqlQuery)) return 'No source selected';
  const fromExpressions = source.getFromExpressions();
  if (fromExpressions.length !== 1) return 'Multiple FROM expressions';
  const fromExpression = fromExpressions[0].getUnderlyingExpression();
  if (fromExpression instanceof SqlTable) {
    return fromExpression.getName();
  } else if (fromExpression instanceof SqlQuery) {
    return formatQuerySource(fromExpression);
  } else {
    return 'Complex FROM expression';
  }
}

export interface SourcePaneProps {
  selectedSource: SqlQuery | undefined;
  onSelectTable(tableName: string): void;
  onShowSourceQuery?: () => void;
  fill?: boolean;
  minimal?: boolean;
  disabled?: boolean;
}

export const SourcePane = React.memo(function SourcePane(props: SourcePaneProps) {
  const { selectedSource, onSelectTable, onShowSourceQuery, fill, minimal, disabled } = props;

  const [tables] = useQueryManager<null, string[]>({
    initQuery: null,
    processQuery: async () => {
      const tables = await queryDruidSql<{ TABLE_NAME: string }>({
        query: `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'TABLE'`,
      });

      return tables.map(d => d.TABLE_NAME);
    },
  });

  return (
    <Popover
      className="source-pane"
      disabled={disabled}
      minimal
      position={Position.BOTTOM_LEFT}
      content={
        <Menu className="source-pane-menu">
          {onShowSourceQuery && (
            <MenuItem text="Show source query..." onClick={onShowSourceQuery} />
          )}
          {onShowSourceQuery && <MenuDivider />}
          {tables.loading && <MenuDivider title="Loading..." />}
          {tables.data?.map((table, i) => (
            <MenuItem key={i} text={table} onClick={() => onSelectTable(table)} />
          ))}
          {!tables.data?.length && <MenuItem text="No tables" disabled />}
        </Menu>
      }
    >
      <Button
        icon={IconNames.TH}
        text={formatQuerySource(selectedSource)}
        rightIcon={IconNames.CARET_DOWN}
        fill={fill}
        minimal={minimal}
        disabled={disabled}
      />
    </Popover>
  );
});
