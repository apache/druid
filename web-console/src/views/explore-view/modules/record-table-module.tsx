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

import { C, SqlQuery } from '@druid-toolkit/query';
import React, { useMemo } from 'react';

import { Loader } from '../../../components';
import { useQueryManager } from '../../../hooks';
import type { ColumnHint } from '../../../utils';
import { filterMap } from '../../../utils';
import { calculateInitPageSize, GenericOutputTable } from '../components';
import { ModuleRepository } from '../module-repository/module-repository';

import './record-table-module.scss';

interface RecordTableParameterValues {
  maxRows: number;
  ascending: boolean;
  showTypeIcons: boolean;
  hideNullColumns: boolean;
}

ModuleRepository.registerModule<RecordTableParameterValues>({
  id: 'record-table',
  title: 'Record table',
  parameters: {
    maxRows: {
      type: 'number',
      label: 'Max rows',
      defaultValue: 200,
      min: 1,
      max: 100000,
      required: true,
    },
    ascending: {
      type: 'boolean',
      defaultValue: false,
    },
    showTypeIcons: {
      type: 'boolean',
      defaultValue: true,
    },
    hideNullColumns: {
      type: 'boolean',
      label: 'Hide all null column',
      defaultValue: false,
    },
  },
  component: function RecordTableModule(props) {
    const { stage, querySource, where, setWhere, parameterValues, runSqlQuery } = props;

    const query = useMemo((): string | undefined => {
      return SqlQuery.create(querySource.query)
        .changeWhereExpression(where)
        .changeLimitValue(parameterValues.maxRows)
        .applyIf(
          querySource.columns.some(e => e.name === '__time') && !parameterValues.ascending,
          q => q.changeOrderByExpression(C('__time').toOrderByExpression('DESC')),
          q => q.changeOrderByClause(querySource.query.orderByClause),
        )
        .toString();
    }, [querySource, where, parameterValues]);

    const [resultState] = useQueryManager({
      query: query,
      processQuery: runSqlQuery,
    });

    const resultData = resultState.getSomeData();

    let columnHints: Map<string, ColumnHint> | undefined;
    if (parameterValues.hideNullColumns && resultData) {
      columnHints = new Map<string, ColumnHint>(
        filterMap(resultData.header, (column, i) =>
          resultData.getColumnByIndex(i)?.every(v => v == null)
            ? [column.name, { hidden: true }]
            : undefined,
        ),
      );
    }

    return (
      <div className="record-table-module module">
        {resultState.error ? (
          <div className="error">{resultState.getErrorMessage()}</div>
        ) : resultData ? (
          <GenericOutputTable
            queryResult={resultData}
            columnHints={columnHints}
            showTypeIcons={parameterValues.showTypeIcons}
            onWhereChange={setWhere}
            initPageSize={calculateInitPageSize(stage.height)}
          />
        ) : undefined}
        {resultState.loading && <Loader />}
      </div>
    );
  },
});
