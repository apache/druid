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

import { QueryResult, QueryRunner, SqlTableRef } from 'druid-query-toolkit';
import React from 'react';

import { Loader, RecordTablePane } from '../../../components';
import { useQueryManager } from '../../../hooks';
import { DruidError } from '../../../utils';

import './datasource-preview-pane.scss';

const queryRunner = new QueryRunner({
  inflateDateStrategy: 'none',
});

export interface DatasourcePreviewPaneProps {
  datasource: string;
}

export const DatasourcePreviewPane = React.memo(function DatasourcePreviewPane(
  props: DatasourcePreviewPaneProps,
) {
  const [recordState] = useQueryManager<string, QueryResult>({
    initQuery: props.datasource,
    processQuery: async (datasource, cancelToken) => {
      let result: QueryResult;
      try {
        result = await queryRunner.runQuery({
          query: `SELECT * FROM ${SqlTableRef.create(datasource)}`,
          extraQueryContext: { sqlOuterLimit: 100 },
          cancelToken,
        });
      } catch (e) {
        throw new DruidError(e);
      }
      return result;
    },
  });

  return (
    <div className="datasource-preview-pane">
      {recordState.loading && <Loader />}
      {recordState.data && <RecordTablePane queryResult={recordState.data} />}
      {recordState.error && (
        <div className="datasource-preview-error">{recordState.error.message}</div>
      )}
    </div>
  );
});
