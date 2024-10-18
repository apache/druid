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

import { Button, Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React, { useEffect, useState } from 'react';

import { useQueryManager } from '../../../../hooks';
import { type ColumnMetadata, queryDruidSql } from '../../../../utils';
import { ColumnTree } from '../../../workbench-view/column-tree/column-tree';
import { FlexibleQueryInput } from '../../../workbench-view/flexible-query-input/flexible-query-input';

import './source-query-pane.scss';

export interface SourceQueryPaneProps {
  source: string;
  onSourceChange(newSource: string): void;
  onClose(): void;
}

export const SourceQueryPane = React.memo(function SourceQueryPane(props: SourceQueryPaneProps) {
  const { source, onSourceChange, onClose } = props;
  const [queryString, setQueryString] = useState(source?.toString() || '');

  useEffect(() => {
    if (source) {
      setQueryString(source);
    }
  }, [source]);

  const [columnMetadataState] = useQueryManager({
    initQuery: '',
    processQuery: async () => {
      return await queryDruidSql<ColumnMetadata>({
        query: `SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS`,
      });
    },
  });

  return (
    <div className="source-query-pane">
      {!columnMetadataState.isError() && (
        <ColumnTree
          getParsedQuery={() => undefined}
          columnMetadataLoading={columnMetadataState.loading}
          columnMetadata={columnMetadataState.data}
          onQueryChange={q => setQueryString(String(q))}
        />
      )}
      <FlexibleQueryInput
        queryString={queryString}
        onQueryStringChange={setQueryString}
        columnMetadata={columnMetadataState.data}
      />
      <div className="button-bar">
        <Button
          intent={Intent.PRIMARY}
          icon={IconNames.CARET_RIGHT}
          text="Set source query"
          onClick={() => {
            onSourceChange(queryString);
          }}
        />
        <Button text="Hide" onClick={() => onClose()} />
      </div>
    </div>
  );
});
