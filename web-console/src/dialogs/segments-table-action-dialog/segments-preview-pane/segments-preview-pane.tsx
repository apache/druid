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

import { QueryResult, QueryRunner } from 'druid-query-toolkit';
import React from 'react';

import { Loader, RecordTablePane } from '../../../components';
// import { Loader, RecordTablePane } from '../../../components';
import { useQueryManager } from '../../../hooks/use-query-manager';
import { DruidError } from '../../../utils';

import './segments-preview-pane.scss';

const queryRunner = new QueryRunner({
  inflateDateStrategy: 'none',
});

interface ParsedSegmentId {
  datasource: string;
  interval: string;
  partitionNumber: number;
  version: string;
}

export function parseSegmentId(segmentId: string): ParsedSegmentId {
  const segmentIdParts = segmentId.split('_');
  const tail = Number(segmentIdParts[segmentIdParts.length - 1]);
  let bump = 1;
  let partitionNumber = 0;

  // Check if segmentId includes a partitionNumber
  if (!isNaN(tail)) {
    partitionNumber = tail;
    bump++;
  }

  const version = segmentIdParts[segmentIdParts.length - bump];
  const interval =
    segmentIdParts[segmentIdParts.length - bump - 2] +
    '/' +
    segmentIdParts[segmentIdParts.length - bump - 1];
  const datasource = segmentIdParts.slice(0, segmentIdParts.length - bump - 2).join('_');

  return {
    datasource: datasource,
    version: version,
    interval: interval,
    partitionNumber: partitionNumber,
  };
}

export interface DatasourcePreviewPaneProps {
  segmentId: string;
}

export const SegmentsPreviewPane = React.memo(function DatasourcePreviewPane(
  props: DatasourcePreviewPaneProps,
) {
  const segmentIdParts = parseSegmentId(props.segmentId);

  const [recordState] = useQueryManager<string, QueryResult>({
    initQuery: segmentIdParts.datasource,
    processQuery: async (datasource, cancelToken) => {
      let result: QueryResult;
      try {
        result = await queryRunner.runQuery({
          query: {
            queryType: 'scan',
            dataSource: datasource,
            intervals: {
              type: 'segments',
              segments: [
                {
                  itvl: segmentIdParts.interval,
                  ver: segmentIdParts.version,
                  part: segmentIdParts.partitionNumber,
                },
              ],
            },
            resultFormat: 'compactedList',
            limit: 1001,
            columns: [],
            granularity: 'all',
          },
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
    <div className="segments-preview-pane">
      {recordState.loading && <Loader />}
      {recordState.data && <RecordTablePane queryResult={recordState.data} />}
      {recordState.error && (
        <div className="segments-preview-error">{recordState.error.message}</div>
      )}
    </div>
  );
});
