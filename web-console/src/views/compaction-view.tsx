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

import axios from 'axios';
import * as React from 'react';
import ReactTable from "react-table";
import {makeBooleanFilter, QueryManager} from "../utils";
import "./compaction-view.scss";

export interface CompactionViewProps extends React.Props<any> {}

export interface CompactionExtraInfoType {
  compactionTaskSlotRatio: string | number,
  maxCompactionTaskSlots: string | number,
}

export interface CompactionViewState {
  compactionLoading: boolean;
  compaction: any[] | null;
  compactionExtraInfo: CompactionExtraInfoType,
  compactionError: string | null;
}

export class CompactionView extends React.Component<CompactionViewProps, CompactionViewState> {
  private compactionQueryManager: QueryManager<string, any[]>;

  constructor(props: CompactionViewProps, context: any) {
    super(props, context);

    this.state = {
      compactionLoading: true,
      compaction: null,
      compactionExtraInfo: {
        compactionTaskSlotRatio: 0,
        maxCompactionTaskSlots: 0,
      },
      compactionError: null,
    };
  }

  componentDidMount(): void {
    this.compactionQueryManager = new QueryManager({
      processQuery: async (query: string) => {
        const resp = await axios.get("/druid/coordinator/v1/config/compaction/");
        return resp.data;
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          compaction: result ? result.compactionConfigs : [],
          compactionLoading: loading,
          compactionError: error,
          compactionExtraInfo: {
            compactionTaskSlotRatio: result ? result.compactionTaskSlotRatio : 0,
            maxCompactionTaskSlots: result ? result.maxCompactionTaskSlots : 0,
          }
        });
      }
    });
    this.compactionQueryManager.runQuery('');
  }

  componentWillUnmount(): void {
    this.compactionQueryManager.terminate();
  }

  renderCompactionTable() {
    const { compaction, compactionLoading, compactionError } = this.state;

    return <ReactTable
      data={compaction || []}
      loading={compactionLoading}
      noDataText={!compactionLoading && compaction && !compaction.length ? 'No compaction' : (compactionError || '')}
      filterable={true}
      columns={[
        {
          Header: 'DataSource',
          id: 'dataSource',
          width: 300,
          accessor: (row) => String(row.dataSource),
        },
        {
          Header: 'KeepSegmentGranularity',
          id: 'keepSegmentGranularity',
          accessor: (row) => String(row.keepSegmentGranularity),
          Filter: makeBooleanFilter()
        },
        {
          Header: 'TaskPriority',
          id: 'taskPriority',
          accessor: (row) => String(row.taskPriority),
        },
        {
          Header: 'InputSegmentSizeBytes',
          id: 'inputSegmentSizeBytes',
          filterable: false,
          accessor: (row) => String(row.inputSegmentSizeBytes),
        },
        {
          Header: 'TargetCompactionSizeBytes',
          id: 'targetCompactionSizeBytes',
          filterable: false,
          accessor: (row) => String(row.targetCompactionSizeBytes),
        },
        {
          Header: 'MaxRowsPerSegment',
          id: 'maxRowsPerSegment',
          filterable: false,
          accessor: (row) => String(row.maxRowsPerSegment),
        },
        {
          Header: 'MaxNumSegmentsToCompact',
          id: 'maxNumSegmentsToCompact',
          filterable: false,
          accessor: (row) => String(row.maxNumSegmentsToCompact),
        },
        {
          Header: 'SkipOffsetFromLatest',
          id: 'skipOffsetFromLatest',
          accessor: (row) => String(row.skipOffsetFromLatest),
        },
        {
          Header: 'TuningConfig',
          id: 'tuningConfig',
          accessor: (row) => String(row.tuningConfig),
        },
        {
          Header: 'TaskContext',
          id: 'taskContext',
          accessor: (row) => String(row.taskContext),
        }
      ]}
      defaultPageSize={50}
      className="-striped -highlight"
    />;
  }

  render() {
    const { compactionTaskSlotRatio, maxCompactionTaskSlots } = this.state.compactionExtraInfo;

    return <div className="compaction-view app-view">
      <div className="control-bar">
        <div className="control-label">Compaction</div>
        <div className="extra-info">
          <span>compactionTaskSlotRatio(<b>{compactionTaskSlotRatio}</b>)</span>
          <span>maxCompactionTaskSlots(<b>{maxCompactionTaskSlots}</b>)</span>
        </div>
      </div>
      {this.renderCompactionTable()}
    </div>
  }
}
