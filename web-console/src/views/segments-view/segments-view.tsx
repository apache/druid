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
import { H5 } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import axios from 'axios';
import * as React from 'react';
import ReactTable from 'react-table';
import { Filter } from 'react-table';

import { TableColumnSelector, ViewControlBar } from '../../components';
import {
  addFilter,
  formatBytes,
  formatNumber,
  LocalStorageKeys, makeBooleanFilter,
  parseList,
  queryDruidSql,
  QueryManager,
  sqlQueryCustomTableFilter,
  TableColumnSelectionHandler
} from '../../utils';

import './segments-view.scss';

const tableColumns: string[] = ['Segment ID', 'Datasource', 'Start', 'End', 'Version', 'Partition',
  'Size', 'Num rows', 'Replicas', 'Is published', 'Is realtime', 'Is available', 'Is overshadowed'];
const tableColumnsNoSql: string[] = ['Segment ID', 'Datasource', 'Start', 'End', 'Version', 'Partition', 'Size'];

export interface SegmentsViewProps extends React.Props<any> {
  goToSql: (initSql: string) => void;
  datasource: string | null;
  onlyUnavailable: boolean | null;
  noSqlMode: boolean;
}

export interface SegmentsViewState {
  segmentsLoading: boolean;
  segments: SegmentQueryResultRow[] | null;
  segmentsError: string | null;
  segmentFilter: Filter[];
  allSegments?: SegmentQueryResultRow[] | null;
}

interface QueryAndSkip {
  query: string;
  skip: number;
}

interface SegmentQueryResultRow {
  datasource: string;
  start: string;
  end: string;
  segment_id: string;
  version: string;
  size: 0;
  partition_num: number;
  payload: any;
  num_rows: number;
  num_replicas: number;
  is_available: number;
  is_published: number;
  is_realtime: number;
  is_overshadowed: number;
}

export class SegmentsView extends React.Component<SegmentsViewProps, SegmentsViewState> {
  private segmentsSqlQueryManager: QueryManager<QueryAndSkip, SegmentQueryResultRow[]>;
  private segmentsJsonQueryManager: QueryManager<any, SegmentQueryResultRow[]>;
  private tableColumnSelectionHandler: TableColumnSelectionHandler;

  constructor(props: SegmentsViewProps, context: any) {
    super(props, context);

    const segmentFilter: Filter[] = [];
    if (props.datasource) segmentFilter.push({ id: 'datasource', value: props.datasource });
    if (props.onlyUnavailable) segmentFilter.push({ id: 'is_available', value: 'false' });

    this.state = {
      segmentsLoading: true,
      segments: null,
      segmentsError: null,
      segmentFilter
    };

    this.segmentsSqlQueryManager = new QueryManager({
      processQuery: async (query: QueryAndSkip) => {
        const results: any[] = (await queryDruidSql({ query: query.query })).slice(query.skip);
        results.forEach(result => {
          try {
            result.payload = JSON.parse(result.payload);
          } catch {
            result.payload = {};
          }
        });
        return results;
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          segments: result,
          segmentsLoading: loading,
          segmentsError: error
        });
      }
    });

    this.segmentsJsonQueryManager = new QueryManager({
      processQuery: async (query: any) => {
        const datasourceList = (await axios.get('/druid/coordinator/v1/metadata/datasources')).data;
        const nestedResults: SegmentQueryResultRow[][] = await Promise.all(datasourceList.map(async (d: string) => {
          const segments = (await axios.get(`/druid/coordinator/v1/datasources/${d}?full`)).data.segments;
          return segments.map((segment: any) => {
            return {
              segment_id: segment.identifier,
              datasource: segment.dataSource,
              start: segment.interval.split('/')[0],
              end: segment.interval.split('/')[1],
              version: segment.version,
              partition_num: segment.shardSpec.partitionNum ? 0 : segment.shardSpec.partitionNum,
              size: segment.size,
              payload: segment,
              num_rows: -1,
              num_replicas: -1,
              is_available: -1,
              is_published: -1,
              is_realtime: -1,
              is_overshadowed: -1
            };
          });
        }));
        const results: SegmentQueryResultRow[] = [].concat.apply([], nestedResults).sort((d1: any, d2: any) => {
          return d2.start.localeCompare(d1.start);
        });
        return results;
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          allSegments: result,
          segments: result ? result.slice(0, 50) : null,
          segmentsLoading: loading,
          segmentsError: error
        });
      }
    });

    this.tableColumnSelectionHandler = new TableColumnSelectionHandler(
      LocalStorageKeys.SEGMENT_TABLE_COLUMN_SELECTION, () => this.setState({})
    );
  }

  componentDidMount(): void {
    if (this.props.noSqlMode) {
      this.segmentsJsonQueryManager.runQuery('init');
    }
  }

  componentWillUnmount(): void {
    this.segmentsSqlQueryManager.terminate();
    this.segmentsJsonQueryManager.terminate();
  }

  private fetchData = (state: any, instance: any) => {
    const { page, pageSize, filtered, sorted } = state;
    const totalQuerySize = (page + 1) * pageSize;

    const queryParts = [
      `SELECT "segment_id", "datasource", "start", "end", "size", "version", "partition_num", "num_replicas", "num_rows", "is_published", "is_available", "is_realtime", "is_overshadowed", "payload"`,
      `FROM sys.segments`
    ];

    const whereParts = filtered.map((f: Filter) => {
      if (f.id.startsWith('is_')) {
        if (f.value === 'all') return null;
        return `${JSON.stringify(f.id)} = ${f.value === 'true' ? 1 : 0}`;
      } else {
        return sqlQueryCustomTableFilter(f);
      }
    }).filter(Boolean);

    if (whereParts.length) {
      queryParts.push('WHERE ' + whereParts.join(' AND '));
    }

    if (sorted.length) {
      queryParts.push('ORDER BY ' + sorted.map((sort: any) => `${JSON.stringify(sort.id)} ${sort.desc ? 'DESC' : 'ASC'}`).join(', '));
    }

    queryParts.push(`LIMIT ${totalQuerySize}`);

    const query = queryParts.join('\n');
    this.segmentsSqlQueryManager.runQuery({
      query,
      skip: totalQuerySize - pageSize
    });
  }

  private fecthClientSideData = (state: any, instance: any) => {
    const { page, pageSize, filtered, sorted } = state;
    const { allSegments } = this.state;
    if (allSegments == null) return;
    const startPage = page * pageSize;
    const endPage = (page + 1) * pageSize;
    const sortPivot = sorted[0].id;
    const sortDesc = sorted[0].desc;
    const selectedSegments = allSegments.sort((d1: any, d2: any) => {
      const v1 = d1[sortPivot];
      const v2 = d2[sortPivot];
      if (typeof (d1[sortPivot]) === 'string') {
        return sortDesc ? v2.localeCompare(v1) : v1.localeCompare(v2);
      } else {
        return sortDesc ? v2 - v1 : v1 - v2;
      }
    }).filter((d: any) => {
      return filtered.every((f: any) => {
        return d[f.id].includes(f.value);
      });
    });
    const segments = selectedSegments.slice(startPage, endPage);
    this.setState({
      segments
    });
  }

  renderSegmentsTable() {
    const { segments, segmentsLoading, segmentsError, segmentFilter } = this.state;
    const { noSqlMode } = this.props;
    const { tableColumnSelectionHandler } = this;

    return <ReactTable
      data={segments || []}
      pages={10000000} // Dummy, we are hiding the page selector
      loading={segmentsLoading}
      noDataText={!segmentsLoading && segments && !segments.length ? 'No segments' : (segmentsError || '')}
      manual
      filterable
      filtered={segmentFilter}
      defaultSorted={[{id: 'start', desc: true}]}
      onFilteredChange={(filtered, column) => {
        this.setState({ segmentFilter: filtered });
      }}
      onFetchData={noSqlMode ? this.fecthClientSideData : this.fetchData}
      showPageJump={false}
      ofText=""
      columns={[
        {
          Header: 'Segment ID',
          accessor: 'segment_id',
          width: 300,
          show: tableColumnSelectionHandler.showColumn('Segment ID')
        },
        {
          Header: 'Datasource',
          accessor: 'datasource',
          Cell: row => {
            const value = row.value;
            return <a onClick={() => { this.setState({ segmentFilter: addFilter(segmentFilter, 'datasource', value)}); }}>{value}</a>;
          },
          show: tableColumnSelectionHandler.showColumn('Datasource')
        },
        {
          Header: 'Start',
          accessor: 'start',
          width: 120,
          defaultSortDesc: true,
          Cell: row => {
            const value = row.value;
            return <a onClick={() => { this.setState({ segmentFilter: addFilter(segmentFilter, 'start', value) }); }}>{value}</a>;
          },
          show: tableColumnSelectionHandler.showColumn('Start')
        },
        {
          Header: 'End',
          accessor: 'end',
          defaultSortDesc: true,
          width: 120,
          Cell: row => {
            const value = row.value;
            return <a onClick={() => { this.setState({ segmentFilter: addFilter(segmentFilter, 'end', value) }); }}>{value}</a>;
          },
          show: tableColumnSelectionHandler.showColumn('End')
        },
        {
          Header: 'Version',
          accessor: 'version',
          defaultSortDesc: true,
          width: 120,
          show: tableColumnSelectionHandler.showColumn('Version')
        },
        {
          Header: 'Partition',
          accessor: 'partition_num',
          width: 60,
          filterable: false,
          show: tableColumnSelectionHandler.showColumn('Partition')
        },
        {
          Header: 'Size',
          accessor: 'size',
          filterable: false,
          defaultSortDesc: true,
          Cell: row => formatBytes(row.value),
          show: tableColumnSelectionHandler.showColumn('Size')
        },
        {
          Header: 'Num rows',
          accessor: 'num_rows',
          filterable: false,
          defaultSortDesc: true,
          Cell: row => formatNumber(row.value),
          show: !noSqlMode && tableColumnSelectionHandler.showColumn('Num rows')
        },
        {
          Header: 'Replicas',
          accessor: 'num_replicas',
          width: 60,
          filterable: false,
          defaultSortDesc: true,
          show: !noSqlMode && tableColumnSelectionHandler.showColumn('Replicas')
        },
        {
          Header: 'Is published',
          id: 'is_published',
          accessor: (row) => String(Boolean(row.is_published)),
          Filter: makeBooleanFilter(),
          show: !noSqlMode && tableColumnSelectionHandler.showColumn('Is published')
        },
        {
          Header: 'Is realtime',
          id: 'is_realtime',
          accessor: (row) => String(Boolean(row.is_realtime)),
          Filter: makeBooleanFilter(),
          show: !noSqlMode && tableColumnSelectionHandler.showColumn('Is realtime')
        },
        {
          Header: 'Is available',
          id: 'is_available',
          accessor: (row) => String(Boolean(row.is_available)),
          Filter: makeBooleanFilter(),
          show: !noSqlMode && tableColumnSelectionHandler.showColumn('Is available')
        },
        {
          Header: 'Is overshadowed',
          id: 'is_overshadowed',
          accessor: (row) => String(Boolean(row.is_overshadowed)),
          Filter: makeBooleanFilter(),
          show: !noSqlMode && tableColumnSelectionHandler.showColumn('Is overshadowed')
        }
      ]}
      defaultPageSize={50}
      SubComponent={rowInfo => {
        const { original } = rowInfo;
        const { payload } = rowInfo.original;
        const dimensions = parseList(payload.dimensions);
        const metrics = parseList(payload.metrics);
        return <div className="segment-detail">
          <H5>Segment ID</H5>
          <p>{original.segment_id}</p>
          <H5>{`Dimensions (${dimensions.length})`}</H5>
          <p>{dimensions.join(', ') || 'No dimension'}</p>
          <H5>{`Metrics (${metrics.length})`}</H5>
          <p>{metrics.join(', ') || 'No metrics'}</p>
        </div>;
      }}
    />;
  }

  render() {
    const { goToSql, noSqlMode } = this.props;
    const { tableColumnSelectionHandler } = this;

    return <div className="segments-view app-view">
      <ViewControlBar label="Segments">
        <Button
          icon={IconNames.REFRESH}
          text="Refresh"
          onClick={() => noSqlMode ? this.segmentsJsonQueryManager.rerunLastQuery() : this.segmentsSqlQueryManager.rerunLastQuery()}
        />
        {
          !noSqlMode &&
          <Button
            icon={IconNames.APPLICATION}
            text="Go to SQL"
            hidden={noSqlMode}
            onClick={() => goToSql(this.segmentsSqlQueryManager.getLastQuery().query)}
          />
        }
        <TableColumnSelector
          columns={noSqlMode ? tableColumnsNoSql : tableColumns}
          onChange={(column) => tableColumnSelectionHandler.changeTableColumnSelector(column)}
          tableColumnsHidden={tableColumnSelectionHandler.hiddenColumns}
        />
      </ViewControlBar>
      {this.renderSegmentsTable()}
    </div>;
  }
}
