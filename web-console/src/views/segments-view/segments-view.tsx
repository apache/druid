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
  ButtonGroup,
  Intent,
  Label,
  Menu,
  MenuItem,
  Popover,
  Position,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import axios from 'axios';
import React from 'react';
import ReactTable from 'react-table';
import { Filter } from 'react-table';

import {
  ACTION_COLUMN_ID,
  ACTION_COLUMN_LABEL,
  ACTION_COLUMN_WIDTH,
  ActionCell,
  RefreshButton,
  TableColumnSelector,
  ViewControlBar,
} from '../../components';
import { AsyncActionDialog } from '../../dialogs';
import { SegmentTableActionDialog } from '../../dialogs/segments-table-action-dialog/segment-table-action-dialog';
import {
  addFilter,
  compact,
  filterMap,
  formatBytes,
  formatNumber,
  LocalStorageKeys,
  makeBooleanFilter,
  queryDruidSql,
  QueryManager,
  sqlQueryCustomTableFilter,
} from '../../utils';
import { BasicAction } from '../../utils/basic-action';
import { LocalStorageBackedArray } from '../../utils/local-storage-backed-array';

import './segments-view.scss';

const tableColumns: string[] = [
  'Segment ID',
  'Datasource',
  'Start',
  'End',
  'Version',
  'Partition',
  'Size',
  'Num rows',
  'Replicas',
  'Is published',
  'Is realtime',
  'Is available',
  'Is overshadowed',
  ACTION_COLUMN_LABEL,
];
const tableColumnsNoSql: string[] = [
  'Segment ID',
  'Datasource',
  'Start',
  'End',
  'Version',
  'Partition',
  'Size',
];

export interface SegmentsViewProps {
  goToQuery: (initSql: string) => void;
  datasource: string | undefined;
  onlyUnavailable: boolean | undefined;
  noSqlMode: boolean;
}

export interface SegmentsViewState {
  segmentsLoading: boolean;
  segments?: SegmentQueryResultRow[];
  segmentsError?: string;
  segmentFilter: Filter[];
  allSegments?: SegmentQueryResultRow[];
  segmentTableActionDialogId?: string;
  datasourceTableActionDialogId?: string;
  actions: BasicAction[];
  terminateSegmentId?: string;
  terminateDatasourceId?: string;
  hiddenColumns: LocalStorageBackedArray<string>;
  groupByInterval: boolean;

  // table state
  page: number | null;
  pageSize: number | null;
  filtered: number[] | null;
  sorted: Sorted | null;
}

interface Sorted {
  id: number;
  desc: boolean;
}

interface SegmentsQuery {
  page: number;
  pageSize: number;
  filtered: Filter[];
  sorted: Sorted[];
  groupByInterval: boolean;
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

export class SegmentsView extends React.PureComponent<SegmentsViewProps, SegmentsViewState> {
  static PAGE_SIZE = 25;

  private segmentsSqlQueryManager: QueryManager<SegmentsQuery, SegmentQueryResultRow[]>;
  private segmentsNoSqlQueryManager: QueryManager<null, SegmentQueryResultRow[]>;

  constructor(props: SegmentsViewProps, context: any) {
    super(props, context);

    const segmentFilter: Filter[] = [];
    if (props.datasource) segmentFilter.push({ id: 'datasource', value: `"${props.datasource}"` });
    if (props.onlyUnavailable) segmentFilter.push({ id: 'is_available', value: 'false' });

    this.state = {
      actions: [],
      segmentsLoading: true,
      segmentFilter,
      hiddenColumns: new LocalStorageBackedArray<string>(
        LocalStorageKeys.SEGMENT_TABLE_COLUMN_SELECTION,
      ),
      groupByInterval: false,

      // Table state
      page: null,
      pageSize: null,
      sorted: null,
      filtered: null,
    };

    this.segmentsSqlQueryManager = new QueryManager({
      debounceIdle: 500,
      processQuery: async (query: SegmentsQuery, setIntermediateQuery) => {
        const totalQuerySize = (query.page + 1) * query.pageSize;

        const whereParts = filterMap(query.filtered, (f: Filter) => {
          if (f.id.startsWith('is_')) {
            if (f.value === 'all') return;
            return `${JSON.stringify(f.id)} = ${f.value === 'true' ? 1 : 0}`;
          } else {
            return sqlQueryCustomTableFilter(f);
          }
        });

        let queryParts: string[];

        let whereClause = '';
        if (whereParts.length) {
          whereClause = whereParts.join(' AND ');
        }

        if (query.groupByInterval) {
          const innerQuery = compact([
            `SELECT "start" || '/' || "end" AS "interval"`,
            `FROM sys.segments`,
            whereClause ? `WHERE ${whereClause}` : '',
            `GROUP BY 1`,
            `LIMIT ${totalQuerySize}`,
          ]).join('\n');

          const intervals: string = (await queryDruidSql({ query: innerQuery }))
            .map(row => `'${row.interval}'`)
            .join(', ');

          queryParts = compact([
            `SELECT`,
            `  ("start" || '/' || "end") AS "interval",`,
            `  "segment_id", "datasource", "start", "end", "size", "version", "partition_num", "num_replicas", "num_rows", "is_published", "is_available", "is_realtime", "is_overshadowed", "payload"`,
            `FROM sys.segments`,
            `WHERE`,
            `  ("start" || '/' || "end") IN (${intervals})`,
            whereClause ? `  AND ${whereClause}` : '',
          ]);

          if (query.sorted.length) {
            queryParts.push(
              'ORDER BY ' +
                query.sorted
                  .map((sort: any) => `${JSON.stringify(sort.id)} ${sort.desc ? 'DESC' : 'ASC'}`)
                  .join(', '),
            );
          }

          queryParts.push(`LIMIT ${totalQuerySize * 1000}`);
        } else {
          queryParts = [
            `SELECT "segment_id", "datasource", "start", "end", "size", "version", "partition_num", "num_replicas", "num_rows", "is_published", "is_available", "is_realtime", "is_overshadowed", "payload"`,
            `FROM sys.segments`,
          ];

          if (whereClause) {
            queryParts.push(`WHERE ${whereClause}`);
          }

          if (query.sorted.length) {
            queryParts.push(
              'ORDER BY ' +
                query.sorted
                  .map((sort: any) => `${JSON.stringify(sort.id)} ${sort.desc ? 'DESC' : 'ASC'}`)
                  .join(', '),
            );
          }

          queryParts.push(`LIMIT ${totalQuerySize}`);
        }
        const sqlQuery = queryParts.join('\n');
        setIntermediateQuery(sqlQuery);
        const results: any[] = (await queryDruidSql({ query: sqlQuery })).slice(
          query.page * query.pageSize,
        );
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
          segmentsError: error,
        });
      },
    });

    this.segmentsNoSqlQueryManager = new QueryManager({
      processQuery: async () => {
        const datasourceList = (await axios.get('/druid/coordinator/v1/metadata/datasources')).data;
        const nestedResults: SegmentQueryResultRow[][] = await Promise.all(
          datasourceList.map(async (d: string) => {
            const segments = (await axios.get(`/druid/coordinator/v1/datasources/${d}?full`)).data
              .segments;

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
                is_overshadowed: -1,
              };
            });
          }),
        );

        const results: SegmentQueryResultRow[] = nestedResults.flat().sort((d1: any, d2: any) => {
          return d2.start.localeCompare(d1.start);
        });

        return results.slice(0, SegmentsView.PAGE_SIZE);
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          allSegments: result,
          segments: result,
          segmentsLoading: loading,
          segmentsError: error,
        });
      },
    });
  }

  componentDidMount(): void {
    const { noSqlMode } = this.props;
    if (noSqlMode) {
      this.segmentsNoSqlQueryManager.runQuery(null);
    }
  }

  componentWillUnmount(): void {
    this.segmentsSqlQueryManager.terminate();
    this.segmentsNoSqlQueryManager.terminate();
  }

  private fetchData = (groupByInterval: boolean, state?: any) => {
    const { page, pageSize, filtered, sorted } = state ? state : this.state;
    this.segmentsSqlQueryManager.runQuery({
      page,
      pageSize,
      filtered,
      sorted,
      groupByInterval: groupByInterval,
    });
  };

  private fetchClientSideData = (state?: any) => {
    const { page, pageSize, filtered, sorted } = state ? state : state;
    const { allSegments } = this.state;
    if (allSegments == null) return;
    const startPage = page * pageSize;
    const endPage = (page + 1) * pageSize;
    const sortPivot = sorted[0].id;
    const sortDesc = sorted[0].desc;
    const selectedSegments = allSegments
      .sort((d1: any, d2: any) => {
        const v1 = d1[sortPivot];
        const v2 = d2[sortPivot];
        if (typeof d1[sortPivot] === 'string') {
          return sortDesc ? v2.localeCompare(v1) : v1.localeCompare(v2);
        } else {
          return sortDesc ? v2 - v1 : v1 - v2;
        }
      })
      .filter((d: any) => {
        return filtered.every((f: any) => {
          return d[f.id].includes(f.value);
        });
      });
    const segments = selectedSegments.slice(startPage, endPage);
    this.setState({
      segments,
    });
  };

  private getSegmentActions(id: string, datasource: string): BasicAction[] {
    const actions: BasicAction[] = [];
    actions.push({
      icon: IconNames.IMPORT,
      title: 'Drop segment (disable)',
      intent: Intent.DANGER,
      onAction: () => this.setState({ terminateSegmentId: id, terminateDatasourceId: datasource }),
    });
    return actions;
  }

  renderSegmentsTable() {
    const {
      segments,
      segmentsLoading,
      segmentsError,
      segmentFilter,
      hiddenColumns,
      groupByInterval,
    } = this.state;
    const { noSqlMode } = this.props;

    return (
      <ReactTable
        data={segments || []}
        pages={10000000} // Dummy, we are hiding the page selector
        loading={segmentsLoading}
        noDataText={
          !segmentsLoading && segments && !segments.length ? 'No segments' : segmentsError || ''
        }
        manual
        filterable
        filtered={segmentFilter}
        defaultSorted={[{ id: 'start', desc: true }]}
        onFilteredChange={filtered => {
          this.setState({ segmentFilter: filtered });
        }}
        onFetchData={
          noSqlMode
            ? this.fetchClientSideData
            : state => {
                this.setState({
                  page: state.page,
                  pageSize: state.pageSize,
                  filtered: state.filtered,
                  sorted: state.sorted,
                });
                if (this.segmentsSqlQueryManager.getLastQuery) {
                  this.fetchData(groupByInterval, state);
                }
              }
        }
        showPageJump={false}
        ofText=""
        pivotBy={groupByInterval ? ['interval'] : []}
        columns={[
          {
            Header: 'Segment ID',
            accessor: 'segment_id',
            width: 300,
            show: hiddenColumns.exists('Segment ID'),
          },
          {
            Header: 'Datasource',
            accessor: 'datasource',
            Cell: row => {
              const value = row.value;
              return (
                <a
                  onClick={() => {
                    this.setState({ segmentFilter: addFilter(segmentFilter, 'datasource', value) });
                  }}
                >
                  {value}
                </a>
              );
            },
            show: hiddenColumns.exists('Datasource'),
          },
          {
            Header: 'Interval',
            accessor: 'interval',
            width: 120,
            defaultSortDesc: true,
            Cell: row => {
              const value = row.value;
              return (
                <a
                  onClick={() => {
                    this.setState({ segmentFilter: addFilter(segmentFilter, 'interval', value) });
                  }}
                >
                  {value}
                </a>
              );
            },
            show: hiddenColumns.exists('interval') && groupByInterval,
          },
          {
            Header: 'Start',
            accessor: 'start',
            width: 120,
            defaultSortDesc: true,
            Cell: row => {
              const value = row.value;
              return (
                <a
                  onClick={() => {
                    this.setState({ segmentFilter: addFilter(segmentFilter, 'start', value) });
                  }}
                >
                  {value}
                </a>
              );
            },
            show: hiddenColumns.exists('Start'),
          },
          {
            Header: 'End',
            accessor: 'end',
            defaultSortDesc: true,
            width: 120,
            Cell: row => {
              const value = row.value;
              return (
                <a
                  onClick={() => {
                    this.setState({ segmentFilter: addFilter(segmentFilter, 'end', value) });
                  }}
                >
                  {value}
                </a>
              );
            },
            show: hiddenColumns.exists('End'),
          },
          {
            Header: 'Version',
            accessor: 'version',
            defaultSortDesc: true,
            width: 120,
            show: hiddenColumns.exists('Version'),
          },
          {
            Header: 'Partition',
            accessor: 'partition_num',
            width: 60,
            filterable: false,
            show: hiddenColumns.exists('Partition'),
          },
          {
            Header: 'Size',
            accessor: 'size',
            filterable: false,
            defaultSortDesc: true,
            Cell: row => formatBytes(row.value),
            show: hiddenColumns.exists('Size'),
          },
          {
            Header: 'Num rows',
            accessor: 'num_rows',
            filterable: false,
            defaultSortDesc: true,
            Cell: row => (row.original.is_available ? formatNumber(row.value) : <em>(unknown)</em>),
            show: !noSqlMode && hiddenColumns.exists('Num rows'),
          },
          {
            Header: 'Replicas',
            accessor: 'num_replicas',
            width: 60,
            filterable: false,
            defaultSortDesc: true,
            show: !noSqlMode && hiddenColumns.exists('Replicas'),
          },
          {
            Header: 'Is published',
            id: 'is_published',
            accessor: row => String(Boolean(row.is_published)),
            Filter: makeBooleanFilter(),
            show: !noSqlMode && hiddenColumns.exists('Is published'),
          },
          {
            Header: 'Is realtime',
            id: 'is_realtime',
            accessor: row => String(Boolean(row.is_realtime)),
            Filter: makeBooleanFilter(),
            show: !noSqlMode && hiddenColumns.exists('Is realtime'),
          },
          {
            Header: 'Is available',
            id: 'is_available',
            accessor: row => String(Boolean(row.is_available)),
            Filter: makeBooleanFilter(),
            show: !noSqlMode && hiddenColumns.exists('Is available'),
          },
          {
            Header: 'Is overshadowed',
            id: 'is_overshadowed',
            accessor: row => String(Boolean(row.is_overshadowed)),
            Filter: makeBooleanFilter(),
            show: !noSqlMode && hiddenColumns.exists('Is overshadowed'),
          },
          {
            Header: ACTION_COLUMN_LABEL,
            id: ACTION_COLUMN_ID,
            accessor: 'segment_id',
            width: ACTION_COLUMN_WIDTH,
            filterable: false,
            Cell: row => {
              if (row.aggregated) return '';
              const id = row.value;
              const datasource = row.row.datasource;
              return (
                <ActionCell
                  onDetail={() => {
                    this.setState({
                      segmentTableActionDialogId: id,
                      datasourceTableActionDialogId: datasource,
                      actions: this.getSegmentActions(id, datasource),
                    });
                  }}
                  actions={this.getSegmentActions(id, datasource)}
                />
              );
            },
            Aggregated: () => '',
            show: hiddenColumns.exists(ACTION_COLUMN_LABEL),
          },
        ]}
        defaultPageSize={SegmentsView.PAGE_SIZE}
      />
    );
  }

  renderTerminateSegmentAction() {
    const { terminateSegmentId, terminateDatasourceId } = this.state;
    if (!terminateDatasourceId || !terminateSegmentId) return;

    return (
      <AsyncActionDialog
        action={async () => {
          const resp = await axios.delete(
            `/druid/coordinator/v1/datasources/${terminateDatasourceId}/segments/${terminateSegmentId}`,
            {},
          );
          return resp.data;
        }}
        confirmButtonText="Drop Segment"
        successText="Segment drop request acknowledged, next time the coordinator runs segment will be dropped"
        failText="Could not drop segment"
        intent={Intent.DANGER}
        onClose={() => {
          this.setState({ terminateSegmentId: undefined });
        }}
        onSuccess={() => {
          this.segmentsNoSqlQueryManager.rerunLastQuery();
          this.segmentsSqlQueryManager.rerunLastQuery();
        }}
      >
        <p>{`Are you sure you want to drop segment '${terminateSegmentId}'?`}</p>
        <p>This action is not reversible.</p>
      </AsyncActionDialog>
    );
  }

  renderBulkSegmentsActions() {
    const { goToQuery, noSqlMode } = this.props;
    const lastSegmentsQuery = this.segmentsSqlQueryManager.getLastIntermediateQuery();

    const bulkSegmentsActionsMenu = (
      <Menu>
        {!noSqlMode && (
          <MenuItem
            icon={IconNames.APPLICATION}
            text="View SQL query for table"
            disabled={!lastSegmentsQuery}
            onClick={() => {
              if (!lastSegmentsQuery) return;
              goToQuery(lastSegmentsQuery);
            }}
          />
        )}
      </Menu>
    );

    return (
      <>
        <Popover content={bulkSegmentsActionsMenu} position={Position.BOTTOM_LEFT}>
          <Button icon={IconNames.MORE} />
        </Popover>
      </>
    );
  }

  render(): JSX.Element {
    const {
      segmentTableActionDialogId,
      datasourceTableActionDialogId,
      actions,
      hiddenColumns,
    } = this.state;
    const { noSqlMode } = this.props;
    const { groupByInterval } = this.state;

    return (
      <>
        <div className="segments-view app-view">
          <ViewControlBar label="Segments">
            <RefreshButton
              onRefresh={auto =>
                noSqlMode
                  ? this.segmentsNoSqlQueryManager.rerunLastQuery(auto)
                  : this.segmentsSqlQueryManager.rerunLastQuery(auto)
              }
              localStorageKey={LocalStorageKeys.SEGMENTS_REFRESH_RATE}
            />
            <Label>Group by</Label>
            <ButtonGroup>
              <Button
                active={!groupByInterval}
                onClick={() => {
                  this.setState({ groupByInterval: false });
                  noSqlMode ? this.fetchClientSideData() : this.fetchData(false);
                }}
              >
                None
              </Button>
              <Button
                active={groupByInterval}
                onClick={() => {
                  this.setState({ groupByInterval: true });
                  this.fetchData(true);
                }}
              >
                Interval
              </Button>
            </ButtonGroup>
            {this.renderBulkSegmentsActions()}
            <TableColumnSelector
              columns={noSqlMode ? tableColumnsNoSql : tableColumns}
              onChange={column =>
                this.setState(prevState => ({
                  hiddenColumns: prevState.hiddenColumns.toggle(column),
                }))
              }
              tableColumnsHidden={hiddenColumns.storedArray}
            />
          </ViewControlBar>
          {this.renderSegmentsTable()}
        </div>
        {this.renderTerminateSegmentAction()}
        {segmentTableActionDialogId && (
          <SegmentTableActionDialog
            segmentId={segmentTableActionDialogId}
            datasourceId={datasourceTableActionDialogId}
            actions={actions}
            onClose={() => this.setState({ segmentTableActionDialogId: undefined })}
          />
        )}
      </>
    );
  }
}
