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

import { Button, ButtonGroup, Intent, Label, MenuItem } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { SqlExpression, SqlRef } from 'druid-query-toolkit';
import * as JSONBig from 'json-bigint-native';
import React from 'react';
import ReactTable, { Filter } from 'react-table';

import {
  ACTION_COLUMN_ID,
  ACTION_COLUMN_LABEL,
  ACTION_COLUMN_WIDTH,
  ActionCell,
  BracedText,
  MoreButton,
  RefreshButton,
  TableColumnSelector,
  ViewControlBar,
} from '../../components';
import { AsyncActionDialog } from '../../dialogs';
import { SegmentTableActionDialog } from '../../dialogs/segments-table-action-dialog/segment-table-action-dialog';
import { Api } from '../../singletons';
import {
  addFilter,
  compact,
  deepGet,
  filterMap,
  formatBytes,
  formatInteger,
  LocalStorageKeys,
  makeBooleanFilter,
  queryDruidSql,
  QueryManager,
  QueryState,
  sqlQueryCustomTableFilter,
} from '../../utils';
import { Capabilities, CapabilitiesMode } from '../../utils';
import { BasicAction } from '../../utils/basic-action';
import { LocalStorageBackedArray } from '../../utils/local-storage-backed-array';

import './segments-view.scss';

const tableColumns: Record<CapabilitiesMode, string[]> = {
  full: [
    'Segment ID',
    'Datasource',
    'Start',
    'End',
    'Version',
    'Time span',
    'Partitioning',
    'Partition',
    'Size',
    'Num rows',
    'Replicas',
    'Is published',
    'Is realtime',
    'Is available',
    'Is overshadowed',
    ACTION_COLUMN_LABEL,
  ],
  'no-sql': [
    'Segment ID',
    'Datasource',
    'Start',
    'End',
    'Version',
    'Partition',
    'Size',
    ACTION_COLUMN_LABEL,
  ],
  'no-proxy': [
    'Segment ID',
    'Datasource',
    'Start',
    'End',
    'Version',
    'Partitioning',
    'Partition',
    'Size',
    'Num rows',
    'Replicas',
    'Is published',
    'Is realtime',
    'Is available',
    'Is overshadowed',
  ],
};

export interface SegmentsViewProps {
  goToQuery: (initSql: string) => void;
  datasource: string | undefined;
  onlyUnavailable: boolean | undefined;
  capabilities: Capabilities;
}

interface Sorted {
  id: string;
  desc: boolean;
}

interface TableState {
  page: number;
  pageSize: number;
  filtered: Filter[];
  sorted: Sorted[];
}

interface SegmentsQuery extends TableState {
  groupByInterval: boolean;
}

interface SegmentQueryResultRow {
  datasource: string;
  start: string;
  end: string;
  segment_id: string;
  version: string;
  time_span: string;
  partitioning: string;
  size: number;
  partition_num: number;
  num_rows: number;
  num_replicas: number;
  is_available: number;
  is_published: number;
  is_realtime: number;
  is_overshadowed: number;
}

export interface SegmentsViewState {
  segmentsState: QueryState<SegmentQueryResultRow[]>;
  trimmedSegments?: SegmentQueryResultRow[];
  segmentFilter: Filter[];
  segmentTableActionDialogId?: string;
  datasourceTableActionDialogId?: string;
  actions: BasicAction[];
  terminateSegmentId?: string;
  terminateDatasourceId?: string;
  hiddenColumns: LocalStorageBackedArray<string>;
  groupByInterval: boolean;
}

export class SegmentsView extends React.PureComponent<SegmentsViewProps, SegmentsViewState> {
  static PAGE_SIZE = 25;

  static WITH_QUERY = `WITH s AS (
  SELECT
    "segment_id", "datasource", "start", "end", "size", "version",
    CASE
      WHEN "start" LIKE '%-01-01T00:00:00.000Z' AND "end" LIKE '%-01-01T00:00:00.000Z' THEN 'Year'
      WHEN "start" LIKE '%-01T00:00:00.000Z' AND "end" LIKE '%-01T00:00:00.000Z' THEN 'Month'
      WHEN "start" LIKE '%T00:00:00.000Z' AND "end" LIKE '%T00:00:00.000Z' THEN 'Day'
      WHEN "start" LIKE '%:00:00.000Z' AND "end" LIKE '%:00:00.000Z' THEN 'Hour'
      WHEN "start" LIKE '%:00.000Z' AND "end" LIKE '%:00.000Z' THEN 'Minute'
      ELSE 'Sub minute'
    END AS "time_span",
    CASE
      WHEN "shard_spec" LIKE '%"type":"numbered"%' THEN 'dynamic'
      WHEN "shard_spec" LIKE '%"type":"hashed"%' THEN 'hashed'
      WHEN "shard_spec" LIKE '%"type":"single"%' THEN 'single_dim'
      WHEN "shard_spec" LIKE '%"type":"none"%' THEN 'none'
      WHEN "shard_spec" LIKE '%"type":"linear"%' THEN 'linear'
      WHEN "shard_spec" LIKE '%"type":"numbered_overwrite"%' THEN 'numbered_overwrite'
      ELSE '-'
    END AS "partitioning",
    "partition_num", "num_replicas", "num_rows",
    "is_published", "is_available", "is_realtime", "is_overshadowed"
  FROM sys.segments
)`;

  private segmentsSqlQueryManager: QueryManager<SegmentsQuery, SegmentQueryResultRow[]>;
  private segmentsNoSqlQueryManager: QueryManager<null, SegmentQueryResultRow[]>;

  private lastTableState: TableState | undefined;

  constructor(props: SegmentsViewProps, context: any) {
    super(props, context);

    const segmentFilter: Filter[] = [];
    if (props.datasource) segmentFilter.push({ id: 'datasource', value: `"${props.datasource}"` });
    if (props.onlyUnavailable) segmentFilter.push({ id: 'is_available', value: 'false' });

    this.state = {
      actions: [],
      segmentsState: QueryState.INIT,
      segmentFilter,
      hiddenColumns: new LocalStorageBackedArray<string>(
        LocalStorageKeys.SEGMENT_TABLE_COLUMN_SELECTION,
      ),
      groupByInterval: false,
    };

    this.segmentsSqlQueryManager = new QueryManager({
      debounceIdle: 500,
      processQuery: async (query: SegmentsQuery, _cancelToken, setIntermediateQuery) => {
        const whereParts = filterMap(query.filtered, (f: Filter) => {
          if (f.id.startsWith('is_')) {
            if (f.value === 'all') return;
            return SqlRef.columnWithQuotes(f.id).equal(f.value === 'true' ? 1 : 0);
          } else {
            return sqlQueryCustomTableFilter(f);
          }
        });

        let queryParts: string[];

        let whereClause = '';
        if (whereParts.length) {
          whereClause = SqlExpression.and(...whereParts).toString();
        }

        if (query.groupByInterval) {
          const innerQuery = compact([
            `SELECT "start" || '/' || "end" AS "interval"`,
            `FROM sys.segments`,
            whereClause ? `WHERE ${whereClause}` : undefined,
            `GROUP BY 1`,
            `ORDER BY 1 DESC`,
            `LIMIT ${query.pageSize}`,
            query.page ? `OFFSET ${query.page * query.pageSize}` : undefined,
          ]).join('\n');

          const intervals: string = (await queryDruidSql({ query: innerQuery }))
            .map(row => `'${row.interval}'`)
            .join(', ');

          queryParts = compact([
            SegmentsView.WITH_QUERY,
            `SELECT "start" || '/' || "end" AS "interval", *`,
            `FROM s`,
            `WHERE`,
            intervals ? `  ("start" || '/' || "end") IN (${intervals})` : 'FALSE',
            whereClause ? `  AND ${whereClause}` : '',
          ]);

          if (query.sorted.length) {
            queryParts.push(
              'ORDER BY ' +
                query.sorted
                  .map((sort: any) => `${JSONBig.stringify(sort.id)} ${sort.desc ? 'DESC' : 'ASC'}`)
                  .join(', '),
            );
          }

          queryParts.push(`LIMIT ${query.pageSize * 1000}`);
        } else {
          queryParts = [SegmentsView.WITH_QUERY, `SELECT *`, `FROM s`];

          if (whereClause) {
            queryParts.push(`WHERE ${whereClause}`);
          }

          if (query.sorted.length) {
            queryParts.push(
              'ORDER BY ' +
                query.sorted
                  .map((sort: any) => `${JSONBig.stringify(sort.id)} ${sort.desc ? 'DESC' : 'ASC'}`)
                  .join(', '),
            );
          }

          queryParts.push(`LIMIT ${query.pageSize}`);

          if (query.page) {
            queryParts.push(`OFFSET ${query.page * query.pageSize}`);
          }
        }
        const sqlQuery = queryParts.join('\n');
        setIntermediateQuery(sqlQuery);
        return await queryDruidSql({ query: sqlQuery });
      },
      onStateChange: segmentsState => {
        this.setState({
          segmentsState,
        });
      },
    });

    this.segmentsNoSqlQueryManager = new QueryManager({
      processQuery: async () => {
        const datasourceList = (await Api.instance.get(
          '/druid/coordinator/v1/metadata/datasources',
        )).data;
        const nestedResults: SegmentQueryResultRow[][] = await Promise.all(
          datasourceList.map(async (d: string) => {
            const segments = (await Api.instance.get(
              `/druid/coordinator/v1/datasources/${Api.encodePath(d)}?full`,
            )).data.segments;

            return segments.map(
              (segment: any): SegmentQueryResultRow => {
                return {
                  segment_id: segment.identifier,
                  datasource: segment.dataSource,
                  start: segment.interval.split('/')[0],
                  end: segment.interval.split('/')[1],
                  version: segment.version,
                  time_span: '-',
                  partitioning: '-',
                  partition_num: deepGet(segment, 'shardSpec.partitionNum') || 0,
                  size: segment.size,
                  num_rows: -1,
                  num_replicas: -1,
                  is_available: -1,
                  is_published: -1,
                  is_realtime: -1,
                  is_overshadowed: -1,
                };
              },
            );
          }),
        );

        return nestedResults.flat().sort((d1, d2) => {
          return d2.start.localeCompare(d1.start);
        });
      },
      onStateChange: segmentsState => {
        this.setState({
          trimmedSegments: segmentsState.data
            ? segmentsState.data.slice(0, SegmentsView.PAGE_SIZE)
            : undefined,
          segmentsState,
        });
      },
    });
  }

  componentDidMount(): void {
    const { capabilities } = this.props;
    if (!capabilities.hasSql() && capabilities.hasCoordinatorAccess()) {
      this.segmentsNoSqlQueryManager.runQuery(null);
    }
  }

  componentWillUnmount(): void {
    this.segmentsSqlQueryManager.terminate();
    this.segmentsNoSqlQueryManager.terminate();
  }

  private fetchData = (groupByInterval: boolean, tableState?: TableState) => {
    if (tableState) this.lastTableState = tableState;
    const { page, pageSize, filtered, sorted } = this.lastTableState!;
    this.segmentsSqlQueryManager.runQuery({
      page,
      pageSize,
      filtered,
      sorted,
      groupByInterval: groupByInterval,
    });
  };

  private fetchClientSideData = (tableState?: TableState) => {
    if (tableState) this.lastTableState = tableState;
    const { page, pageSize, filtered, sorted } = this.lastTableState!;

    this.setState(state => {
      const allSegments = state.segmentsState.data;
      if (!allSegments) return {};
      const sortKey = sorted[0].id as keyof SegmentQueryResultRow;
      const sortDesc = sorted[0].desc;

      return {
        trimmedSegments: allSegments
          .filter(d => {
            return filtered.every((f: any) => {
              return String(d[f.id as keyof SegmentQueryResultRow]).includes(f.value);
            });
          })
          .sort((d1, d2) => {
            const v1 = d1[sortKey] as any;
            const v2 = d2[sortKey] as any;
            if (typeof v1 === 'string') {
              return sortDesc ? v2.localeCompare(v1) : v1.localeCompare(v2);
            } else {
              return sortDesc ? v2 - v1 : v1 - v2;
            }
          })
          .slice(page * pageSize, (page + 1) * pageSize),
      };
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
      segmentsState,
      trimmedSegments,
      segmentFilter,
      hiddenColumns,
      groupByInterval,
    } = this.state;
    const { capabilities } = this.props;

    const segments = trimmedSegments || segmentsState.data || [];

    const sizeValues = segments.map(d => formatBytes(d.size)).concat('(realtime)');

    const numRowsValues = segments.map(d => formatInteger(d.num_rows)).concat('(unknown)');

    const renderFilterableCell = (field: string) => {
      return (row: { value: any }) => {
        const value = row.value;
        return (
          <a
            onClick={() => {
              this.setState({
                segmentFilter: addFilter(segmentFilter, field, value),
              });
            }}
          >
            {value}
          </a>
        );
      };
    };

    return (
      <ReactTable
        data={segments}
        pages={10000000} // Dummy, we are hiding the page selector
        loading={segmentsState.loading}
        noDataText={segmentsState.isEmpty() ? 'No segments' : segmentsState.getErrorMessage() || ''}
        manual
        filterable
        filtered={segmentFilter}
        defaultSorted={[{ id: 'start', desc: true }]}
        onFilteredChange={filtered => {
          this.setState({ segmentFilter: filtered });
        }}
        onFetchData={tableState => {
          if (capabilities.hasSql()) {
            this.fetchData(groupByInterval, tableState);
          } else if (capabilities.hasCoordinatorAccess()) {
            this.fetchClientSideData(tableState);
          }
        }}
        showPageJump={false}
        ofText=""
        pivotBy={groupByInterval ? ['interval'] : []}
        columns={[
          {
            Header: 'Segment ID',
            show: hiddenColumns.exists('Segment ID'),
            accessor: 'segment_id',
            width: 300,
          },
          {
            Header: 'Datasource',
            show: hiddenColumns.exists('Datasource'),
            accessor: 'datasource',
            Cell: renderFilterableCell('datasource'),
          },
          {
            Header: 'Interval',
            show: groupByInterval,
            accessor: 'interval',
            width: 120,
            defaultSortDesc: true,
            Cell: renderFilterableCell('interval'),
          },
          {
            Header: 'Start',
            show: hiddenColumns.exists('Start'),
            accessor: 'start',
            width: 120,
            defaultSortDesc: true,
            Cell: renderFilterableCell('start'),
          },
          {
            Header: 'End',
            show: hiddenColumns.exists('End'),
            accessor: 'end',
            defaultSortDesc: true,
            width: 120,
            Cell: renderFilterableCell('end'),
          },
          {
            Header: 'Version',
            show: hiddenColumns.exists('Version'),
            accessor: 'version',
            defaultSortDesc: true,
            width: 120,
          },
          {
            Header: 'Time span',
            show: capabilities.hasSql() && hiddenColumns.exists('Time span'),
            accessor: 'time_span',
            width: 100,
            filterable: true,
            Cell: renderFilterableCell('time_span'),
          },
          {
            Header: 'Partitioning',
            show: capabilities.hasSql() && hiddenColumns.exists('Partitioning'),
            accessor: 'partitioning',
            width: 100,
            filterable: true,
            Cell: renderFilterableCell('partitioning'),
          },
          {
            Header: 'Partition',
            show: hiddenColumns.exists('Partition'),
            accessor: 'partition_num',
            width: 60,
            filterable: false,
          },
          {
            Header: 'Size',
            show: hiddenColumns.exists('Size'),
            accessor: 'size',
            filterable: false,
            defaultSortDesc: true,
            Cell: row => (
              <BracedText
                text={
                  row.value === 0 && row.original.is_realtime === 1
                    ? '(realtime)'
                    : formatBytes(row.value)
                }
                braces={sizeValues}
              />
            ),
          },
          {
            Header: 'Num rows',
            show: capabilities.hasSql() && hiddenColumns.exists('Num rows'),
            accessor: 'num_rows',
            filterable: false,
            defaultSortDesc: true,
            Cell: row => (
              <BracedText
                text={row.original.is_available ? formatInteger(row.value) : '(unknown)'}
                braces={numRowsValues}
              />
            ),
          },
          {
            Header: 'Replicas',
            show: capabilities.hasSql() && hiddenColumns.exists('Replicas'),
            accessor: 'num_replicas',
            width: 60,
            filterable: false,
            defaultSortDesc: true,
          },
          {
            Header: 'Is published',
            show: capabilities.hasSql() && hiddenColumns.exists('Is published'),
            id: 'is_published',
            accessor: row => String(Boolean(row.is_published)),
            Filter: makeBooleanFilter(),
          },
          {
            Header: 'Is realtime',
            show: capabilities.hasSql() && hiddenColumns.exists('Is realtime'),
            id: 'is_realtime',
            accessor: row => String(Boolean(row.is_realtime)),
            Filter: makeBooleanFilter(),
          },
          {
            Header: 'Is available',
            show: capabilities.hasSql() && hiddenColumns.exists('Is available'),
            id: 'is_available',
            accessor: row => String(Boolean(row.is_available)),
            Filter: makeBooleanFilter(),
          },
          {
            Header: 'Is overshadowed',
            show: capabilities.hasSql() && hiddenColumns.exists('Is overshadowed'),
            id: 'is_overshadowed',
            accessor: row => String(Boolean(row.is_overshadowed)),
            Filter: makeBooleanFilter(),
          },
          {
            Header: ACTION_COLUMN_LABEL,
            show: capabilities.hasCoordinatorAccess() && hiddenColumns.exists(ACTION_COLUMN_LABEL),
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
          const resp = await Api.instance.delete(
            `/druid/coordinator/v1/datasources/${Api.encodePath(
              terminateDatasourceId,
            )}/segments/${Api.encodePath(terminateSegmentId)}`,
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
    const { goToQuery, capabilities } = this.props;
    const lastSegmentsQuery = this.segmentsSqlQueryManager.getLastIntermediateQuery();

    return (
      <MoreButton>
        {capabilities.hasSql() && (
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
      </MoreButton>
    );
  }

  render(): JSX.Element {
    const {
      segmentTableActionDialogId,
      datasourceTableActionDialogId,
      actions,
      hiddenColumns,
    } = this.state;
    const { capabilities } = this.props;
    const { groupByInterval } = this.state;

    return (
      <>
        <div className="segments-view app-view">
          <ViewControlBar label="Segments">
            <RefreshButton
              onRefresh={auto =>
                capabilities.hasSql()
                  ? this.segmentsSqlQueryManager.rerunLastQuery(auto)
                  : this.segmentsNoSqlQueryManager.rerunLastQuery(auto)
              }
              localStorageKey={LocalStorageKeys.SEGMENTS_REFRESH_RATE}
            />
            <Label>Group by</Label>
            <ButtonGroup>
              <Button
                active={!groupByInterval}
                onClick={() => {
                  this.setState({ groupByInterval: false });
                  if (capabilities.hasSql()) {
                    this.fetchData(false);
                  } else {
                    this.fetchClientSideData();
                  }
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
              columns={tableColumns[capabilities.getMode()]}
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
        {segmentTableActionDialogId && datasourceTableActionDialogId && (
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
