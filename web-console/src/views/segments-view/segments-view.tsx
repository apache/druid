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

import { Button, ButtonGroup, Intent, Label, MenuItem, Switch } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import classNames from 'classnames';
import { SqlExpression, SqlRef } from 'druid-query-toolkit';
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
  SegmentTimeline,
  TableColumnSelector,
  ViewControlBar,
} from '../../components';
import { AsyncActionDialog } from '../../dialogs';
import { SegmentTableActionDialog } from '../../dialogs/segments-table-action-dialog/segment-table-action-dialog';
import { Api } from '../../singletons';
import {
  addFilter,
  booleanCustomTableFilter,
  Capabilities,
  CapabilitiesMode,
  compact,
  deepGet,
  filterMap,
  formatBytes,
  formatInteger,
  getNeedleAndMode,
  LocalStorageBackedVisibility,
  LocalStorageKeys,
  makeBooleanFilter,
  NumberLike,
  queryDruidSql,
  QueryManager,
  QueryState,
  sqlQueryCustomTableFilter,
  STANDARD_TABLE_PAGE_SIZE,
  STANDARD_TABLE_PAGE_SIZE_OPTIONS,
} from '../../utils';
import { BasicAction } from '../../utils/basic-action';

import './segments-view.scss';

const tableColumns: Record<CapabilitiesMode, string[]> = {
  'full': [
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
  visibleColumns: LocalStorageBackedVisibility;
  capabilities: Capabilities;
  groupByInterval: boolean;
}

interface SegmentQueryResultRow {
  datasource: string;
  start: string;
  end: string;
  interval: string;
  segment_id: string;
  version: string;
  time_span: string;
  partitioning: string;
  size: number;
  partition_num: number;
  num_rows: NumberLike;
  num_replicas: number;
  is_available: number;
  is_published: number;
  is_realtime: number;
  is_overshadowed: number;
}

export interface SegmentsViewState {
  segmentsState: QueryState<SegmentQueryResultRow[]>;
  segmentFilter: Filter[];
  segmentTableActionDialogId?: string;
  datasourceTableActionDialogId?: string;
  actions: BasicAction[];
  terminateSegmentId?: string;
  terminateDatasourceId?: string;
  visibleColumns: LocalStorageBackedVisibility;
  groupByInterval: boolean;
  showSegmentTimeline: boolean;
}

export class SegmentsView extends React.PureComponent<SegmentsViewProps, SegmentsViewState> {
  static baseQuery(visibleColumns: LocalStorageBackedVisibility) {
    const columns = compact([
      visibleColumns.shown('Segment ID') && `"segment_id"`,
      visibleColumns.shown('Datasource') && `"datasource"`,
      visibleColumns.shown('Start') && `"start"`,
      visibleColumns.shown('End') && `"end"`,
      visibleColumns.shown('Version') && `"version"`,
      visibleColumns.shown('Time span') &&
        `CASE
  WHEN "start" = '-146136543-09-08T08:23:32.096Z' AND "end" = '146140482-04-24T15:36:27.903Z' THEN 'All'
  WHEN "start" LIKE '%-01-01T00:00:00.000Z' AND "end" LIKE '%-01-01T00:00:00.000Z' THEN 'Year'
  WHEN "start" LIKE '%-01T00:00:00.000Z' AND "end" LIKE '%-01T00:00:00.000Z' THEN 'Month'
  WHEN "start" LIKE '%T00:00:00.000Z' AND "end" LIKE '%T00:00:00.000Z' THEN 'Day'
  WHEN "start" LIKE '%:00:00.000Z' AND "end" LIKE '%:00:00.000Z' THEN 'Hour'
  WHEN "start" LIKE '%:00.000Z' AND "end" LIKE '%:00.000Z' THEN 'Minute'
  ELSE 'Sub minute'
END AS "time_span"`,
      visibleColumns.shown('Partitioning') &&
        `CASE
  WHEN "shard_spec" LIKE '%"type":"numbered"%' THEN 'dynamic'
  WHEN "shard_spec" LIKE '%"type":"hashed"%' THEN 'hashed'
  WHEN "shard_spec" LIKE '%"type":"single"%' THEN 'single_dim'
  WHEN "shard_spec" LIKE '%"type":"none"%' THEN 'none'
  WHEN "shard_spec" LIKE '%"type":"linear"%' THEN 'linear'
  WHEN "shard_spec" LIKE '%"type":"numbered_overwrite"%' THEN 'numbered_overwrite'
  ELSE '-'
END AS "partitioning"`,
      visibleColumns.shown('Partition') && `"partition_num"`,
      visibleColumns.shown('Size') && `"size"`,
      visibleColumns.shown('Num rows') && `"num_rows"`,
      visibleColumns.shown('Replicas') && `"num_replicas"`,
      visibleColumns.shown('Is published') && `"is_published"`,
      visibleColumns.shown('Is available') && `"is_available"`,
      visibleColumns.shown('Is realtime') && `"is_realtime"`,
      visibleColumns.shown('Is overshadowed') && `"is_overshadowed"`,
    ]);

    if (!columns.length) {
      columns.push(`"segment_id"`);
    }

    return `WITH s AS (SELECT\n${columns.join(',\n')}\nFROM sys.segments)`;
  }

  static computeTimeSpan(start: string, end: string): string {
    if (start.endsWith('-01-01T00:00:00.000Z') && end.endsWith('-01-01T00:00:00.000Z')) {
      return 'Year';
    }

    if (start.endsWith('-01T00:00:00.000Z') && end.endsWith('-01T00:00:00.000Z')) {
      return 'Month';
    }

    if (start.endsWith('T00:00:00.000Z') && end.endsWith('T00:00:00.000Z')) {
      return 'Day';
    }

    if (start.endsWith(':00:00.000Z') && end.endsWith(':00:00.000Z')) {
      return 'Hour';
    }

    if (start.endsWith(':00.000Z') && end.endsWith(':00.000Z')) {
      return 'Minute';
    }

    return 'Sub minute';
  }

  private readonly segmentsQueryManager: QueryManager<SegmentsQuery, SegmentQueryResultRow[]>;

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
      visibleColumns: new LocalStorageBackedVisibility(
        LocalStorageKeys.SEGMENT_TABLE_COLUMN_SELECTION,
        ['Time span', 'Partitioning'],
      ),
      groupByInterval: false,
      showSegmentTimeline: false,
    };

    this.segmentsQueryManager = new QueryManager({
      debounceIdle: 500,
      processQuery: async (query: SegmentsQuery, _cancelToken, setIntermediateQuery) => {
        const {
          page,
          pageSize,
          filtered,
          sorted,
          visibleColumns,
          capabilities,
          groupByInterval,
        } = query;

        if (capabilities.hasSql()) {
          const whereParts = filterMap(filtered, (f: Filter) => {
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

          if (groupByInterval) {
            const innerQuery = compact([
              `SELECT "start" || '/' || "end" AS "interval"`,
              `FROM sys.segments`,
              whereClause ? `WHERE ${whereClause}` : undefined,
              `GROUP BY 1`,
              `ORDER BY 1 DESC`,
              `LIMIT ${pageSize}`,
              page ? `OFFSET ${page * pageSize}` : undefined,
            ]).join('\n');

            const intervals: string = (await queryDruidSql({ query: innerQuery }))
              .map(row => `'${row.interval}'`)
              .join(', ');

            queryParts = compact([
              SegmentsView.baseQuery(visibleColumns),
              `SELECT "start" || '/' || "end" AS "interval", *`,
              `FROM s`,
              `WHERE`,
              intervals ? `  ("start" || '/' || "end") IN (${intervals})` : 'FALSE',
              whereClause ? `  AND ${whereClause}` : '',
            ]);

            if (sorted.length) {
              queryParts.push(
                'ORDER BY ' +
                  sorted
                    .map((sort: any) => `${SqlRef.column(sort.id)} ${sort.desc ? 'DESC' : 'ASC'}`)
                    .join(', '),
              );
            }

            queryParts.push(`LIMIT ${pageSize * 1000}`);
          } else {
            queryParts = [SegmentsView.baseQuery(visibleColumns), `SELECT *`, `FROM s`];

            if (whereClause) {
              queryParts.push(`WHERE ${whereClause}`);
            }

            if (sorted.length) {
              queryParts.push(
                'ORDER BY ' +
                  sorted
                    .map((sort: any) => `${SqlRef.column(sort.id)} ${sort.desc ? 'DESC' : 'ASC'}`)
                    .join(', '),
              );
            }

            queryParts.push(`LIMIT ${pageSize}`);

            if (page) {
              queryParts.push(`OFFSET ${page * pageSize}`);
            }
          }
          const sqlQuery = queryParts.join('\n');
          setIntermediateQuery(sqlQuery);
          return await queryDruidSql({ query: sqlQuery });
        } else if (capabilities.hasCoordinatorAccess()) {
          let datasourceList: string[] = (
            await Api.instance.get('/druid/coordinator/v1/metadata/datasources')
          ).data;

          const datasourceFilter = filtered.find(({ id }) => id === 'datasource');
          if (datasourceFilter) {
            datasourceList = datasourceList.filter(datasource =>
              booleanCustomTableFilter(datasourceFilter, datasource),
            );
          }

          if (sorted.length && sorted[0].id === 'datasource') {
            datasourceList.sort(
              sorted[0].desc ? (d1, d2) => d1.localeCompare(d2) : (d1, d2) => d2.localeCompare(d1),
            );
          }

          const maxResults = (page + 1) * pageSize;
          let results: SegmentQueryResultRow[] = [];

          const n = Math.min(datasourceList.length, maxResults);
          for (let i = 0; i < n && results.length < maxResults; i++) {
            const segments = (
              await Api.instance.get(
                `/druid/coordinator/v1/datasources/${Api.encodePath(datasourceList[i])}?full`,
              )
            ).data?.segments;
            if (!Array.isArray(segments)) continue;

            let segmentQueryResultRows: SegmentQueryResultRow[] = segments.map((segment: any) => {
              const [start, end] = segment.interval.split('/');
              return {
                segment_id: segment.identifier,
                datasource: segment.dataSource,
                start,
                end,
                interval: segment.interval,
                version: segment.version,
                time_span: SegmentsView.computeTimeSpan(start, end),
                partitioning: deepGet(segment, 'shardSpec.type') || '-',
                partition_num: deepGet(segment, 'shardSpec.partitionNum') || 0,
                size: segment.size,
                num_rows: -1,
                num_replicas: -1,
                is_available: -1,
                is_published: -1,
                is_realtime: -1,
                is_overshadowed: -1,
              };
            });

            if (filtered.length) {
              segmentQueryResultRows = segmentQueryResultRows.filter((d: SegmentQueryResultRow) => {
                return filtered.every(filter => {
                  return booleanCustomTableFilter(
                    filter,
                    d[filter.id as keyof SegmentQueryResultRow],
                  );
                });
              });
            }

            results = results.concat(segmentQueryResultRows);
          }

          return results.slice(page * pageSize, maxResults);
        } else {
          throw new Error('must have SQL or coordinator access to load this view');
        }
      },
      onStateChange: segmentsState => {
        this.setState({
          segmentsState,
        });
      },
    });
  }

  componentWillUnmount(): void {
    this.segmentsQueryManager.terminate();
  }

  private readonly fetchData = (groupByInterval: boolean, tableState?: TableState) => {
    const { capabilities } = this.props;
    const { visibleColumns } = this.state;
    if (tableState) this.lastTableState = tableState;
    const { page, pageSize, filtered, sorted } = this.lastTableState!;
    this.segmentsQueryManager.runQuery({
      page,
      pageSize,
      filtered,
      sorted,
      visibleColumns,
      capabilities,
      groupByInterval,
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
    const { segmentsState, segmentFilter, visibleColumns, groupByInterval } = this.state;
    const { capabilities } = this.props;

    const segments = segmentsState.data || [];

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

    const hasSql = capabilities.hasSql();

    // Only allow filtering of columns other than datasource if in SQL mode or we are filtering on an exact datasource
    const allowGeneralFilter =
      hasSql ||
      segmentFilter.some(
        filter => filter.id === 'datasource' && getNeedleAndMode(filter).mode === 'exact',
      );

    return (
      <ReactTable
        data={segments}
        pages={10000000} // Dummy, we are hiding the page selector
        loading={segmentsState.loading}
        noDataText={segmentsState.isEmpty() ? 'No segments' : segmentsState.getErrorMessage() || ''}
        manual
        filterable
        filtered={segmentFilter}
        defaultSorted={[hasSql ? { id: 'start', desc: true } : { id: 'datasource', desc: false }]}
        onFilteredChange={filtered => {
          this.setState({ segmentFilter: filtered });
        }}
        onFetchData={tableState => {
          this.fetchData(groupByInterval, tableState);
        }}
        showPageJump={false}
        ofText=""
        pivotBy={groupByInterval ? ['interval'] : []}
        defaultPageSize={STANDARD_TABLE_PAGE_SIZE}
        pageSizeOptions={STANDARD_TABLE_PAGE_SIZE_OPTIONS}
        showPagination={segments.length > STANDARD_TABLE_PAGE_SIZE}
        columns={[
          {
            Header: 'Segment ID',
            show: visibleColumns.shown('Segment ID'),
            accessor: 'segment_id',
            width: 300,
            sortable: hasSql,
            filterable: allowGeneralFilter,
          },
          {
            Header: 'Datasource',
            show: visibleColumns.shown('Datasource'),
            accessor: 'datasource',
            Cell: renderFilterableCell('datasource'),
          },
          {
            Header: 'Interval',
            show: groupByInterval,
            accessor: 'interval',
            width: 120,
            sortable: hasSql,
            defaultSortDesc: true,
            filterable: allowGeneralFilter,
            Cell: renderFilterableCell('interval'),
          },
          {
            Header: 'Start',
            show: visibleColumns.shown('Start'),
            accessor: 'start',
            width: 120,
            sortable: hasSql,
            defaultSortDesc: true,
            filterable: allowGeneralFilter,
            Cell: renderFilterableCell('start'),
          },
          {
            Header: 'End',
            show: visibleColumns.shown('End'),
            accessor: 'end',
            width: 120,
            sortable: hasSql,
            defaultSortDesc: true,
            filterable: allowGeneralFilter,
            Cell: renderFilterableCell('end'),
          },
          {
            Header: 'Version',
            show: visibleColumns.shown('Version'),
            accessor: 'version',
            width: 120,
            sortable: hasSql,
            defaultSortDesc: true,
            filterable: allowGeneralFilter,
          },
          {
            Header: 'Time span',
            show: visibleColumns.shown('Time span'),
            accessor: 'time_span',
            width: 100,
            sortable: hasSql,
            filterable: allowGeneralFilter,
            Cell: renderFilterableCell('time_span'),
          },
          {
            Header: 'Partitioning',
            show: visibleColumns.shown('Partitioning'),
            accessor: 'partitioning',
            width: 100,
            sortable: hasSql,
            filterable: allowGeneralFilter,
            Cell: renderFilterableCell('partitioning'),
          },
          {
            Header: 'Partition',
            show: visibleColumns.shown('Partition'),
            accessor: 'partition_num',
            width: 60,
            filterable: false,
            sortable: hasSql,
          },
          {
            Header: 'Size',
            show: visibleColumns.shown('Size'),
            accessor: 'size',
            filterable: false,
            sortable: hasSql,
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
            show: hasSql && visibleColumns.shown('Num rows'),
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
            show: hasSql && visibleColumns.shown('Replicas'),
            accessor: 'num_replicas',
            width: 60,
            filterable: false,
            defaultSortDesc: true,
          },
          {
            Header: 'Is published',
            show: hasSql && visibleColumns.shown('Is published'),
            id: 'is_published',
            accessor: row => String(Boolean(row.is_published)),
            Filter: makeBooleanFilter(),
          },
          {
            Header: 'Is realtime',
            show: hasSql && visibleColumns.shown('Is realtime'),
            id: 'is_realtime',
            accessor: row => String(Boolean(row.is_realtime)),
            Filter: makeBooleanFilter(),
          },
          {
            Header: 'Is available',
            show: hasSql && visibleColumns.shown('Is available'),
            id: 'is_available',
            accessor: row => String(Boolean(row.is_available)),
            Filter: makeBooleanFilter(),
          },
          {
            Header: 'Is overshadowed',
            show: hasSql && visibleColumns.shown('Is overshadowed'),
            id: 'is_overshadowed',
            accessor: row => String(Boolean(row.is_overshadowed)),
            Filter: makeBooleanFilter(),
          },
          {
            Header: ACTION_COLUMN_LABEL,
            show: capabilities.hasCoordinatorAccess() && visibleColumns.shown(ACTION_COLUMN_LABEL),
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
          this.segmentsQueryManager.rerunLastQuery();
        }}
      >
        <p>{`Are you sure you want to drop segment '${terminateSegmentId}'?`}</p>
        <p>This action is not reversible.</p>
      </AsyncActionDialog>
    );
  }

  renderBulkSegmentsActions() {
    const { goToQuery, capabilities } = this.props;
    const lastSegmentsQuery = this.segmentsQueryManager.getLastIntermediateQuery();

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
      visibleColumns,
      showSegmentTimeline,
    } = this.state;
    const { capabilities } = this.props;
    const { groupByInterval } = this.state;

    return (
      <>
        <div
          className={classNames('segments-view app-view', {
            'show-segment-timeline': showSegmentTimeline,
          })}
        >
          <ViewControlBar label="Segments">
            <RefreshButton
              onRefresh={auto => this.segmentsQueryManager.rerunLastQuery(auto)}
              localStorageKey={LocalStorageKeys.SEGMENTS_REFRESH_RATE}
            />
            <Label>Group by</Label>
            <ButtonGroup>
              <Button
                active={!groupByInterval}
                onClick={() => {
                  this.setState({ groupByInterval: false });
                  this.fetchData(false);
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
            <Switch
              checked={showSegmentTimeline}
              label="Show segment timeline"
              onChange={() => this.setState({ showSegmentTimeline: !showSegmentTimeline })}
              disabled={!capabilities.hasSqlOrCoordinatorAccess()}
            />
            <TableColumnSelector
              columns={tableColumns[capabilities.getMode()]}
              onChange={column =>
                this.setState(prevState => ({
                  visibleColumns: prevState.visibleColumns.toggle(column),
                }))
              }
              onClose={added => {
                if (!added) return;
                this.fetchData(groupByInterval);
              }}
              tableColumnsHidden={visibleColumns.getHiddenColumns()}
            />
          </ViewControlBar>
          {showSegmentTimeline && <SegmentTimeline capabilities={capabilities} />}
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
