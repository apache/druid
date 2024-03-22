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

import { Button, ButtonGroup, Code, Intent, Label, MenuItem, Switch } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { C, L, SqlComparison, SqlExpression } from '@druid-toolkit/query';
import classNames from 'classnames';
import * as JSONBig from 'json-bigint-native';
import React from 'react';
import type { Filter } from 'react-table';
import ReactTable from 'react-table';

import {
  ACTION_COLUMN_ID,
  ACTION_COLUMN_LABEL,
  ACTION_COLUMN_WIDTH,
  ActionCell,
  BracedText,
  MoreButton,
  RefreshButton,
  SegmentTimeline,
  TableClickableCell,
  TableColumnSelector,
  TableFilterableCell,
  ViewControlBar,
} from '../../components';
import { AsyncActionDialog } from '../../dialogs';
import { SegmentTableActionDialog } from '../../dialogs/segments-table-action-dialog/segment-table-action-dialog';
import { ShowValueDialog } from '../../dialogs/show-value-dialog/show-value-dialog';
import type { QueryWithContext } from '../../druid-models';
import type { Capabilities, CapabilitiesMode } from '../../helpers';
import {
  booleanCustomTableFilter,
  BooleanFilterInput,
  parseFilterModeAndNeedle,
  sqlQueryCustomTableFilter,
  STANDARD_TABLE_PAGE_SIZE,
  STANDARD_TABLE_PAGE_SIZE_OPTIONS,
} from '../../react-table';
import { Api } from '../../singletons';
import type { NumberLike } from '../../utils';
import {
  compact,
  countBy,
  deepGet,
  filterMap,
  formatBytes,
  formatInteger,
  hasPopoverOpen,
  isNumberLikeNaN,
  LocalStorageBackedVisibility,
  LocalStorageKeys,
  oneOf,
  queryDruidSql,
  QueryManager,
  QueryState,
  twoLines,
} from '../../utils';
import type { BasicAction } from '../../utils/basic-action';

import './segments-view.scss';

const tableColumns: Record<CapabilitiesMode, string[]> = {
  'full': [
    'Segment ID',
    'Datasource',
    'Start',
    'End',
    'Version',
    'Time span',
    'Shard type',
    'Shard spec',
    'Partition',
    'Size',
    'Num rows',
    'Avg. row size',
    'Replicas',
    'Replication factor',
    'Is available',
    'Is active',
    'Is realtime',
    'Is published',
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
    'Shard type',
    'Shard spec',
    'Partition',
    'Size',
    'Num rows',
    'Avg. row size',
    'Replicas',
    'Replication factor',
    'Is available',
    'Is active',
    'Is realtime',
    'Is published',
    'Is overshadowed',
  ],
};

function formatRangeDimensionValue(dimension: any, value: any): string {
  return `${C(String(dimension))}=${L(String(value))}`;
}

interface Sorted {
  id: string;
  desc: boolean;
}

function sortedToOrderByClause(sorted: Sorted[]): string | undefined {
  if (!sorted.length) return;
  return 'ORDER BY ' + sorted.map(sort => `${C(sort.id)} ${sort.desc ? 'DESC' : 'ASC'}`).join(', ');
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
  shard_spec: string;
  partition_num: number;
  size: number;
  num_rows: NumberLike;
  avg_row_size: NumberLike;
  num_replicas: number;
  replication_factor: number;
  is_available: number;
  is_active: number;
  is_realtime: number;
  is_published: number;
  is_overshadowed: number;
}

export interface SegmentsViewProps {
  filters: Filter[];
  onFiltersChange(filters: Filter[]): void;
  goToQuery(queryWithContext: QueryWithContext): void;
  capabilities: Capabilities;
}

export interface SegmentsViewState {
  segmentsState: QueryState<SegmentQueryResultRow[]>;
  segmentTableActionDialogId?: string;
  datasourceTableActionDialogId?: string;
  actions: BasicAction[];
  terminateSegmentId?: string;
  terminateDatasourceId?: string;
  visibleColumns: LocalStorageBackedVisibility;
  groupByInterval: boolean;
  showSegmentTimeline: boolean;
  showFullShardSpec?: string;
}

export class SegmentsView extends React.PureComponent<SegmentsViewProps, SegmentsViewState> {
  static baseQuery(visibleColumns: LocalStorageBackedVisibility) {
    const columns = compact([
      visibleColumns.shown('Segment ID') && `"segment_id"`,
      visibleColumns.shown('Datasource') && `"datasource"`,
      `"start"`,
      `"end"`,
      `"version"`,
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
      (visibleColumns.shown('Shard type') || visibleColumns.shown('Shard spec')) && `"shard_spec"`,
      visibleColumns.shown('Partition') && `"partition_num"`,
      visibleColumns.shown('Size') && `"size"`,
      visibleColumns.shown('Num rows') && `"num_rows"`,
      visibleColumns.shown('Avg. row size') &&
        `CASE WHEN "num_rows" <> 0 THEN ("size" / "num_rows") ELSE 0 END AS "avg_row_size"`,
      visibleColumns.shown('Replicas') && `"num_replicas"`,
      visibleColumns.shown('Replication factor') && `"replication_factor"`,
      visibleColumns.shown('Is available') && `"is_available"`,
      visibleColumns.shown('Is active') && `"is_active"`,
      visibleColumns.shown('Is realtime') && `"is_realtime"`,
      visibleColumns.shown('Is published') && `"is_published"`,
      visibleColumns.shown('Is overshadowed') && `"is_overshadowed"`,
    ]);

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

  constructor(props: SegmentsViewProps) {
    super(props);

    this.state = {
      actions: [],
      segmentsState: QueryState.INIT,
      visibleColumns: new LocalStorageBackedVisibility(
        LocalStorageKeys.SEGMENT_TABLE_COLUMN_SELECTION,
        ['Time span', 'Is published', 'Is overshadowed'],
      ),
      groupByInterval: false,
      showSegmentTimeline: false,
    };

    this.segmentsQueryManager = new QueryManager({
      debounceIdle: 500,
      processQuery: async (query: SegmentsQuery, _cancelToken, setIntermediateQuery) => {
        const { page, pageSize, filtered, sorted, visibleColumns, capabilities, groupByInterval } =
          query;

        if (capabilities.hasSql()) {
          const whereParts = filterMap(filtered, (f: Filter) => {
            if (f.id === 'shard_type') {
              // Special handling for shard_type that needs to be search in the shard_spec
              // Creates filters like `shard_spec LIKE '%"type":"numbered"%'`
              const modeAndNeedle = parseFilterModeAndNeedle(f);
              if (!modeAndNeedle) return;
              const shardSpecColumn = C('shard_spec');
              switch (modeAndNeedle.mode) {
                case '=':
                  return SqlComparison.like(shardSpecColumn, `%"type":"${modeAndNeedle.needle}"%`);

                case '!=':
                  return SqlComparison.notLike(
                    shardSpecColumn,
                    `%"type":"${modeAndNeedle.needle}"%`,
                  );

                default:
                  return SqlComparison.like(shardSpecColumn, `%"type":"${modeAndNeedle.needle}%`);
              }
            } else if (f.id.startsWith('is_')) {
              switch (f.value) {
                case '=false':
                  return C(f.id).equal(0);

                case '=true':
                  return C(f.id).equal(1);

                default:
                  return;
              }
            } else {
              return sqlQueryCustomTableFilter(f);
            }
          });

          let queryParts: string[];

          let filterClause = '';
          if (whereParts.length) {
            filterClause = SqlExpression.and(...whereParts).toString();
          }

          let effectiveSorted = sorted;
          if (!effectiveSorted.find(sort => sort.id === 'version') && effectiveSorted.length) {
            // Ensure there is a sort on version as a tiebreaker
            effectiveSorted = effectiveSorted.concat([
              {
                id: 'version',
                desc: effectiveSorted[0].desc, // Take the first direction if it exists
              },
            ]);
          }

          const base = SegmentsView.baseQuery(visibleColumns);
          const orderByClause = sortedToOrderByClause(effectiveSorted);

          if (groupByInterval) {
            const innerQuery = compact([
              `SELECT "start", "end"`,
              `FROM sys.segments`,
              filterClause ? `WHERE ${filterClause}` : undefined,
              `GROUP BY 1, 2`,
              sortedToOrderByClause(sorted.filter(sort => oneOf(sort.id, 'start', 'end'))) ||
                `ORDER BY 1 DESC`,
              `LIMIT ${pageSize}`,
              page ? `OFFSET ${page * pageSize}` : undefined,
            ]).join('\n');

            const intervals: string = (await queryDruidSql({ query: innerQuery }))
              .map(({ start, end }) => `'${start}/${end}'`)
              .join(', ');

            queryParts = compact([
              base,
              `SELECT "start" || '/' || "end" AS "interval", *`,
              `FROM s`,
              `WHERE`,
              intervals ? `  ("start" || '/' || "end") IN (${intervals})` : 'FALSE',
              filterClause ? `  AND ${filterClause}` : '',
              orderByClause,
              `LIMIT ${pageSize * 1000}`,
            ]);
          } else {
            queryParts = compact([
              base,
              `SELECT *`,
              `FROM s`,
              filterClause ? `WHERE ${filterClause}` : undefined,
              orderByClause,
              `LIMIT ${pageSize}`,
              page ? `OFFSET ${page * pageSize}` : undefined,
            ]);
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
                shard_spec: deepGet(segment, 'shardSpec'),
                partition_num: deepGet(segment, 'shardSpec.partitionNum') || 0,
                size: segment.size,
                num_rows: -1,
                avg_row_size: -1,
                num_replicas: -1,
                replication_factor: -1,
                is_available: -1,
                is_active: -1,
                is_realtime: -1,
                is_published: -1,
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

  private onDetail(segmentId: string, datasource: string): void {
    this.setState({
      segmentTableActionDialogId: segmentId,
      datasourceTableActionDialogId: datasource,
      actions: this.getSegmentActions(segmentId, datasource),
    });
  }

  private renderFilterableCell(field: string, enableComparisons = false) {
    const { filters, onFiltersChange } = this.props;

    return (row: { value: any }) => (
      <TableFilterableCell
        field={field}
        value={row.value}
        filters={filters}
        onFiltersChange={onFiltersChange}
        enableComparisons={enableComparisons}
      >
        {row.value}
      </TableFilterableCell>
    );
  }

  renderSegmentsTable() {
    const { capabilities, filters, onFiltersChange } = this.props;
    const { segmentsState, visibleColumns, groupByInterval } = this.state;

    const segments = segmentsState.data || [];

    const sizeValues = segments.map(d => formatBytes(d.size)).concat('(realtime)');

    const numRowsValues = segments.map(d => formatInteger(d.num_rows)).concat('(unknown)');

    const avgRowSizeValues = segments.map(d => formatInteger(d.avg_row_size));

    const hasSql = capabilities.hasSql();

    // Only allow filtering of columns other than datasource if in SQL mode, or if we are filtering on an exact datasource
    const allowGeneralFilter =
      hasSql ||
      filters.some(
        filter => filter.id === 'datasource' && parseFilterModeAndNeedle(filter)?.mode === '=',
      );

    return (
      <ReactTable
        data={segments}
        pages={10000000} // Dummy, we are hiding the page selector
        loading={segmentsState.loading}
        noDataText={
          segmentsState.isEmpty()
            ? `No segments${filters.length ? ' matching filter' : ''}`
            : segmentsState.getErrorMessage() || ''
        }
        manual
        filterable
        filtered={filters}
        onFilteredChange={onFiltersChange}
        defaultSorted={[hasSql ? { id: 'start', desc: true } : { id: 'datasource', desc: false }]}
        onFetchData={tableState => {
          this.fetchData(groupByInterval, tableState);
        }}
        showPageJump={false}
        ofText=""
        pivotBy={groupByInterval ? ['interval'] : []}
        defaultPageSize={STANDARD_TABLE_PAGE_SIZE}
        pageSizeOptions={STANDARD_TABLE_PAGE_SIZE_OPTIONS}
        showPagination
        columns={[
          {
            Header: 'Segment ID',
            show: visibleColumns.shown('Segment ID'),
            accessor: 'segment_id',
            width: 280,
            sortable: hasSql,
            filterable: allowGeneralFilter,
            Cell: row => (
              <TableClickableCell
                onClick={() => this.onDetail(row.value, row.row.datasource)}
                hoverIcon={IconNames.SEARCH_TEMPLATE}
              >
                {row.value}
              </TableClickableCell>
            ),
          },
          {
            Header: 'Datasource',
            show: visibleColumns.shown('Datasource'),
            accessor: 'datasource',
            width: 140,
            Cell: this.renderFilterableCell('datasource'),
          },
          {
            Header: 'Interval',
            show: groupByInterval,
            accessor: 'interval',
            width: 120,
            sortable: hasSql,
            defaultSortDesc: true,
            filterable: allowGeneralFilter,
            Cell: this.renderFilterableCell('interval'),
          },
          {
            Header: 'Start',
            show: visibleColumns.shown('Start'),
            accessor: 'start',
            headerClassName: 'enable-comparisons',
            width: 160,
            sortable: hasSql,
            defaultSortDesc: true,
            filterable: allowGeneralFilter,
            Cell: this.renderFilterableCell('start', true),
          },
          {
            Header: 'End',
            show: visibleColumns.shown('End'),
            accessor: 'end',
            headerClassName: 'enable-comparisons',
            width: 160,
            sortable: hasSql,
            defaultSortDesc: true,
            filterable: allowGeneralFilter,
            Cell: this.renderFilterableCell('end', true),
          },
          {
            Header: 'Version',
            show: visibleColumns.shown('Version'),
            accessor: 'version',
            width: 160,
            sortable: hasSql,
            defaultSortDesc: true,
            filterable: allowGeneralFilter,
            Cell: this.renderFilterableCell('version'),
          },
          {
            Header: 'Time span',
            show: visibleColumns.shown('Time span'),
            accessor: 'time_span',
            width: 100,
            sortable: hasSql,
            filterable: allowGeneralFilter,
            Cell: this.renderFilterableCell('time_span'),
          },
          {
            Header: 'Shard type',
            show: visibleColumns.shown('Shard type'),
            id: 'shard_type',
            width: 100,
            sortable: false,
            accessor: d => {
              let v: any;
              try {
                v = JSONBig.parse(d.shard_spec);
              } catch {}

              if (typeof v?.type !== 'string') return '-';
              return v?.type;
            },
            Cell: this.renderFilterableCell('shard_type', true),
          },
          {
            Header: 'Shard spec',
            show: visibleColumns.shown('Shard spec'),
            id: 'shard_spec',
            accessor: 'shard_spec',
            width: 400,
            sortable: false,
            filterable: false,
            Cell: ({ value }) => {
              let v: any;
              try {
                v = JSONBig.parse(value);
              } catch {}

              const onShowFullShardSpec = () => {
                this.setState({
                  showFullShardSpec:
                    v && typeof v === 'object' ? JSONBig.stringify(v, undefined, 2) : String(value),
                });
              };

              switch (v?.type) {
                case 'range': {
                  const dimensions: string[] = v.dimensions || [];
                  const formatEdge = (values: string[]) =>
                    dimensions.map((d, i) => formatRangeDimensionValue(d, values[i])).join('; ');

                  return (
                    <TableClickableCell
                      className="range-detail"
                      onClick={onShowFullShardSpec}
                      hoverIcon={IconNames.EYE_OPEN}
                    >
                      <span className="range-label">Start:</span>
                      {Array.isArray(v.start) ? formatEdge(v.start) : '-∞'}
                      <br />
                      <span className="range-label">End:</span>
                      {Array.isArray(v.end) ? formatEdge(v.end) : '∞'}
                    </TableClickableCell>
                  );
                }

                case 'single': {
                  return (
                    <TableClickableCell
                      className="range-detail"
                      onClick={onShowFullShardSpec}
                      hoverIcon={IconNames.EYE_OPEN}
                    >
                      <span className="range-label">Start:</span>
                      {v.start != null ? formatRangeDimensionValue(v.dimension, v.start) : '-∞'}
                      <br />
                      <span className="range-label">End:</span>
                      {v.end != null ? formatRangeDimensionValue(v.dimension, v.end) : '∞'}
                    </TableClickableCell>
                  );
                }

                case 'hashed': {
                  const { partitionDimensions } = v;
                  if (!Array.isArray(partitionDimensions)) return value;
                  return (
                    <TableClickableCell
                      onClick={onShowFullShardSpec}
                      hoverIcon={IconNames.EYE_OPEN}
                    >
                      {`hash(${
                        partitionDimensions.length
                          ? partitionDimensions.join(', ')
                          : '<all dimensions>'
                      })`}
                    </TableClickableCell>
                  );
                }

                case 'numbered':
                case 'none':
                case 'tombstone':
                  return (
                    <TableClickableCell
                      onClick={onShowFullShardSpec}
                      hoverIcon={IconNames.EYE_OPEN}
                    >
                      No detail
                    </TableClickableCell>
                  );

                default:
                  return (
                    <TableClickableCell
                      onClick={onShowFullShardSpec}
                      hoverIcon={IconNames.EYE_OPEN}
                    >
                      {String(value)}
                    </TableClickableCell>
                  );
              }
            },
            Aggregated: opt => {
              const { subRows } = opt;
              const previewValues = filterMap(subRows, row => row['shard_spec'].type);
              const previewCount = countBy(previewValues);
              return (
                <div className="default-aggregated">
                  {Object.keys(previewCount)
                    .sort()
                    .map(v => `${v} (${previewCount[v]})`)
                    .join(', ')}
                </div>
              );
            },
          },
          {
            Header: 'Partition',
            show: visibleColumns.shown('Partition'),
            accessor: 'partition_num',
            width: 60,
            filterable: false,
            sortable: hasSql,
            className: 'padded',
          },
          {
            Header: 'Size',
            show: visibleColumns.shown('Size'),
            accessor: 'size',
            filterable: false,
            sortable: hasSql,
            defaultSortDesc: true,
            width: 120,
            className: 'padded',
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
            width: 120,
            className: 'padded',
            Cell: row => (
              <BracedText
                text={row.original.is_available ? formatInteger(row.value) : '(unknown)'}
                braces={numRowsValues}
                unselectableThousandsSeparator
              />
            ),
          },
          {
            Header: twoLines('Avg. row size', <i>(bytes)</i>),
            show: capabilities.hasSql() && visibleColumns.shown('Avg. row size'),
            accessor: 'avg_row_size',
            filterable: false,
            width: 100,
            className: 'padded',
            Cell: ({ value }) => {
              if (isNumberLikeNaN(value)) return '-';
              return (
                <BracedText
                  text={formatInteger(value)}
                  braces={avgRowSizeValues}
                  unselectableThousandsSeparator
                />
              );
            },
          },
          {
            Header: twoLines('Replicas', <i>(actual)</i>),
            show: hasSql && visibleColumns.shown('Replicas'),
            accessor: 'num_replicas',
            width: 80,
            filterable: false,
            defaultSortDesc: true,
            className: 'padded',
          },
          {
            Header: twoLines('Replication factor', <i>(desired)</i>),
            show: hasSql && visibleColumns.shown('Replication factor'),
            accessor: 'replication_factor',
            width: 80,
            filterable: false,
            defaultSortDesc: true,
            className: 'padded',
          },
          {
            Header: 'Is available',
            show: hasSql && visibleColumns.shown('Is available'),
            id: 'is_available',
            accessor: row => String(Boolean(row.is_available)),
            Filter: BooleanFilterInput,
            className: 'padded',
            width: 100,
          },
          {
            Header: 'Is active',
            show: hasSql && visibleColumns.shown('Is active'),
            id: 'is_active',
            accessor: row => String(Boolean(row.is_active)),
            Filter: BooleanFilterInput,
            className: 'padded',
            width: 100,
          },
          {
            Header: 'Is realtime',
            show: hasSql && visibleColumns.shown('Is realtime'),
            id: 'is_realtime',
            accessor: row => String(Boolean(row.is_realtime)),
            Filter: BooleanFilterInput,
            className: 'padded',
            width: 100,
          },
          {
            Header: 'Is published',
            show: hasSql && visibleColumns.shown('Is published'),
            id: 'is_published',
            accessor: row => String(Boolean(row.is_published)),
            Filter: BooleanFilterInput,
            className: 'padded',
            width: 100,
          },
          {
            Header: 'Is overshadowed',
            show: hasSql && visibleColumns.shown('Is overshadowed'),
            id: 'is_overshadowed',
            accessor: row => String(Boolean(row.is_overshadowed)),
            Filter: BooleanFilterInput,
            className: 'padded',
            width: 100,
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
                    this.onDetail(id, datasource);
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
        <p>
          Are you sure you want to drop segment <Code>{terminateSegmentId}</Code>?
        </p>
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
              goToQuery({ queryString: lastSegmentsQuery });
            }}
          />
        )}
      </MoreButton>
    );
  }

  render() {
    const {
      segmentTableActionDialogId,
      datasourceTableActionDialogId,
      actions,
      visibleColumns,
      showSegmentTimeline,
      showFullShardSpec,
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
              onRefresh={auto => {
                if (auto && hasPopoverOpen()) return;
                this.segmentsQueryManager.rerunLastQuery(auto);
              }}
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
        {showFullShardSpec && (
          <ShowValueDialog
            title="Full shard spec"
            str={showFullShardSpec}
            onClose={() => this.setState({ showFullShardSpec: undefined })}
          />
        )}
      </>
    );
  }
}
