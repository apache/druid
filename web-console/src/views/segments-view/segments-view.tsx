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

import { Button, ButtonGroup, Intent, Label, MenuItem, Switch, Tag } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { C, L, SqlComparison, SqlExpression } from 'druid-query-toolkit';
import * as JSONBig from 'json-bigint-native';
import type { ReactNode } from 'react';
import React from 'react';
import type { Filter, SortingRule } from 'react-table';
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
  SplitterLayout,
  TableClickableCell,
  TableColumnSelector,
  type TableColumnSelectorColumn,
  TableFilterableCell,
  ViewControlBar,
} from '../../components';
import { AsyncActionDialog } from '../../dialogs';
import { SegmentTableActionDialog } from '../../dialogs/segments-table-action-dialog/segment-table-action-dialog';
import { ShowValueDialog } from '../../dialogs/show-value-dialog/show-value-dialog';
import type { QueryContext, QueryWithContext, ShardSpec } from '../../druid-models';
import { computeSegmentTimeSpan, getDatasourceColor } from '../../druid-models';
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
import type { AuxiliaryQueryFn, NumberLike, TableState } from '../../utils';
import {
  applySorting,
  assemble,
  compact,
  countBy,
  filterMap,
  findMap,
  formatBytes,
  formatInteger,
  getApiArray,
  hasOverlayOpen,
  isNumberLikeNaN,
  LocalStorageBackedVisibility,
  LocalStorageKeys,
  oneOf,
  queryDruidSql,
  QueryManager,
  QueryState,
  ResultWithAuxiliaryWork,
  sortedToOrderByClause,
  twoLines,
} from '../../utils';
import type { BasicAction } from '../../utils/basic-action';

import './segments-view.scss';

const TABLE_COLUMNS_BY_MODE: Record<CapabilitiesMode, TableColumnSelectorColumn[]> = {
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
  ],
  'no-proxy': [
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
  ],
  'no-sql': [
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
    'Replication factor',
    'Is realtime',
    'Is overshadowed',
  ],
};

function maybeParseJsonBig(str: string): any {
  try {
    return JSONBig.parse(str);
  } catch {
    return undefined;
  }
}

function formatRangeDimensionValue(dimension: any, value: any): string {
  return `${C(String(dimension))}=${L(String(value))}`;
}

function segmentFiltersToExpression(filters: Filter[]): SqlExpression {
  return SqlExpression.and(
    ...filterMap(filters, filter => {
      if (filter.id === 'shard_type') {
        // Special handling for shard_type that needs to be searched for in the shard_spec
        // Creates filters like `shard_spec LIKE '%"type":"numbered"%'`
        const modeAndNeedle = parseFilterModeAndNeedle(filter);
        if (!modeAndNeedle) return;
        const shardSpecColumn = C('shard_spec');
        switch (modeAndNeedle.mode) {
          case '=':
            return SqlComparison.like(shardSpecColumn, `%"type":"${modeAndNeedle.needle}"%`);

          case '!=':
            return SqlComparison.notLike(shardSpecColumn, `%"type":"${modeAndNeedle.needle}"%`);

          default:
            return SqlComparison.like(shardSpecColumn, `%"type":"${modeAndNeedle.needle}%`);
        }
      } else if (filter.id.startsWith('is_')) {
        switch (filter.value) {
          case '=false':
            return C(filter.id).equal(0);

          case '=true':
            return C(filter.id).equal(1);

          default:
            return;
        }
      } else {
        return sqlQueryCustomTableFilter(filter);
      }
    }),
  );
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
  shard_spec: ShardSpec;
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

interface SegmentsWithAuxiliaryInfo {
  readonly segments: SegmentQueryResultRow[];
  readonly count: number;
}

export interface SegmentsViewProps {
  filters: Filter[];
  onFiltersChange(filters: Filter[]): void;
  goToQuery(queryWithContext: QueryWithContext): void;
  capabilities: Capabilities;
}

export interface SegmentsViewState {
  segmentsState: QueryState<SegmentsWithAuxiliaryInfo>;
  segmentTableActionDialogId?: string;
  datasourceTableActionDialogId?: string;
  actions: BasicAction[];

  visibleColumns: LocalStorageBackedVisibility;
  groupByInterval: boolean;
  showSegmentTimeline?: { capabilities: Capabilities; datasource?: string };
  page: number;
  pageSize: number;
  sorted: SortingRule[];

  terminateSegmentId?: string;
  terminateDatasourceId?: string;
  showFullShardSpec?: string;
}

export class SegmentsView extends React.PureComponent<SegmentsViewProps, SegmentsViewState> {
  static baseQuery(visibleColumns: LocalStorageBackedVisibility) {
    const columns = compact([
      `"segment_id"`,
      visibleColumns.shown('Datasource') && `"datasource"`,
      `"start"`,
      `"end"`,
      `"version"`,
      visibleColumns.shown('Shard type', 'Shard spec') && `"shard_spec"`,
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

  private readonly segmentsQueryManager: QueryManager<SegmentsQuery, SegmentsWithAuxiliaryInfo>;

  constructor(props: SegmentsViewProps) {
    super(props);

    this.state = {
      actions: [],
      segmentsState: QueryState.INIT,
      visibleColumns: new LocalStorageBackedVisibility(
        LocalStorageKeys.SEGMENT_TABLE_COLUMN_SELECTION,
        ['Is published', 'Is overshadowed'],
      ),
      groupByInterval: false,
      page: 0,
      pageSize: STANDARD_TABLE_PAGE_SIZE,
      sorted: [
        props.capabilities.hasSql()
          ? { id: 'start', desc: true }
          : { id: 'datasource', desc: false },
      ],
    };

    this.segmentsQueryManager = new QueryManager({
      debounceIdle: 500,
      processQuery: async (query: SegmentsQuery, cancelToken, setIntermediateQuery) => {
        const { page, pageSize, filtered, sorted, visibleColumns, capabilities, groupByInterval } =
          query;

        let segments: SegmentQueryResultRow[];
        let count = -1;
        const auxiliaryQueries: AuxiliaryQueryFn<SegmentsWithAuxiliaryInfo>[] = [];

        if (capabilities.hasSql()) {
          const whereExpression = segmentFiltersToExpression(filtered);

          let filterClause = '';
          if (whereExpression.toString() !== 'TRUE') {
            filterClause = whereExpression.toString();
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

          let queryParts: string[];
          const sqlQueryContext: QueryContext = {};
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

            // This is needed because there might be an IN filter with {pageSize} intervals, the number of which exceeds the default inFunctionThreshold, set it to something greater than the {pageSize}
            sqlQueryContext.inFunctionThreshold = pageSize + 1;
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
          let result = await queryDruidSql(
            { query: sqlQuery, context: sqlQueryContext },
            cancelToken,
          );

          if (visibleColumns.shown('Shard type', 'Shard spec')) {
            result = result.map(sr => ({
              ...sr,
              shard_spec: maybeParseJsonBig(sr.shard_spec),
            }));
          }

          segments = result as SegmentQueryResultRow[];

          auxiliaryQueries.push(async (segmentsWithAuxiliaryInfo, cancelToken) => {
            const sqlQuery = assemble(
              'SELECT COUNT(*) AS "cnt"',
              'FROM "sys"."segments"',
              filterClause ? `WHERE ${filterClause}` : undefined,
            ).join('\n');
            const cnt: any = (
              await queryDruidSql<{ cnt: number }>(
                {
                  query: sqlQuery,
                },
                cancelToken,
              )
            )[0].cnt;
            return {
              ...segmentsWithAuxiliaryInfo,
              count: typeof cnt === 'number' ? cnt : -1,
            };
          });
        } else if (capabilities.hasCoordinatorAccess()) {
          let datasourceList: string[] = [];
          const datasourceFilter = filtered.find(({ id }) => id === 'datasource');
          if (datasourceFilter) {
            datasourceList = (
              await getApiArray('/druid/coordinator/v1/metadata/datasources', cancelToken)
            ).filter((datasource: string) =>
              booleanCustomTableFilter(datasourceFilter, datasource),
            );
          }

          let results = (
            await getApiArray(
              `/druid/coordinator/v1/metadata/segments?includeOvershadowedStatus&includeRealtimeSegments${datasourceList
                .map(d => `&datasources=${Api.encodePath(d)}`)
                .join('')}`,
              cancelToken,
            )
          ).map((segment: any) => {
            const [start, end] = segment.interval.split('/');
            return {
              segment_id: segment.identifier,
              datasource: segment.dataSource,
              start,
              end,
              interval: segment.interval,
              version: segment.version,
              shard_spec: segment.shardSpec,
              partition_num: segment.shardSpec?.partitionNum || 0,
              size: segment.size,
              num_rows: -1,
              avg_row_size: -1,
              num_replicas: -1,
              replication_factor: segment.replicationFactor,
              is_available: -1,
              is_active: -1,
              is_realtime: Number(segment.realtime),
              is_published: -1,
              is_overshadowed: Number(segment.overshadowed),
            };
          });

          if (filtered.length) {
            results = results.filter((d: SegmentQueryResultRow) => {
              return filtered.every(filter => {
                return booleanCustomTableFilter(
                  filter,
                  d[filter.id as keyof SegmentQueryResultRow],
                );
              });
            });
          }

          count = results.length;
          const maxResults = (page + 1) * pageSize;
          segments = applySorting(results, sorted).slice(page * pageSize, maxResults);
        } else {
          throw new Error('must have SQL or coordinator access to load this view');
        }

        return new ResultWithAuxiliaryWork<SegmentsWithAuxiliaryInfo>(
          { segments, count },
          auxiliaryQueries,
        );
      },
      onStateChange: segmentsState => {
        this.setState({
          segmentsState,
        });
      },
    });
  }

  componentDidMount() {
    this.fetchData();
  }

  componentWillUnmount(): void {
    this.segmentsQueryManager.terminate();
  }

  componentDidUpdate(
    prevProps: Readonly<SegmentsViewProps>,
    prevState: Readonly<SegmentsViewState>,
  ) {
    const { filters } = this.props;
    const { groupByInterval, page, pageSize, sorted } = this.state;
    if (
      !segmentFiltersToExpression(filters).equals(segmentFiltersToExpression(prevProps.filters)) ||
      groupByInterval !== prevState.groupByInterval ||
      page !== prevState.page ||
      pageSize !== prevState.pageSize ||
      sortedToOrderByClause(sorted) !== sortedToOrderByClause(prevState.sorted)
    ) {
      this.fetchData();
    }
  }

  private readonly refresh = (auto: boolean): void => {
    if (auto && hasOverlayOpen()) return;
    this.segmentsQueryManager.rerunLastQuery(auto);

    const { showSegmentTimeline } = this.state;
    if (showSegmentTimeline) {
      // Create a new capabilities object to force the segment timeline to re-render
      this.setState(({ showSegmentTimeline }) => ({
        showSegmentTimeline: {
          ...showSegmentTimeline,
          capabilities: this.props.capabilities.clone(),
        },
      }));
    }
  };

  private readonly fetchData = () => {
    const { capabilities, filters } = this.props;
    const { visibleColumns, groupByInterval, page, pageSize, sorted } = this.state;
    this.segmentsQueryManager.runQuery({
      page,
      pageSize,
      filtered: filters,
      sorted,
      visibleColumns,
      capabilities,
      groupByInterval,
    });
  };

  private readonly handleFilterChange = (filters: Filter[]) => {
    this.goToFirstPage();
    this.props.onFiltersChange(filters);
  };

  private goToFirstPage() {
    if (this.state.page) {
      this.setState({ page: 0 });
    }
  }

  private getSegmentActions(id: string, datasource: string): BasicAction[] {
    const { capabilities } = this.props;
    const actions: BasicAction[] = [];

    if (capabilities.hasOverlordAccess()) {
      actions.push({
        icon: IconNames.IMPORT,
        title: 'Drop segment (disable)',
        intent: Intent.DANGER,
        onAction: () =>
          this.setState({ terminateSegmentId: id, terminateDatasourceId: datasource }),
      });
    }

    return actions;
  }

  private onDetail(segmentId: string, datasource: string): void {
    this.setState({
      segmentTableActionDialogId: segmentId,
      datasourceTableActionDialogId: datasource,
      actions: this.getSegmentActions(segmentId, datasource),
    });
  }

  private renderFilterableCell(
    field: string,
    enableComparisons = false,
    valueFn: (value: string) => ReactNode = String,
  ) {
    const { filters } = this.props;
    const { handleFilterChange } = this;

    return function FilterableCell(row: { value: any }) {
      return (
        <TableFilterableCell
          field={field}
          value={row.value}
          filters={filters}
          onFiltersChange={handleFilterChange}
          enableComparisons={enableComparisons}
        >
          {valueFn(row.value)}
        </TableFilterableCell>
      );
    };
  }

  renderSegmentsTable() {
    const { capabilities, filters } = this.props;
    const {
      segmentsState,
      visibleColumns,
      groupByInterval,
      page,
      pageSize,
      sorted,
      showSegmentTimeline,
    } = this.state;

    const { segments, count } = segmentsState.data || {
      segments: [],
      count: -1,
    };

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
        pages={count >= 0 ? Math.ceil(count / pageSize) : 10000000}
        loading={segmentsState.loading}
        noDataText={
          segmentsState.isEmpty()
            ? `No segments${filters.length ? ' matching filter' : ''}`
            : segmentsState.getErrorMessage() || ''
        }
        manual
        filterable
        filtered={filters}
        onFilteredChange={this.handleFilterChange}
        sorted={sorted}
        onSortedChange={sorted => this.setState({ sorted })}
        page={page}
        onPageChange={page => this.setState({ page })}
        pageSize={pageSize}
        onPageSizeChange={pageSize => this.setState({ pageSize })}
        pageSizeOptions={STANDARD_TABLE_PAGE_SIZE_OPTIONS}
        showPagination
        showPageJump={false}
        ofText={count >= 0 ? `of ${formatInteger(count)}` : ''}
        pivotBy={groupByInterval ? ['interval'] : []}
        columns={[
          {
            Header: 'Segment ID',
            show: visibleColumns.shown('Segment ID'),
            accessor: 'segment_id',
            width: 280,
            filterable: allowGeneralFilter,
            Cell: row => (
              <TableClickableCell
                tooltip="Show detail"
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
            Cell: this.renderFilterableCell(
              'datasource',
              false,
              showSegmentTimeline
                ? value => (
                    <>
                      <span style={{ color: getDatasourceColor(value) }}>&#9632;</span> {value}
                    </>
                  )
                : String,
            ),
          },
          {
            Header: 'Interval',
            show: groupByInterval,
            accessor: 'interval',
            width: 120,
            defaultSortDesc: true,
            filterable: allowGeneralFilter,
            Cell: this.renderFilterableCell('interval'),
          },
          {
            Header: 'Start',
            show: visibleColumns.shown('Start'),
            accessor: 'start',
            headerClassName: 'enable-comparisons',
            width: 180,
            defaultSortDesc: true,
            filterable: allowGeneralFilter,
            Cell: this.renderFilterableCell('start', true),
          },
          {
            Header: 'End',
            show: visibleColumns.shown('End'),
            accessor: 'end',
            headerClassName: 'enable-comparisons',
            width: 180,
            defaultSortDesc: true,
            filterable: allowGeneralFilter,
            Cell: this.renderFilterableCell('end', true),
          },
          {
            Header: 'Version',
            show: visibleColumns.shown('Version'),
            accessor: 'version',
            width: 180,
            defaultSortDesc: true,
            filterable: allowGeneralFilter,
            Cell: this.renderFilterableCell('version', true),
          },
          {
            Header: 'Time span',
            show: visibleColumns.shown('Time span'),
            id: 'time_span',
            className: 'padded',
            accessor: ({ start, end }) => computeSegmentTimeSpan(start, end),
            width: 100,
            sortable: false,
            filterable: false,
          },
          {
            Header: 'Shard type',
            show: visibleColumns.shown('Shard type'),
            id: 'shard_type',
            width: 100,
            sortable: false,
            accessor: ({ shard_spec }) => {
              if (typeof shard_spec?.type !== 'string') return '-';
              return shard_spec?.type;
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
              const onShowFullShardSpec = () => {
                this.setState({
                  showFullShardSpec:
                    value && typeof value === 'object'
                      ? JSONBig.stringify(value, undefined, 2)
                      : String(value),
                });
              };

              switch (value?.type) {
                case 'range': {
                  const dimensions: string[] = value.dimensions || [];
                  const formatEdge = (values: string[]) =>
                    dimensions.map((d, i) => formatRangeDimensionValue(d, values[i])).join('; ');

                  return (
                    <TableClickableCell
                      className="range-detail"
                      tooltip="Show full shardSpec"
                      onClick={onShowFullShardSpec}
                      hoverIcon={IconNames.EYE_OPEN}
                    >
                      <span className="range-label">Start:</span>
                      {Array.isArray(value.start) ? formatEdge(value.start) : '-∞'}
                      <br />
                      <span className="range-label">End:</span>
                      {Array.isArray(value.end) ? formatEdge(value.end) : '∞'}
                    </TableClickableCell>
                  );
                }

                case 'single': {
                  return (
                    <TableClickableCell
                      className="range-detail"
                      tooltip="Show full shardSpec"
                      onClick={onShowFullShardSpec}
                      hoverIcon={IconNames.EYE_OPEN}
                    >
                      <span className="range-label">Start:</span>
                      {value.start != null
                        ? formatRangeDimensionValue(value.dimension, value.start)
                        : '-∞'}
                      <br />
                      <span className="range-label">End:</span>
                      {value.end != null
                        ? formatRangeDimensionValue(value.dimension, value.end)
                        : '∞'}
                    </TableClickableCell>
                  );
                }

                case 'hashed': {
                  const { partitionDimensions } = value;
                  if (!Array.isArray(partitionDimensions)) return JSONBig.stringify(value);
                  return (
                    <TableClickableCell
                      tooltip="Show full shardSpec"
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
                      tooltip="Show full shardSpec"
                      onClick={onShowFullShardSpec}
                      hoverIcon={IconNames.EYE_OPEN}
                    >
                      No detail
                    </TableClickableCell>
                  );

                default:
                  return (
                    <TableClickableCell
                      tooltip="Show full shardSpec"
                      onClick={onShowFullShardSpec}
                      hoverIcon={IconNames.EYE_OPEN}
                    >
                      {JSONBig.stringify(value)}
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
            className: 'padded',
          },
          {
            Header: 'Size',
            show: visibleColumns.shown('Size'),
            accessor: 'size',
            filterable: false,
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
            show: visibleColumns.shown('Replication factor'),
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
            show: visibleColumns.shown('Is realtime'),
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
            show: visibleColumns.shown('Is overshadowed'),
            id: 'is_overshadowed',
            accessor: row => String(Boolean(row.is_overshadowed)),
            Filter: BooleanFilterInput,
            className: 'padded',
            width: 100,
          },
          {
            Header: ACTION_COLUMN_LABEL,
            show: capabilities.hasCoordinatorAccess(),
            id: ACTION_COLUMN_ID,
            accessor: 'segment_id',
            width: ACTION_COLUMN_WIDTH,
            filterable: false,
            sortable: false,
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
                  menuTitle={id}
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
            `/druid/indexer/v1/datasources/${Api.encodePath(
              terminateDatasourceId,
            )}/segments/${Api.encodePath(terminateSegmentId)}`,
            {},
          );
          return resp.data;
        }}
        confirmButtonText="Drop segment"
        successText="Segment drop request acknowledged, next time the overlord runs segment will be dropped"
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
          Are you sure you want to drop segment <Tag minimal>{terminateSegmentId}</Tag>?
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
            disabled={typeof lastSegmentsQuery !== 'string'}
            onClick={() => {
              if (typeof lastSegmentsQuery !== 'string') return;
              goToQuery({ queryString: lastSegmentsQuery });
            }}
          />
        )}
      </MoreButton>
    );
  }

  render() {
    const { capabilities, filters } = this.props;
    const {
      segmentTableActionDialogId,
      datasourceTableActionDialogId,
      actions,
      visibleColumns,
      showSegmentTimeline,
      showFullShardSpec,
      groupByInterval,
    } = this.state;

    return (
      <div className="segments-view app-view">
        <ViewControlBar label="Segments">
          <RefreshButton
            onRefresh={this.refresh}
            localStorageKey={LocalStorageKeys.SEGMENTS_REFRESH_RATE}
          />
          <Label>Group by</Label>
          <ButtonGroup>
            <Button
              active={!groupByInterval}
              onClick={() => {
                this.setState({ groupByInterval: false });
              }}
            >
              None
            </Button>
            <Button
              active={groupByInterval}
              onClick={() => {
                this.setState({ groupByInterval: true });
              }}
            >
              Interval
            </Button>
          </ButtonGroup>
          {this.renderBulkSegmentsActions()}
          <Switch
            checked={Boolean(showSegmentTimeline)}
            label="Show segment timeline"
            onChange={() =>
              this.setState({
                showSegmentTimeline: showSegmentTimeline
                  ? undefined
                  : {
                      capabilities,
                      datasource: findMap(filters, filter =>
                        filter.id === 'datasource' && /^=[^=|]+$/.exec(String(filter.value))
                          ? filter.value.slice(1)
                          : undefined,
                      ),
                    },
              })
            }
            disabled={!capabilities.hasSqlOrCoordinatorAccess()}
          />
          <TableColumnSelector
            columns={TABLE_COLUMNS_BY_MODE[capabilities.getMode()]}
            onChange={column =>
              this.setState(prevState => ({
                visibleColumns: prevState.visibleColumns.toggle(column),
              }))
            }
            onClose={added => {
              if (!added) return;
              this.fetchData();
            }}
            tableColumnsHidden={visibleColumns.getHiddenColumns()}
          />
        </ViewControlBar>
        <SplitterLayout
          className="timeline-segments-splitter"
          vertical
          percentage
          secondaryInitialSize={35}
          primaryIndex={1}
          primaryMinSize={20}
          secondaryMinSize={10}
        >
          {showSegmentTimeline && (
            <SegmentTimeline
              capabilities={showSegmentTimeline.capabilities}
              datasource={showSegmentTimeline.datasource}
              getIntervalActionButton={(start, end, datasource, realtime) => {
                return (
                  <Button
                    text="Apply fitler to table"
                    small
                    rightIcon={IconNames.ARROW_DOWN}
                    onClick={() =>
                      this.handleFilterChange(
                        compact([
                          start && { id: 'start', value: `>=${start.toISOString()}` },
                          end && { id: 'end', value: `<${end.toISOString()}` },
                          datasource && { id: 'datasource', value: `=${datasource}` },
                          typeof realtime === 'boolean'
                            ? { id: 'is_realtime', value: `=${realtime}` }
                            : undefined,
                        ]),
                      )
                    }
                  />
                );
              }}
            />
          )}
          {this.renderSegmentsTable()}
        </SplitterLayout>
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
      </div>
    );
  }
}
