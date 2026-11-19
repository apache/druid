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

// ConsoleTable renders the tables of the console. Its props and DOM structure are derived from react-table v6
// (https://github.com/tannerlinsley/react-table/tree/v6) released under the MIT License. The data processing
// (filtering, sorting, grouping, pagination, and column resizing) is done by TanStack Table.

import type {
  ColumnDef,
  ColumnFilter,
  ColumnSort,
  ExpandedState,
  Header,
  Row,
} from '@tanstack/react-table';
import {
  columnFilteringFeature,
  columnGroupingFeature,
  columnResizingFeature,
  columnSizingFeature,
  createFilteredRowModel,
  createGroupedRowModel,
  createPaginatedRowModel,
  createSortedRowModel,
  rowExpandingFeature,
  rowPaginationFeature,
  rowSortingFeature,
  tableFeatures,
  useTable,
} from '@tanstack/react-table';
import classNames from 'classnames';
import type { ComponentType, CSSProperties, ReactNode } from 'react';
import React, { isValidElement, useMemo, useRef, useState } from 'react';

import { countBy, deepGet } from '../../utils';
import { TableFilter } from '../../utils/table-filters';
import { Loader } from '../loader/loader';

import { ConsoleTablePagination } from './console-table-pagination/console-table-pagination';
import { DEFAULT_TABLE_CLASS_NAME } from './constants';
import { GenericFilterInput } from './table-filter-inputs';

export type { ColumnFilter, ColumnSort };

/** The information about a row that is passed to the render functions */
export interface ConsoleTableRowInfo<T> {
  /** The data item this row was created from (the first item in the group for aggregated rows) */
  original: T;
  /** The values of all the columns in this row keyed by column id */
  row: Record<string, any>;
  /** The index of the data item in `data` */
  index: number;
  /** True for the rows that represent a group of rows (when using pivotBy) */
  aggregated: boolean;
  /** The rows in the group (for aggregated rows), each has the values keyed by column id and the data item as `_original` */
  subRows: any[];
}

export interface ConsoleTableCellInfo<T, V = any> extends ConsoleTableRowInfo<T> {
  value: V;
  column: ConsoleTableColumn<T>;
  isExpanded: boolean;
}

export interface ConsoleTableFilterProps {
  column: ConsoleTableColumn;
  filter: ColumnFilter | undefined;
  onChange(value: string): void;
}

export interface ConsoleTableColumn<T = any> {
  /** The column id, defaults to the accessor if the accessor is a string */
  id?: string;
  Header?: ReactNode | ComponentType<{ column: ConsoleTableColumn<T> }>;
  /** A path into the data item (like 'a.b') or a function that computes the cell value */
  accessor?: string | ((d: T) => any);
  /** Renders the cell, the value of the accessor is rendered if not set */
  Cell?: ComponentType<ConsoleTableCellInfo<T>>;
  /** Renders the cell for aggregated rows (when using pivotBy), by default the distinct values in the group are listed */
  Aggregated?: ComponentType<ConsoleTableCellInfo<T>>;
  /** Renders the filter input, GenericFilterInput is used by default */
  Filter?: ComponentType<ConsoleTableFilterProps>;
  /** A custom filter predicate, `row` has the values keyed by column id and the data item as `_original` */
  filterMethod?: (filter: ColumnFilter, row: any) => boolean;
  /** A custom comparator for the values of this column (always return the ascending order) */
  sortMethod?: (a: any, b: any, desc: boolean) => number;
  /** Set to false to hide the column */
  show?: boolean;
  width?: number;
  minWidth?: number;
  maxWidth?: number;
  className?: string;
  headerClassName?: string;
  sortable?: boolean;
  filterable?: boolean;
  resizable?: boolean;
  /** Sort in descending order when the column header is first clicked */
  defaultSortDesc?: boolean;
  /** Sub columns, to show a header that spans several columns */
  columns?: ConsoleTableColumn<T>[];
}

export interface ConsoleTableProps<T> {
  data: readonly T[];
  columns: ConsoleTableColumn<T>[];
  className?: string;
  loading?: boolean;
  noDataText?: ReactNode;

  /** Allow sorting by clicking on the column headers (shift + click to sort by several columns) */
  sortable?: boolean;
  sorted?: ColumnSort[];
  defaultSorted?: ColumnSort[];
  onSortedChange?(sorted: ColumnSort[]): void;

  /** Show a row of filter inputs under the headers */
  filterable?: boolean;
  filtered?: ColumnFilter[];
  onFilteredChange?(filtered: ColumnFilter[]): void;

  page?: number;
  onPageChange?(page: number): void;
  pageSize?: number;
  defaultPageSize?: number;
  onPageSizeChange?(pageSize: number, page: number): void;
  pageSizeOptions?: number[];
  showPagination?: boolean;
  showPageJump?: boolean;
  /** The text after the row range in the pagination ('of' shows the total row count) */
  ofText?: string;

  /** The data is already filtered, sorted, and paginated (by the server) */
  manual?: boolean;
  /** The page count when `manual` is set */
  pages?: number;

  /** Group the rows by the values of these columns */
  pivotBy?: string[];

  /** Makes the rows expandable, rendering this under an expanded row */
  SubComponent?(rowInfo: ConsoleTableRowInfo<T>): ReactNode;
  /** Collapse the expanded rows when the data changes, defaults to true */
  collapseOnDataChange?: boolean;
}

const features = tableFeatures({
  columnFilteringFeature,
  filteredRowModel: createFilteredRowModel(),
  columnGroupingFeature,
  groupedRowModel: createGroupedRowModel(),
  rowSortingFeature,
  sortedRowModel: createSortedRowModel(),
  rowExpandingFeature,
  rowPaginationFeature,
  paginatedRowModel: createPaginatedRowModel(),
  columnSizingFeature,
  columnResizingFeature,
});

// TanStack Table constrains the row type so it is typed loosely internally
type TableRow = Row<typeof features, any>;

const EMPTY_ARRAY: any[] = [];
const DEFAULT_PAGE_SIZE_OPTIONS = [5, 10, 20, 25, 50, 100];
const DEFAULT_MIN_WIDTH = 100;
const EXPANDER_WIDTH = 35;
const MIN_RESIZE_WIDTH = 11;

interface LeafColumn<T> {
  id: string;
  column: ConsoleTableColumn<T>;
  expander?: boolean;
  pivoted?: boolean;
}

interface ColumnGroup<T> {
  column?: ConsoleTableColumn<T>;
  leaves: LeafColumn<T>[];
}

function getColumnId(column: ConsoleTableColumn, fallback: string): string {
  return column.id ?? (typeof column.accessor === 'string' ? column.accessor : fallback);
}

function getAllLeafColumns<T>(columns: ConsoleTableColumn<T>[]): LeafColumn<T>[] {
  let i = 0;
  return columns.flatMap(column =>
    (column.columns || [column]).map(leaf => ({
      id: getColumnId(leaf, `__column${i++}`),
      column: leaf,
    })),
  );
}

// Works out the visible columns and the header groups: the pivot columns go first, followed by the other
// visible columns, with the adjacent columns that are not in a column group getting a common empty group header
function getVisibleColumnGroups<T>(
  columns: ConsoleTableColumn<T>[],
  allLeafColumns: LeafColumn<T>[],
  pivotBy: string[],
  hasSubComponent: boolean,
): ColumnGroup<T>[] {
  const leafColumnsByColumn = new Map(allLeafColumns.map(leaf => [leaf.column, leaf]));
  const isShown = (leaf: LeafColumn<T>) => !pivotBy.includes(leaf.id) && leaf.column.show !== false;

  const groups: ColumnGroup<T>[] = [];
  let currentSpan: LeafColumn<T>[] = [];
  const endSpan = () => {
    if (!currentSpan.length) return;
    groups.push({ leaves: currentSpan });
    currentSpan = [];
  };

  if (hasSubComponent) {
    currentSpan.push({
      id: '__expander',
      column: { width: EXPANDER_WIDTH, sortable: false, resizable: false, filterable: false },
      expander: true,
    });
  }

  const pivotLeaves = pivotBy.flatMap(id => {
    const leaf = allLeafColumns.find(leaf => leaf.id === id);
    return leaf ? [{ ...leaf, pivoted: true }] : [];
  });
  if (pivotLeaves.length) {
    endSpan();
    groups.push({ column: { Header: <strong>Pivoted</strong> }, leaves: pivotLeaves });
  }

  for (const column of columns) {
    if (column.columns) {
      const leaves = column.columns.map(c => leafColumnsByColumn.get(c)!).filter(isShown);
      if (!leaves.length) continue;
      endSpan();
      groups.push({ column, leaves });
    } else {
      const leaf = leafColumnsByColumn.get(column)!;
      if (isShown(leaf)) currentSpan.push(leaf);
    }
  }
  endSpan();

  return groups;
}

const rowValuesCache = new WeakMap<TableRow, Record<string, any>>();

/** The values of all the columns in a row keyed by column id (and the data item as `_original`) */
function getRowValues(row: TableRow): Record<string, any> {
  let values = rowValuesCache.get(row);
  if (!values) {
    values = { _original: row.original, _index: row.index };
    for (const column of row.table.getAllLeafColumns()) values[column.id] = row.getValue(column.id);
    rowValuesCache.set(row, values);
  }
  return values;
}

function defaultFilterMethod(filter: ColumnFilter, row: Record<string, any>): boolean {
  return TableFilter.fromFilter(filter).matches(row[filter.id]);
}

function defaultSortMethod(a: any, b: any): number {
  // Force null and undefined to the bottom and compare strings case insensitively
  a = a ?? '';
  b = b ?? '';
  a = typeof a === 'string' ? a.toLowerCase() : a;
  b = typeof b === 'string' ? b.toLowerCase() : b;
  if (a > b) return 1;
  if (a < b) return -1;
  return 0;
}

/**
 * Renders a column renderer: a function is used as a component (so it can use hooks), anything else
 * is rendered as is
 */
function renderComponent<P extends object>(
  component: ReactNode | ComponentType<P> | undefined,
  props: P,
  fallback: ReactNode = component as ReactNode,
): ReactNode {
  if (isValidElement(component) || typeof component === 'string') return component;
  if (typeof component === 'function') return React.createElement(component, props);
  return fallback;
}

function asPx(value: number | undefined): string | undefined {
  return typeof value === 'number' && !isNaN(value) ? `${value}px` : undefined;
}

function flexStyle(flex: number, width: number, maxWidth: number | undefined): CSSProperties {
  return { flex: `${flex} 0 auto`, width: asPx(width), maxWidth: asPx(maxWidth) };
}

function Expander({ isExpanded }: { isExpanded: boolean }) {
  return <div className={classNames('ct-expander', isExpanded && '-open')}>•</div>;
}

function DefaultAggregated({ subRows, column }: ConsoleTableCellInfo<any>) {
  const id = getColumnId(column, '');
  const previewCount = countBy(subRows.filter(d => typeof d[id] !== 'undefined').map(d => d[id]));
  return (
    <div className="default-aggregated">
      {Object.keys(previewCount)
        .sort()
        .map(v => `${v} (${previewCount[v]})`)
        .join(', ')}
    </div>
  );
}

function sortedEqual(a: readonly ColumnSort[], b: readonly ColumnSort[]): boolean {
  return a.length === b.length && a.every((s, i) => s.id === b[i].id && s.desc === b[i].desc);
}

function filteredEqual(a: readonly ColumnFilter[], b: readonly ColumnFilter[]): boolean {
  return a.length === b.length && a.every((f, i) => f.id === b[i].id && f.value === b[i].value);
}

/** Returns the previous value while the new one is equal to it so the (memoized) row models do not recompute */
function useStableValue<V>(value: V, equals: (a: V, b: V) => boolean): V {
  const [stableValue, setStableValue] = useState(value);
  if (stableValue !== value && !equals(stableValue, value)) {
    setStableValue(value);
    return value;
  }
  return stableValue;
}

export function ConsoleTable<T>(props: ConsoleTableProps<T>) {
  const {
    data,
    columns,
    className = DEFAULT_TABLE_CLASS_NAME,
    loading = false,
    noDataText = 'No rows found',
    sortable = true,
    defaultSorted = EMPTY_ARRAY,
    onSortedChange,
    filterable = false,
    onFilteredChange,
    onPageChange,
    defaultPageSize = 20,
    onPageSizeChange,
    pageSizeOptions = DEFAULT_PAGE_SIZE_OPTIONS,
    showPagination = true,
    showPageJump = true,
    ofText = 'of',
    manual = false,
    pivotBy = EMPTY_ARRAY,
    SubComponent,
    collapseOnDataChange = true,
  } = props;

  const [internalSorted, setInternalSorted] = useState<ColumnSort[]>(defaultSorted);
  const [internalFiltered, setInternalFiltered] = useState<ColumnFilter[]>(EMPTY_ARRAY);
  const [internalPage, setInternalPage] = useState(0);
  const [internalPageSize, setInternalPageSize] = useState(defaultPageSize);
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});

  // Adjust the internal state when the props it depends on change
  const [prevDefaultSorted, setPrevDefaultSorted] = useState(defaultSorted);
  if (!sortedEqual(prevDefaultSorted, defaultSorted)) {
    setPrevDefaultSorted(defaultSorted);
    setInternalSorted(defaultSorted);
  }
  const [prevDefaultPageSize, setPrevDefaultPageSize] = useState(defaultPageSize);
  if (prevDefaultPageSize !== defaultPageSize) {
    setPrevDefaultPageSize(defaultPageSize);
    setInternalPageSize(defaultPageSize);
  }
  const [prevData, setPrevData] = useState(data);
  if (prevData !== data) {
    setPrevData(data);
    if (collapseOnDataChange) setExpanded({});
  }

  const sorted = useStableValue(props.sorted ?? internalSorted, sortedEqual);
  const filtered = useStableValue(props.filtered ?? internalFiltered, filteredEqual);
  const stablePivotBy = useStableValue(pivotBy, (a, b) => String(a) === String(b));
  const pageSize = props.pageSize ?? internalPageSize;

  // Collapse the rows when what they are made from changes
  const [prevRowInputs, setPrevRowInputs] = useState([sorted, filtered, stablePivotBy]);
  if (
    prevRowInputs[0] !== sorted ||
    prevRowInputs[1] !== filtered ||
    prevRowInputs[2] !== stablePivotBy
  ) {
    setPrevRowInputs([sorted, filtered, stablePivotBy]);
    setExpanded({});
  }

  const allLeafColumns = useMemo(() => getAllLeafColumns(columns), [columns]);

  const { columnDefs, tableData } = useMemo(() => {
    const columnDefs: ColumnDef<typeof features, any, any>[] = allLeafColumns.map(
      ({ id, column }) => {
        const { accessor } = column;
        return {
          id,
          accessorFn:
            typeof accessor === 'function'
              ? accessor
              : typeof accessor === 'string'
                ? (d: T) => deepGet(d as any, accessor)
                : () => undefined,
          filterFn: (row, _columnId, value) => {
            if (column.filterable === false) return true;
            return (column.filterMethod || defaultFilterMethod)({ id, value }, getRowValues(row));
          },
          sortFn: (rowA, rowB) => {
            const sorting = rowA.table.atoms.sorting.get();
            const desc = Boolean(sorting.find(s => s.id === id)?.desc);
            const comparison = (column.sortMethod || defaultSortMethod)(
              rowA.getValue(id),
              rowB.getValue(id),
              desc,
            );
            if (comparison || sorting[sorting.length - 1]?.id !== id || rowA.getIsGrouped()) {
              return comparison;
            }

            // Break the ties by the data order, reversed if the first sort is descending
            const tie = sorting[0].desc ? rowB.index - rowA.index : rowA.index - rowB.index;
            return desc ? -tie : tie;
          },
          sortUndefined: false,
          size: column.width ?? column.minWidth ?? DEFAULT_MIN_WIDTH,
          minSize: MIN_RESIZE_WIDTH,
          enableResizing: column.resizable ?? true,
        };
      },
    );

    // The cell values are cached on the rows so give TanStack Table new data when the accessors change
    return { columnDefs, tableData: data.slice() };
  }, [allLeafColumns, data]);

  const page = props.page ?? internalPage;

  const table = useTable({
    features,
    data: tableData,
    columns: columnDefs,
    state: {
      sorting: sorted,
      columnFilters: filtered,
      grouping: stablePivotBy,
      expanded: expanded as ExpandedState,
      pagination: { pageIndex: page, pageSize },
    },
    manualFiltering: manual,
    manualSorting: manual,
    manualPagination: manual,
    groupedColumnMode: false,
    autoResetPageIndex: false,
    autoResetExpanded: false,
    columnResizeMode: 'onChange',
  });

  const rowCount = table.getPrePaginatedRowModel().rows.length;
  const pages = manual ? (props.pages ?? -1) : Math.ceil(rowCount / pageSize);
  if (!manual && props.page === undefined) {
    // Keep the page in range when the number of rows goes down (only when the page is not controlled)
    const clampedPage = Math.max(Math.min(internalPage, pages - 1), 0);
    if (clampedPage !== internalPage) setInternalPage(clampedPage);
  }
  const pageRows = table.getRowModel().rows;

  const columnGroups = getVisibleColumnGroups(
    columns,
    allLeafColumns,
    stablePivotBy,
    Boolean(SubComponent),
  );
  const visibleLeaves = columnGroups.flatMap(group => group.leaves);
  const hasHeaderGroups = columns.some(column => column.columns);
  const hasFilters = filterable || visibleLeaves.some(leaf => leaf.column.filterable);

  const { columnSizing, columnResizing } = table.state;
  const getResizedWidth = (leaf: LeafColumn<T>): number | undefined => columnSizing[leaf.id];
  const getWidth = (leaf: LeafColumn<T>): number =>
    getResizedWidth(leaf) ?? leaf.column.width ?? leaf.column.minWidth ?? DEFAULT_MIN_WIDTH;
  const getMaxWidth = (leaf: LeafColumn<T>): number | undefined =>
    getResizedWidth(leaf) ?? leaf.column.width ?? leaf.column.maxWidth;
  const getLeafStyle = (leaf: LeafColumn<T>) => {
    const width = getWidth(leaf);
    return flexStyle(width, width, getMaxWidth(leaf));
  };
  const rowMinWidth = visibleLeaves.reduce((total, leaf) => total + getWidth(leaf), 0);

  const leafHeaders = new Map<string, Header<typeof features, any>>(
    table.getLeafHeaders().map(header => [header.column.id, header]),
  );

  // A click that ends a column resize should not also sort the column
  const suppressSortClick = useRef(false);

  function changePage(newPage: number) {
    setExpanded({});
    setInternalPage(newPage);
    onPageChange?.(newPage);
  }

  function changePageSize(newPageSize: number) {
    const newPage = Math.floor((pageSize * page) / newPageSize);
    setInternalPageSize(newPageSize);
    setInternalPage(newPage);
    onPageSizeChange?.(newPageSize, newPage);
  }

  // A click cycles between the sort directions, shift + click adds the column to the sort (or removes it once
  // it has cycled through both directions)
  function sortColumn(leaf: LeafColumn<T>, additive: boolean) {
    const firstSortDirection = leaf.column.defaultSortDesc ?? false;
    const secondSortDirection = !firstSortDirection;
    let newSorted = sorted.map(s => ({ ...s }));
    const existingIndex = newSorted.findIndex(s => s.id === leaf.id);
    if (existingIndex > -1) {
      const existing = newSorted[existingIndex];
      if (existing.desc === secondSortDirection) {
        if (additive) {
          newSorted.splice(existingIndex, 1);
        } else {
          existing.desc = firstSortDirection;
          newSorted = [existing];
        }
      } else {
        existing.desc = secondSortDirection;
        if (!additive) newSorted = [existing];
      }
    } else if (additive) {
      newSorted.push({ id: leaf.id, desc: firstSortDirection });
    } else {
      newSorted = [{ id: leaf.id, desc: firstSortDirection }];
    }

    if ((!sorted.length && newSorted.length) || !additive) setInternalPage(0);
    setInternalSorted(newSorted);
    onSortedChange?.(newSorted);
  }

  function filterColumn(leaf: LeafColumn<T>, value: string) {
    const newFiltered = filtered.filter(f => f.id !== leaf.id);
    if (value !== '') newFiltered.push({ id: leaf.id, value });
    setInternalPage(0);
    setInternalFiltered(newFiltered);
    onFilteredChange?.(newFiltered);
  }

  function toggleExpanded(row: TableRow) {
    setExpanded({ ...expanded, [row.id]: !expanded[row.id] });
  }

  function startResize(e: React.MouseEvent | React.TouchEvent, leaf: LeafColumn<T>) {
    e.stopPropagation();
    const header = leafHeaders.get(leaf.id);
    if (!header) return;

    // Start the resize from the rendered width (the column might have grown to fill the table)
    const renderedWidth = e.currentTarget.parentElement!.getBoundingClientRect().width;
    table.setColumnSizing(old => ({ ...old, [leaf.id]: renderedWidth }));

    suppressSortClick.current = true;
    const endEvent = e.type === 'touchstart' ? 'touchend' : 'mouseup';
    document.addEventListener(
      endEvent,
      () => {
        setTimeout(() => {
          suppressSortClick.current = false;
        }, 0);
      },
      { once: true },
    );

    header.getResizeHandler()(e.nativeEvent);
  }

  function renderHeaderGroups() {
    return (
      <div className="ct-thead -headerGroups" style={{ minWidth: `${rowMinWidth}px` }}>
        <div className="ct-tr" role="row">
          {columnGroups.map((group, i) => {
            const flex = group.leaves.reduce(
              (total, leaf) =>
                total +
                (leaf.column.width || getResizedWidth(leaf)
                  ? 0
                  : (leaf.column.minWidth ?? DEFAULT_MIN_WIDTH)),
              0,
            );
            const width = group.leaves.reduce((total, leaf) => total + getWidth(leaf), 0);
            const maxWidth = group.leaves.reduce<number | undefined>((total, leaf) => {
              const leafMaxWidth = getMaxWidth(leaf);
              return total === undefined || leafMaxWidth === undefined
                ? undefined
                : total + leafMaxWidth;
            }, 0);
            return (
              <div
                key={i}
                className={classNames('ct-th', group.column?.headerClassName)}
                role="columnheader"
                tabIndex={-1}
                style={flexStyle(flex, width, maxWidth)}
              >
                {group.column && renderComponent(group.column.Header, { column: group.column })}
              </div>
            );
          })}
        </div>
      </div>
    );
  }

  function renderHeaders() {
    return (
      <div className="ct-thead -header" style={{ minWidth: `${rowMinWidth}px` }}>
        <div className="ct-tr" role="row">
          {visibleLeaves.map(leaf => {
            const { column } = leaf;
            const sort = sorted.find(s => s.id === leaf.id);
            const isResizable = column.resizable ?? true;
            const isSortable = column.sortable ?? sortable;
            return (
              <div
                key={leaf.id}
                className={classNames(
                  'ct-th',
                  column.headerClassName,
                  isResizable && 'ct-resizable-header',
                  sort ? (sort.desc ? '-sort-desc' : '-sort-asc') : '',
                  isSortable && '-cursor-pointer',
                  column.show === false && '-hidden',
                )}
                role="columnheader"
                tabIndex={-1}
                style={getLeafStyle(leaf)}
                onClick={e => {
                  if (suppressSortClick.current) return;
                  if (isSortable) sortColumn(leaf, e.shiftKey);
                }}
              >
                <div className={classNames(isResizable && 'ct-resizable-header-content')}>
                  {renderComponent(column.Header, { column })}
                </div>
                {isResizable && (
                  <div
                    className="ct-resizer"
                    onMouseDown={e => startResize(e, leaf)}
                    onTouchStart={e => startResize(e, leaf)}
                  />
                )}
              </div>
            );
          })}
        </div>
      </div>
    );
  }

  function renderFilters() {
    return (
      <div className="ct-thead -filters" style={{ minWidth: `${rowMinWidth}px` }}>
        <div className="ct-tr" role="row">
          {visibleLeaves.map(leaf => {
            const { column } = leaf;
            const isFilterable = column.filterable ?? filterable;
            return (
              <div
                key={leaf.id}
                className={classNames('ct-th', column.headerClassName)}
                role="columnheader"
                tabIndex={-1}
                style={getLeafStyle(leaf)}
              >
                {isFilterable &&
                  React.createElement(column.Filter || GenericFilterInput, {
                    column: { ...column, id: leaf.id },
                    filter: filtered.find(f => f.id === leaf.id),
                    onChange: (value: string) => filterColumn(leaf, value),
                  })}
              </div>
            );
          })}
        </div>
      </div>
    );
  }

  function getRowInfo(row: TableRow): ConsoleTableRowInfo<T> {
    const aggregated = row.getIsGrouped();
    return {
      original: row.original,
      row: getRowValues(row),
      index: row.index,
      aggregated,
      subRows: aggregated ? row.subRows.map(getRowValues) : [],
    };
  }

  function renderCell(leaf: LeafColumn<T>, row: TableRow, rowInfo: ConsoleTableRowInfo<T>) {
    const { column } = leaf;
    const isExpanded = Boolean(expanded[row.id]);
    let content: ReactNode = null;
    let expandable = false;
    let isPivot = false;

    if (leaf.expander) {
      expandable = true;
      content = <Expander isExpanded={isExpanded} />;
    } else if (leaf.pivoted) {
      // In a grouped row the grouping column shows the group value, it is empty in the rows of the group
      if (rowInfo.aggregated && row.groupingColumnId === leaf.id) {
        expandable = true;
        isPivot = true;
        let groupLabel = String(row.groupingValue);
        if (groupLabel === 'undefined') groupLabel = 'n/a';
        content = (
          <div>
            <Expander isExpanded={isExpanded} />
            <span className="default-pivoted">{`${groupLabel} (${row.subRows.length})`}</span>
          </div>
        );
      }
    } else {
      const cellInfo: ConsoleTableCellInfo<T> = {
        ...rowInfo,
        value: row.getValue(leaf.id),
        column,
        isExpanded,
      };
      content = rowInfo.aggregated
        ? renderComponent(column.Aggregated || DefaultAggregated, cellInfo)
        : renderComponent(column.Cell, cellInfo, cellInfo.value);
    }

    return (
      <div
        key={leaf.id}
        className={classNames(
          'ct-td',
          column.className,
          !expandable && column.show === false && 'hidden',
          expandable && 'ct-expandable',
          isPivot && 'ct-pivot',
        )}
        role="gridcell"
        style={getLeafStyle(leaf)}
        onClick={expandable ? () => toggleExpanded(row) : undefined}
      >
        {content}
      </div>
    );
  }

  let viewIndex = 0;

  function renderRow(row: TableRow, key: string): ReactNode {
    const rowInfo = getRowInfo(row);
    const isExpanded = Boolean(expanded[row.id]);
    const rowClassName = viewIndex++ % 2 ? '-even' : '-odd';
    return (
      <div key={key} className="ct-tr-group" role="rowgroup">
        <div className={classNames('ct-tr', rowClassName)} role="row">
          {visibleLeaves.map(leaf => renderCell(leaf, row, rowInfo))}
        </div>
        {isExpanded &&
          (row.subRows.length
            ? row.subRows.map((subRow, i) => renderRow(subRow, `${key}_${i}`))
            : SubComponent?.(rowInfo))}
      </div>
    );
  }

  function renderPadRow(i: number) {
    return (
      <div key={`pad-${i}`} className="ct-tr-group" role="rowgroup">
        <div
          className={classNames('ct-tr', '-padRow', (pageRows.length + i) % 2 ? '-even' : '-odd')}
          role="row"
        >
          {visibleLeaves.map(leaf => (
            <div
              key={leaf.id}
              className={classNames(
                'ct-td',
                leaf.column.className,
                leaf.column.show === false && 'hidden',
              )}
              role="gridcell"
              style={getLeafStyle(leaf)}
            >
              <span>&nbsp;</span>
            </div>
          ))}
        </div>
      </div>
    );
  }

  const padRowCount = Math.max(pageSize - pageRows.length, 0);
  return (
    <div className={classNames('console-table', className)}>
      <div
        className={classNames('ct-table', columnResizing.isResizingColumn ? 'ct-resizing' : '')}
        role="grid"
      >
        {hasHeaderGroups && renderHeaderGroups()}
        {renderHeaders()}
        {hasFilters && renderFilters()}
        <div className="ct-tbody" style={{ minWidth: `${rowMinWidth}px` }}>
          {pageRows.map((row, i) => renderRow(row, String(i)))}
          {Array.from({ length: padRowCount }, (_, i) => renderPadRow(i))}
        </div>
      </div>
      {showPagination && (
        <div className="pagination-bottom">
          <ConsoleTablePagination
            page={page}
            pages={pages}
            pageSize={pageSize}
            pageSizeOptions={pageSizeOptions}
            showPageJump={showPageJump}
            canPrevious={page > 0}
            canNext={page + 1 < pages}
            onPageChange={changePage}
            onPageSizeChange={changePageSize}
            ofText={ofText}
            rowCount={rowCount}
          />
        </div>
      )}
      {!pageRows.length && !!noDataText && <div className="ct-no-data">{noDataText}</div>}
      <Loader loading={loading} loadingText="" />
    </div>
  );
}
