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
  AnchorButton,
  Button,
  ButtonGroup,
  Callout,
  FormGroup,
  Intent,
  Menu,
  MenuDivider,
  MenuItem,
  Tag,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import {
  C,
  Column,
  F,
  QueryResult,
  QueryRunner,
  SqlExpression,
  SqlQuery,
  SqlType,
} from '@druid-toolkit/query';
import classNames from 'classnames';
import { select, selectAll } from 'd3-selection';
import type { JSX } from 'react';
import React, { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';

import { ClearableInput, LearnMore, Loader } from '../../../components';
import { AsyncActionDialog } from '../../../dialogs';
import type { Execution, ExternalConfig, IngestQueryPattern } from '../../../druid-models';
import {
  changeQueryPatternExpression,
  DETECTION_TIMESTAMP_SPEC,
  fitIngestQueryPattern,
  getDestinationMode,
  getQueryPatternExpression,
  getQueryPatternExpressionType,
  ingestQueryPatternToQuery,
  possibleDruidFormatForValues,
  TIME_COLUMN,
  WorkbenchQuery,
} from '../../../druid-models';
import {
  executionBackgroundResultStatusCheck,
  extractResult,
  submitTaskQuery,
} from '../../../helpers';
import { useLastDefined, usePermanentCallback, useQueryManager } from '../../../hooks';
import { useLastDefinedDeep } from '../../../hooks/use-last-defined-deep';
import { getLink } from '../../../links';
import { AppToaster } from '../../../singletons';
import type { QueryAction } from '../../../utils';
import {
  caseInsensitiveContains,
  change,
  dataTypeToIcon,
  deepSet,
  DruidError,
  filterMap,
  oneOf,
  queryDruidSql,
  sampleDataToQuery,
  tickIcon,
  timeFormatToSql,
  wait,
  without,
} from '../../../utils';
import { getHeaderFromSampleResponse, postToSampler } from '../../../utils/sampler';
import { FlexibleQueryInput } from '../../workbench-view/flexible-query-input/flexible-query-input';
import { ColumnActions } from '../column-actions/column-actions';
import { ColumnEditor } from '../column-editor/column-editor';
import { DestinationDialog } from '../destination-dialog/destination-dialog';
import { ExpressionEditorDialog } from '../expression-editor-dialog/expression-editor-dialog';
import { TitleFrame } from '../title-frame/title-frame';

import { ColumnList } from './column-list/column-list';
import { PreviewError } from './preview-error/preview-error';
import { PreviewTable } from './preview-table/preview-table';
import { RollupAnalysisPane } from './rollup-analysis-pane/rollup-analysis-pane';

import './schema-step.scss';

const queryRunner = new QueryRunner();

function digestQueryString(queryString: string): {
  ingestQueryPattern?: IngestQueryPattern;
  ingestPatternError?: string;
  parsedQuery?: SqlQuery;
} {
  let ingestQueryPattern: IngestQueryPattern | undefined;
  const parsedQuery = SqlQuery.maybeParse(queryString);
  let ingestPatternError: string | undefined;
  if (parsedQuery) {
    try {
      ingestQueryPattern = fitIngestQueryPattern(parsedQuery);
    } catch (e) {
      ingestPatternError = e.message;
    }
  } else {
    ingestPatternError = 'Unparsable query';
  }

  return {
    ingestQueryPattern,
    ingestPatternError,
    parsedQuery,
  };
}

interface TimeSuggestion {
  label: string | JSX.Element;
  queryAction: QueryAction;
}

function getTimeSuggestions(queryResult: QueryResult, parsedQuery: SqlQuery): TimeSuggestion[] {
  const hasGroupBy = parsedQuery.hasGroupBy();
  const timeColumnIndex = queryResult.header.findIndex(({ name }) => name === TIME_COLUMN);
  if (timeColumnIndex !== -1) {
    const timeColumn = queryResult.header[timeColumnIndex];
    switch (timeColumn.sqlType) {
      case 'TIMESTAMP':
        return []; // All good, nothing to do

      case 'VARCHAR':
      case 'BIGINT': {
        const selectExpression = parsedQuery.getSelectExpressionForIndex(timeColumnIndex);
        if (!selectExpression) return [];

        const values = queryResult.rows.map(row => row[timeColumnIndex]);
        const possibleDruidFormat = possibleDruidFormatForValues(values);
        const formatSql = possibleDruidFormat ? timeFormatToSql(possibleDruidFormat) : undefined;
        if (!formatSql) return [];
        const newSelectExpression = formatSql.fillPlaceholders([
          selectExpression.getUnderlyingExpression(),
        ]);

        return [
          {
            label: `Parse as '${possibleDruidFormat}'`,
            queryAction: q =>
              q.removeSelectIndex(timeColumnIndex).addSelect(newSelectExpression.as(TIME_COLUMN), {
                insertIndex: 0,
                addToGroupBy: hasGroupBy ? 'start' : undefined,
              }),
          },
        ];
      }

      default:
        return [
          {
            label: `Remove __time column which is of an unusable type ${timeColumn.sqlType}`,
            queryAction: q => q.removeSelectIndex(timeColumnIndex),
          },
        ];
    }
  } else {
    const suggestions: TimeSuggestion[] = filterMap(queryResult.header, (c, i) => {
      const selectExpression = parsedQuery.getSelectExpressionForIndex(i);
      if (!selectExpression) return;

      if (c.sqlType === 'TIMESTAMP') {
        return {
          label: (
            <>
              {'Use '}
              <strong>{c.name}</strong>
              {' as the primary time column'}
            </>
          ),
          queryAction: q =>
            q.removeSelectIndex(timeColumnIndex).addSelect(selectExpression.as(TIME_COLUMN), {
              insertIndex: 0,
              addToGroupBy: hasGroupBy ? 'start' : undefined,
            }),
        };
      }

      const values = queryResult.rows.map(row => row[i]);
      const possibleDruidFormat = possibleDruidFormatForValues(values);
      const formatSql = possibleDruidFormat ? timeFormatToSql(possibleDruidFormat) : undefined;
      if (!formatSql) return;
      const newSelectExpression = formatSql.fillPlaceholders([
        selectExpression.getUnderlyingExpression(),
      ]);

      return {
        label: (
          <>
            {`Use `}
            <strong>{c.name}</strong>
            {` parsed as '${possibleDruidFormat}'`}
          </>
        ),
        queryAction: q =>
          q.removeSelectIndex(i).addSelect(newSelectExpression.as(TIME_COLUMN), {
            insertIndex: 0,
            addToGroupBy: hasGroupBy ? 'start' : undefined,
          }),
      };
    });

    if (suggestions.length) return suggestions;
    return [
      {
        label: 'Use a constant as the primary time column',
        queryAction: q =>
          q.addSelect(SqlExpression.parse(`TIMESTAMP '2000-01-01 00:00:00'`).as(TIME_COLUMN), {
            insertIndex: 0,
            addToGroupBy: hasGroupBy ? 'start' : undefined,
          }),
      },
    ];
  }
}

const GRANULARITIES: string[] = ['hour', 'day', 'month', 'all'];

type Mode = 'table' | 'list' | 'sql';

interface EditorColumn {
  index: number;
  expression?: SqlExpression;
  type: 'dimension' | 'metric';
  dirty?: boolean;
}

export interface SchemaStepProps {
  queryString: string;
  onQueryStringChange(queryString: string): void;
  enableAnalyze: boolean;
  goToQuery: () => void;
  onBack(): void;
  onDone(): void | Promise<void>;
  extraCallout?: JSX.Element;
}

export const SchemaStep = function SchemaStep(props: SchemaStepProps) {
  const {
    queryString,
    onQueryStringChange,
    enableAnalyze,
    goToQuery,
    onBack,
    onDone,
    extraCallout,
  } = props;
  const [mode, setMode] = useState<Mode>('table');
  const [columnSearch, setColumnSearch] = useState('');
  const [editorColumn, setEditorColumn] = useState<EditorColumn | undefined>();
  const [showAddFilterEditor, setShowAddFilterEditor] = useState(false);
  const [filterInEditor, setFilterInEditor] = useState<SqlExpression | undefined>();
  const [showRollupConfirm, setShowRollupConfirm] = useState(false);
  const [showRollupAnalysisPane, setShowRollupAnalysisPane] = useState(false);
  const [showDestinationDialog, setShowDestinationDialog] = useState(false);
  const lastWorkingQueryPattern = useRef<IngestQueryPattern | undefined>();

  const columnFilter = useCallback(
    (columnName: string) => caseInsensitiveContains(columnName, columnSearch),
    [columnSearch],
  );

  const { ingestQueryPattern, ingestPatternError, parsedQuery } = useMemo(
    () => digestQueryString(queryString),
    [queryString],
  );

  const updatePattern = useCallback(
    (ingestQueryPattern: IngestQueryPattern) => {
      onQueryStringChange(ingestQueryPatternToQuery(ingestQueryPattern).toString());
    },
    [onQueryStringChange],
  );

  const effectiveMode: Mode = ingestQueryPattern ? mode : 'sql';

  const handleQueryAction = usePermanentCallback((queryAction: QueryAction) => {
    if (!parsedQuery) return;
    setEditorColumn(undefined);
    onQueryStringChange(parsedQuery.apply(queryAction).toString());
  });

  const handleModeSelect = (newMode: Mode) => {
    if (newMode === 'sql' && editorColumn) {
      if (editorColumn.dirty) {
        AppToaster.show({
          message:
            'Please save or discard the changes in the column editor before switching to the SQL tab.',
          intent: Intent.WARNING,
        });
        return;
      }

      setEditorColumn(undefined);
    }

    setMode(newMode);
  };

  const handleColumnSelect = usePermanentCallback((index: number) => {
    if (!ingestQueryPattern) return;

    if (editorColumn?.dirty) {
      AppToaster.show({
        message:
          'Please save or discard the changes in the column editor before switching columns.',
        intent: Intent.WARNING,
      });
      return;
    }

    if (editorColumn?.index === index) {
      setEditorColumn(undefined);
      return;
    }

    const expression = getQueryPatternExpression(ingestQueryPattern, index);
    const expressionType = getQueryPatternExpressionType(ingestQueryPattern, index);
    if (!expression || !expressionType) return;
    setEditorColumn({
      index,
      expression,
      type: expressionType,
    });
  });

  function handleNewColumnOfType(type: 'dimension' | 'metric') {
    if (editorColumn?.dirty) {
      AppToaster.show({
        message: 'Please save or discard the changes in the column editor.',
        intent: Intent.WARNING,
      });
      return;
    }

    setEditorColumn({
      index: -1,
      type,
    });
  }

  const selectedColumnIndex = editorColumn ? editorColumn.index : -1;

  // Use this direct DOM manipulation via d3 to avoid re-rendering the table when the selection changes
  useLayoutEffect(() => {
    if (mode !== 'table') return;
    selectAll('.preview-table .rt-th').classed('selected', false);
    if (selectedColumnIndex !== -1) {
      select(`.preview-table .rt-th.column${selectedColumnIndex}`).classed('selected', true);
    }
  }, [mode, selectedColumnIndex, columnSearch]);

  function toggleRollup() {
    if (!ingestQueryPattern) return;

    if (ingestQueryPattern.metrics) {
      updatePattern({ ...ingestQueryPattern, metrics: undefined });
    } else {
      const countExpression = ingestQueryPattern.dimensions.find(groupedExpression =>
        oneOf(groupedExpression.getOutputName(), 'count', '__count'),
      );

      updatePattern({
        ...ingestQueryPattern,
        dimensions: without(ingestQueryPattern.dimensions, countExpression),
        metrics: [
          (countExpression ? F.sum(countExpression.getUnderlyingExpression()) : F.count()).as(
            countExpression?.getOutputName() || 'count',
          ),
        ],
      });
    }
  }

  const [existingTableState] = useQueryManager<string, string[]>({
    initQuery: '',
    processQuery: async (_: string, _cancelToken) => {
      // Check if datasource already exists
      const tables = await queryDruidSql({
        query: `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'druid' ORDER BY TABLE_NAME ASC`,
        resultFormat: 'array',
      });

      return tables.map(t => t[0]);
    },
  });

  const sampleExternalConfig = useLastDefinedDeep(
    ingestQueryPattern && mode !== 'sql' // Only sample the data if we are not in the SQL tab live editing the SQL
      ? ingestQueryPattern.mainExternalConfig
      : undefined,
  );

  const [sampleState] = useQueryManager<ExternalConfig, QueryResult, Execution>({
    query: sampleExternalConfig,
    processQuery: async sampleExternalConfig => {
      const sampleResponse = await postToSampler(
        {
          type: 'index_parallel',
          spec: {
            ioConfig: {
              type: 'index_parallel',
              inputSource: sampleExternalConfig.inputSource,
              inputFormat: deepSet(sampleExternalConfig.inputFormat, 'keepNullColumns', true),
            },
            dataSchema: {
              dataSource: 'sample',
              timestampSpec: DETECTION_TIMESTAMP_SPEC,
              dimensionsSpec: {
                dimensions: filterMap(sampleExternalConfig.signature, s => {
                  const columnName = s.getColumnName();
                  if (columnName === TIME_COLUMN) return;
                  if (s.columnType.isArray()) {
                    return {
                      type: 'auto',
                      castToType: s.columnType.getNativeType().toUpperCase(),
                      name: columnName,
                    };
                  } else {
                    const t = s.columnType.getNativeType();
                    return {
                      name: columnName,
                      type: t === 'COMPLEX<json>' ? 'json' : t,
                    };
                  }
                }),
              },
              granularitySpec: {
                rollup: false,
              },
            },
          },
          samplerConfig: {
            numRows: 50,
            timeoutMs: 15000,
          },
        },
        'sample',
      );

      const columns = getHeaderFromSampleResponse(sampleResponse).map(({ name, type }) => {
        return new Column({
          name,
          nativeType: type,
          sqlType: name === TIME_COLUMN ? 'TIMESTAMP' : SqlType.fromNativeType(type).toString(),
        });
      });

      return new QueryResult({
        header: columns,
        rows: filterMap(sampleResponse.data, r => {
          const { parsed } = r;
          if (!parsed) return;
          return columns.map(({ name }) => parsed[name]);
        }),
      });
    },
  });

  const sampleDataQuery = useMemo(() => {
    if (!sampleState.data) return;
    return sampleDataToQuery(sampleState.data);
  }, [sampleState.data]);

  const previewQueryString = useLastDefined(
    ingestQueryPattern && mode !== 'sql' && sampleDataQuery
      ? ingestQueryPatternToQuery(ingestQueryPattern, true, sampleDataQuery).toString()
      : undefined,
  );

  const [previewResultState] = useQueryManager<string, QueryResult, Execution>({
    query: previewQueryString,
    processQuery: async (previewQueryString, cancelToken) => {
      if (WorkbenchQuery.isTaskEngineNeeded(previewQueryString)) {
        return extractResult(
          await submitTaskQuery({
            query: previewQueryString,
            context: {
              groupByEnableMultiValueUnnesting: false,
              finalizeAggregations: false,
              sqlOuterLimit: 25,
            },
            cancelToken,
          }),
        );
      } else {
        let result: QueryResult;
        try {
          result = await queryRunner.runQuery({
            query: previewQueryString,
            extraQueryContext: { sqlOuterLimit: 25, sqlStringifyArrays: false },
            cancelToken,
          });
        } catch (e) {
          throw new DruidError(e);
        }

        return result.attachQuery({}, SqlQuery.maybeParse(previewQueryString));
      }
    },
    backgroundStatusCheck: executionBackgroundResultStatusCheck,
  });

  useEffect(() => {
    if (!previewResultState.data) return;
    lastWorkingQueryPattern.current = ingestQueryPattern;
    // eslint-disable-next-line react-hooks/exhaustive-deps -- excluding 'ingestQueryPattern'
  }, [previewResultState]);

  const unusedColumns = ingestQueryPattern
    ? ingestQueryPattern.mainExternalConfig.signature.filter(columnDeclaration => {
        const columnName = columnDeclaration.getColumnName();
        return (
          !ingestQueryPattern.dimensions.some(d => d.containsColumnName(columnName)) &&
          !ingestQueryPattern.metrics?.some(m => m.containsColumnName(columnName))
        );
      })
    : [];

  const timeColumn =
    ingestQueryPattern &&
    ingestQueryPattern.dimensions.find(d => d.getOutputName() === TIME_COLUMN);

  const timeSuggestions =
    previewResultState.data && parsedQuery
      ? getTimeSuggestions(previewResultState.data, parsedQuery)
      : [];

  const previewResultSomeData = previewResultState.getSomeData();

  return (
    <TitleFrame
      className="schema-step"
      title="Load data"
      subtitle="Configure schema"
      toolbar={
        <>
          <Popover2
            position="bottom"
            content={
              <Menu>
                <MenuItem
                  icon={IconNames.PLUS}
                  text="Add filter"
                  onClick={() => setShowAddFilterEditor(true)}
                />
                {ingestQueryPattern && ingestQueryPattern.filters.length > 0 && (
                  <>
                    <MenuDivider />
                    {ingestQueryPattern.filters.map((filter, i) => (
                      <MenuItem
                        key={i}
                        icon={IconNames.FILTER}
                        text={filter.toString()}
                        onClick={() => setFilterInEditor(filter)}
                      />
                    ))}
                  </>
                )}
              </Menu>
            }
          >
            <Button icon={IconNames.FILTER} minimal>
              Filters &nbsp;
              <Tag minimal round>
                {ingestQueryPattern ? ingestQueryPattern.filters.length : '?'}
              </Tag>
            </Button>
          </Popover2>
          {ingestQueryPattern && (
            <Popover2
              position="bottom"
              content={
                <Menu>
                  {timeColumn ? (
                    GRANULARITIES.map(g => (
                      <MenuItem
                        key={g}
                        icon={tickIcon(g === ingestQueryPattern.partitionedBy)}
                        text={g}
                        onClick={() => {
                          if (!ingestQueryPattern) return;
                          updatePattern({
                            ...ingestQueryPattern,
                            partitionedBy: g,
                          });
                        }}
                      />
                    ))
                  ) : (
                    <MenuItem
                      text="Only available when a primary time column is selected"
                      disabled
                    />
                  )}
                </Menu>
              }
            >
              <Button icon={IconNames.SPLIT_COLUMNS} minimal>
                Partition &nbsp;
                <Tag minimal round>
                  {ingestQueryPattern.partitionedBy === 'all'
                    ? 'all time'
                    : ingestQueryPattern.partitionedBy}
                </Tag>
              </Button>
            </Popover2>
          )}
          {ingestQueryPattern && (
            <Popover2
              position="bottom"
              content={
                <Menu>
                  {ingestQueryPattern.clusteredBy.map((p, i) => (
                    <MenuItem
                      key={i}
                      icon={IconNames.MERGE_COLUMNS}
                      text={ingestQueryPattern.dimensions[p].getOutputName()}
                    >
                      <MenuItem
                        icon={IconNames.CROSS}
                        text="Remove"
                        onClick={() =>
                          updatePattern({
                            ...ingestQueryPattern,
                            clusteredBy: without(ingestQueryPattern.clusteredBy, p),
                          })
                        }
                      />
                    </MenuItem>
                  ))}
                  <MenuItem icon={IconNames.PLUS} text="Add column clustering">
                    {filterMap(ingestQueryPattern.dimensions, (dimension, i) => {
                      const outputName = dimension.getOutputName();
                      if (
                        outputName === TIME_COLUMN ||
                        ingestQueryPattern.clusteredBy.includes(i)
                      ) {
                        return;
                      }

                      return (
                        <MenuItem
                          key={i}
                          text={outputName}
                          onClick={() =>
                            updatePattern({
                              ...ingestQueryPattern,
                              clusteredBy: ingestQueryPattern.clusteredBy.concat([i]),
                            })
                          }
                          shouldDismissPopover={false}
                        />
                      );
                    })}
                  </MenuItem>
                </Menu>
              }
            >
              <Button icon={IconNames.MERGE_COLUMNS} minimal>
                Cluster &nbsp;
                <Tag minimal round>
                  {ingestQueryPattern.clusteredBy.length}
                </Tag>
              </Button>
            </Popover2>
          )}
          <Button
            icon={IconNames.COMPRESSED}
            onClick={() => {
              setEditorColumn(undefined); // Clear any selected column if any
              setShowRollupConfirm(true);
            }}
            minimal
          >
            Rollup &nbsp;
            <Tag minimal round>
              {ingestQueryPattern?.metrics ? 'On' : 'Off'}
            </Tag>
          </Button>
          {ingestQueryPattern && existingTableState.data && (
            <Button
              className="destination-button"
              icon={IconNames.MULTI_SELECT}
              minimal
              onClick={() => setShowDestinationDialog(true)}
            >
              {`Datasource: ${ingestQueryPattern.destinationTableName} `}
              <Tag minimal round>
                {getDestinationMode(ingestQueryPattern, existingTableState.data)}
              </Tag>
            </Button>
          )}
        </>
      }
    >
      <div className={classNames('schema-container', { 'with-analysis': showRollupAnalysisPane })}>
        <div className="loader-controls">
          <div className="control-line left">
            <ButtonGroup>
              <Button
                icon={IconNames.TH_LIST}
                text="Table"
                disabled={!ingestQueryPattern}
                active={effectiveMode === 'table'}
                onClick={() => handleModeSelect('table')}
              />
              <Button
                icon={IconNames.LIST_COLUMNS}
                text="List"
                disabled={!ingestQueryPattern}
                active={effectiveMode === 'list'}
                onClick={() => handleModeSelect('list')}
              />
              <Button
                icon={IconNames.APPLICATION}
                text="SQL"
                active={effectiveMode === 'sql'}
                onClick={() => handleModeSelect('sql')}
              />
            </ButtonGroup>
            {enableAnalyze && ingestQueryPattern?.metrics && (
              <Button
                icon={IconNames.LIGHTBULB}
                text="Analyze rollup"
                minimal
                active={showRollupAnalysisPane}
                onClick={() => setShowRollupAnalysisPane(!showRollupAnalysisPane)}
              />
            )}
          </div>
          {effectiveMode !== 'sql' && ingestQueryPattern && (
            <div className="control-line right">
              <Popover2
                className="add-column-control"
                position="bottom"
                content={
                  <Menu>
                    {ingestQueryPattern.metrics ? (
                      <>
                        <MenuItem
                          icon={IconNames.PLUS}
                          text="Custom dimension"
                          onClick={() => handleNewColumnOfType('dimension')}
                        />
                        <MenuItem
                          icon={IconNames.PLUS}
                          text="Custom metric"
                          onClick={() => handleNewColumnOfType('metric')}
                        />
                      </>
                    ) : (
                      <MenuItem
                        icon={IconNames.PLUS}
                        text="Custom column"
                        onClick={() => handleNewColumnOfType('dimension')}
                      />
                    )}
                    <MenuDivider />
                    {unusedColumns.length ? (
                      unusedColumns.map((columnDeclaration, i) => {
                        const columnName = columnDeclaration.getColumnName();
                        return (
                          <MenuItem
                            key={i}
                            icon={dataTypeToIcon(columnDeclaration.columnType.getNativeType())}
                            text={columnName}
                            onClick={() => {
                              handleQueryAction(q =>
                                q.addSelect(
                                  C(columnName),
                                  ingestQueryPattern.metrics
                                    ? { insertIndex: 'last-grouping', addToGroupBy: 'end' }
                                    : {},
                                ),
                              );
                            }}
                          />
                        );
                      })
                    ) : (
                      <MenuItem icon={IconNames.BLANK} text="No column suggestions" disabled />
                    )}
                  </Menu>
                }
              >
                <Button className="add-column" icon={IconNames.PLUS} text="Add column" />
              </Popover2>
              <ClearableInput
                className="column-filter-control"
                value={columnSearch}
                placeholder="Search columns"
                onChange={setColumnSearch}
              />
            </div>
          )}
          {effectiveMode === 'sql' && (
            <div className="control-line right">
              <Button rightIcon={IconNames.ARROW_TOP_RIGHT} onClick={goToQuery}>
                Open in <strong>Query</strong> view
              </Button>
            </div>
          )}
        </div>
        <div className="preview">
          {effectiveMode === 'table' && (
            <>
              {previewResultState.isError() ? (
                <PreviewError
                  errorMessage={String(previewResultState.getErrorMessage())}
                  onRevert={
                    lastWorkingQueryPattern.current &&
                    (() => {
                      if (!lastWorkingQueryPattern.current) return;
                      updatePattern(lastWorkingQueryPattern.current);
                    })
                  }
                />
              ) : (
                previewResultSomeData && (
                  <PreviewTable
                    queryResult={previewResultSomeData}
                    onQueryAction={handleQueryAction}
                    columnFilter={columnFilter}
                    selectedColumnIndex={-1}
                    onEditColumn={handleColumnSelect}
                  />
                )
              )}
              {previewResultState.isLoading() && <Loader />}
              {previewResultState.isInit() && sampleState.isLoading() && (
                <Loader loadingText="Loading data sample..." />
              )}
              {sampleState.getErrorMessage() && 'Sample error'}
            </>
          )}
          {effectiveMode === 'list' &&
            ingestQueryPattern &&
            (previewResultState.isError() ? (
              <PreviewError
                errorMessage={String(previewResultState.getErrorMessage())}
                onRevert={
                  lastWorkingQueryPattern.current &&
                  (() => {
                    if (!lastWorkingQueryPattern.current) return;
                    updatePattern(lastWorkingQueryPattern.current);
                  })
                }
              />
            ) : (
              previewResultSomeData && (
                <ColumnList
                  queryResult={previewResultSomeData}
                  columnFilter={columnFilter}
                  selectedColumnIndex={selectedColumnIndex}
                  onEditColumn={handleColumnSelect}
                />
              )
            ))}
          {effectiveMode === 'sql' && (
            <FlexibleQueryInput
              queryString={queryString}
              onQueryStringChange={onQueryStringChange}
              columnMetadata={undefined}
              leaveBackground
            />
          )}
        </div>
        <div className="controls">
          <div className="top-controls">
            {!editorColumn && (
              <FormGroup>
                <Callout>
                  <p>
                    Each column in Druid must have an assigned type (string, long, float, double,
                    complex, etc).
                  </p>
                  <p>
                    Types are implicitly determined for your columns. If you want to change the type
                    of a column you can cast it to a specific type. You can do that by clicking on a
                    column header.
                  </p>
                  <LearnMore href={`${getLink('DOCS')}/ingestion/schema-design.html`} />
                </Callout>
              </FormGroup>
            )}
            {editorColumn && ingestQueryPattern && (
              <>
                <ColumnEditor
                  key={editorColumn.index}
                  initExpression={editorColumn.expression}
                  onApply={newColumn => {
                    if (!editorColumn) return;
                    updatePattern(
                      changeQueryPatternExpression(
                        ingestQueryPattern,
                        editorColumn.index,
                        editorColumn.type,
                        newColumn,
                      ),
                    );
                  }}
                  onCancel={() => setEditorColumn(undefined)}
                  dirty={() => {
                    if (!editorColumn.dirty) return;
                    setEditorColumn({ ...editorColumn, dirty: true });
                  }}
                  queryResult={previewResultState.data}
                  headerIndex={editorColumn.index}
                />
                {editorColumn.index !== -1 && (
                  <ColumnActions
                    queryResult={previewResultState.data}
                    headerIndex={editorColumn.index}
                    onQueryAction={handleQueryAction}
                  />
                )}
              </>
            )}
            {ingestPatternError && (
              <FormGroup>
                <Callout intent={Intent.DANGER}>{ingestPatternError}</Callout>
              </FormGroup>
            )}
            {!editorColumn && timeSuggestions.length > 0 && (
              <Callout
                className="time-column-warning"
                intent={Intent.WARNING}
                title="No __time column defined"
              >
                {timeSuggestions.map((timeSuggestion, i) => (
                  <FormGroup key={i}>
                    <Button
                      icon={IconNames.CLEAN}
                      text={timeSuggestion.label}
                      intent={Intent.SUCCESS}
                      onClick={() => handleQueryAction(timeSuggestion.queryAction)}
                    />
                  </FormGroup>
                ))}
                <AnchorButton
                  icon={IconNames.HELP}
                  text="Learn more..."
                  href={`${getLink('DOCS')}/ingestion/schema-model.html#primary-timestamp`}
                  target="_blank"
                  intent={Intent.WARNING}
                  minimal
                />
              </Callout>
            )}
          </div>
          <div className="bottom-controls">
            {extraCallout && (
              <FormGroup>
                <Callout>{extraCallout}</Callout>
              </FormGroup>
            )}
            <div className="prev-next-bar">
              <Button className="back" icon={IconNames.ARROW_LEFT} text="Back" onClick={onBack} />
              <Button
                className="next"
                icon={IconNames.CLOUD_UPLOAD}
                text="Start loading data"
                intent={Intent.PRIMARY}
                onClick={() => void onDone()}
              />
            </div>
          </div>
        </div>
        {showRollupAnalysisPane && ingestQueryPattern && (
          <RollupAnalysisPane
            dimensions={ingestQueryPattern.dimensions}
            seedQuery={ingestQueryPatternToQuery(ingestQueryPattern, true, sampleDataQuery)}
            queryResult={previewResultState.data}
            onEditColumn={handleColumnSelect}
            onClose={() => setShowRollupAnalysisPane(false)}
          />
        )}
        {showAddFilterEditor && ingestQueryPattern && (
          <ExpressionEditorDialog
            title="Add filter"
            onSave={newExpression =>
              updatePattern({
                ...ingestQueryPattern,
                filters: ingestQueryPattern.filters.concat(newExpression),
              })
            }
            onClose={() => setShowAddFilterEditor(false)}
          />
        )}
        {filterInEditor && ingestQueryPattern && (
          <ExpressionEditorDialog
            title="Edit filter"
            expression={filterInEditor}
            onSave={newFilter =>
              updatePattern({
                ...ingestQueryPattern,
                filters: change(ingestQueryPattern.filters, filterInEditor, newFilter),
              })
            }
            onDelete={() =>
              updatePattern({
                ...ingestQueryPattern,
                filters: without(ingestQueryPattern.filters, filterInEditor),
              })
            }
            onClose={() => setFilterInEditor(undefined)}
          />
        )}
        {showRollupConfirm && ingestQueryPattern && (
          <AsyncActionDialog
            action={async () => {
              await wait(100); // A hack to make it async. Revisit
              toggleRollup();
            }}
            confirmButtonText={`Yes - ${ingestQueryPattern.metrics ? 'disable' : 'enable'} rollup`}
            successText={`Rollup was ${
              ingestQueryPattern.metrics ? 'disabled' : 'enabled'
            }. Schema has been updated.`}
            failText="Could change rollup"
            intent={Intent.WARNING}
            onClose={() => setShowRollupConfirm(false)}
          >
            <p>{`Are you sure you want to ${
              ingestQueryPattern.metrics ? 'disable' : 'enable'
            } rollup?`}</p>
            <p>Making this change will reset any work you have done in this section.</p>
          </AsyncActionDialog>
        )}
        {showDestinationDialog && ingestQueryPattern && existingTableState.data && (
          <DestinationDialog
            existingTables={existingTableState.data}
            ingestQueryPattern={ingestQueryPattern}
            changeIngestQueryPattern={updatePattern}
            onClose={() => setShowDestinationDialog(false)}
          />
        )}
      </div>
    </TitleFrame>
  );
};
