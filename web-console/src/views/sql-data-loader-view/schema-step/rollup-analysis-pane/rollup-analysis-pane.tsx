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

import { Button, Callout, Intent, Tag } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { CancelToken } from 'axios';
import { QueryResult, SqlExpression, SqlFunction, SqlQuery } from 'druid-query-toolkit';
import React, { useEffect } from 'react';

import { Execution } from '../../../../druid-models';
import { executionBackgroundStatusCheck, submitTaskQuery } from '../../../../helpers';
import { useQueryManager } from '../../../../hooks';
import { filterMap, formatPercentClapped, IntermediateQueryState } from '../../../../utils';

import './rollup-analysis-pane.scss';

const CAST_AS_VARCHAR_TEMPLATE = SqlExpression.parse(`NVL(CAST(? AS VARCHAR), '__NULL__')`);
const CAST_TIMESTAMP_TEMPLATE = SqlExpression.parse(`CAST(TIMESTAMP_TO_MILLIS(?) AS VARCHAR)`);
const CAST_ARRAY_TEMPLATE = SqlExpression.parse(`MV_TO_STRING(?, ',')`);

function expressionCastToString(ex: SqlExpression): SqlExpression {
  if (ex instanceof SqlFunction) {
    switch (ex.getEffectiveFunctionName()) {
      case 'TIME_PARSE':
        return CAST_TIMESTAMP_TEMPLATE.fillPlaceholders([ex]);

      case 'MV_TO_ARRAY':
        return CAST_ARRAY_TEMPLATE.fillPlaceholders([ex]);
    }
  }

  return CAST_AS_VARCHAR_TEMPLATE.fillPlaceholders([ex]);
}

function countDistinct(ex: SqlExpression): SqlExpression {
  return SqlFunction.simple('APPROX_COUNT_DISTINCT_DS_HLL', [ex]);
}

function within(a: number, b: number, percent: number): boolean {
  return Math.abs(a - b) / Math.abs(Math.min(a, b)) < percent;
}

function pairs(n: number): [number, number][] {
  const p: [number, number][] = [];
  for (let i = 0; i < n - 1; i++) {
    for (let j = i + 1; j < n; j++) {
      p.push([i, j]);
    }
  }
  return p;
}

interface AnalyzeQuery {
  expressions: readonly SqlExpression[];
  deep: boolean;
}

interface AnalyzeResult {
  deep: boolean;
  count: number;
  overall: number;
  counts: number[];
  pairCounts: number[];
  implications: Implication[];
}

function queryResultToAnalysis(
  analyzeQuery: AnalyzeQuery,
  queryResult: QueryResult,
): AnalyzeResult {
  const row = queryResult.rows[0];
  const numExpressions = analyzeQuery.expressions.length;

  const counts = row.slice(2, numExpressions + 2);
  const pairCounts = row.slice(numExpressions + 2);
  return {
    deep: analyzeQuery.deep,
    count: row[0],
    overall: row[1],
    counts,
    pairCounts,
    implications: getImplications(counts, pairCounts),
  };
}

type ImplicationType = 'implication' | 'equivalent';

interface Implication {
  type: ImplicationType;
  a: number;
  b: number;
}

function getImplications(counts: number[], pairCounts: number[]): Implication[] {
  const pairIndexes = pairs(counts.length);

  return filterMap(pairIndexes, ([i, j], index): Implication | undefined => {
    const pairCount = pairCounts[index];
    if (counts[i] < 2) return;
    if (counts[j] < 2) return;
    const iImpliesJ = within(counts[i], pairCount, 0.01);
    const jImpliesI = within(counts[j], pairCount, 0.01);

    if (iImpliesJ && jImpliesI) {
      return { type: 'equivalent', a: i, b: j };
    } else if (iImpliesJ) {
      return { type: 'implication', a: i, b: j };
    } else if (jImpliesI) {
      return { type: 'implication', a: j, b: i };
    }

    return;
  }).sort((x, y) => {
    const diffA = x.a - y.a;
    if (diffA !== 0) return diffA;
    return x.b - y.b;
  });
}

interface RollupAnalysisPaneProps {
  dimensions: readonly SqlExpression[];
  seedQuery: SqlQuery;
  queryResult: QueryResult | undefined;
  onEditColumn(columnIndex: number): void;
  onClose(): void;
}

export const RollupAnalysisPane = React.memo(function RollupAnalysisPane(
  props: RollupAnalysisPaneProps,
) {
  const { dimensions, seedQuery, queryResult, onEditColumn, onClose } = props;

  const [analyzeQueryState, analyzeQueryManager] = useQueryManager<
    AnalyzeQuery,
    AnalyzeResult,
    Execution
  >({
    processQuery: async (analyzeQuery: AnalyzeQuery, cancelToken) => {
      const { expressions, deep } = analyzeQuery;

      const groupedAsStrings = expressions.map(ex =>
        expressionCastToString(ex.getUnderlyingExpression()),
      );

      const expressionPairs: SqlExpression[] = deep
        ? pairs(expressions.length).map(([i, j]) =>
            countDistinct(SqlFunction.simple('CONCAT', [groupedAsStrings[i], groupedAsStrings[j]])),
          )
        : [];

      const queryString = seedQuery
        .changeSelectExpressions(groupedAsStrings.map(countDistinct).concat(expressionPairs))
        .addSelect(countDistinct(SqlFunction.simple('CONCAT', groupedAsStrings)), {
          insertIndex: 0,
        })
        .addSelect(SqlFunction.COUNT_STAR, { insertIndex: 0 })
        .changeGroupByExpressions([])
        .changeHavingClause(undefined)
        .changeOrderByClause(undefined)
        .toString();

      const res = await submitTaskQuery({
        query: queryString,
        context: {},
        cancelToken,
      });

      if (res instanceof IntermediateQueryState) return res;

      if (res.result) {
        return queryResultToAnalysis(analyzeQuery, res.result);
      } else {
        throw new Error(res.getErrorMessage() || 'unexpected destination');
      }
    },
    backgroundStatusCheck: async (
      execution: Execution,
      analyzeQuery: AnalyzeQuery,
      cancelToken: CancelToken,
    ) => {
      const res = await executionBackgroundStatusCheck(execution, analyzeQuery, cancelToken);
      if (res instanceof IntermediateQueryState) return res;

      if (res.result) {
        return queryResultToAnalysis(analyzeQuery, res.result);
      } else {
        throw new Error(res.getErrorMessage() || 'unexpected destination');
      }
    },
  });

  const analysis = analyzeQueryState.data;

  useEffect(() => {
    if (!analysis) return;
    analyzeQueryManager.reset();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [queryResult]);

  function renderOverallResult() {
    if (!analysis) return;
    const rollup = analysis.overall / analysis.count;
    return (
      <p>
        {`Estimated size after rollup is `}
        <span className="strong">{formatPercentClapped(rollup)}</span>
        {` of raw data size. `}
        {rollup < 0.8 ? (
          <span className="strong">That is decent.</span>
        ) : (
          <>
            At this rate rollup is <span className="strong">not worth it</span>.
          </>
        )}
      </p>
    );
  }

  function renderDimension(dimensionIndex: number) {
    if (!queryResult) return;
    const outputName = dimensions[dimensionIndex].getOutputName();
    const headerIndex = queryResult.header.findIndex(c => c.name === outputName);
    if (headerIndex === -1) return;

    return (
      <Tag interactive intent={Intent.WARNING} onClick={() => onEditColumn(headerIndex)}>
        {outputName}
      </Tag>
    );
  }

  let singleDimensionSuggestions: JSX.Element[] = [];
  let pairDimensionSuggestions: JSX.Element[] = [];
  let implicationSuggestions: JSX.Element[] = [];
  if (analysis) {
    const { count, counts, pairCounts, implications } = analysis;

    singleDimensionSuggestions = filterMap(counts, (c, i) => {
      const variability = c / count;
      if (variability < 0.8) return;
      return (
        <span className="suggestion" key={`single_${i}`}>
          {renderDimension(i)}
          {` has variability of ${formatPercentClapped(variability)}`}
        </span>
      );
    });

    pairDimensionSuggestions = filterMap(pairs(analysis.counts.length), ([i, j], index) => {
      if (typeof pairCounts[index] !== 'number') return;
      const variability = pairCounts[index] / count;
      if (variability < 0.8) return;
      return (
        <span className="suggestion" key={`pair_${index}`}>
          {renderDimension(i)}
          {' together with '}
          {renderDimension(j)}
          {` has variability of ${formatPercentClapped(variability)}`}
        </span>
      );
    });

    implicationSuggestions = filterMap(dimensions, (_d, i) => {
      if (implications.some(imp => imp.type === 'implication' && imp.b === i)) return;
      return (
        <span className="suggestion" key={`imp_${i}`}>
          {'Remove '}
          {renderDimension(i)}
          {` (variability of ${formatPercentClapped(counts[i] / count)})`}
        </span>
      );
    });
  }

  return (
    <Callout className="rollup-analysis-pane">
      <Button className="close" icon={IconNames.CROSS} onClick={onClose} minimal />
      {analyzeQueryState.isInit() && (
        <>
          <p>
            Use this tool to analyze which dimensions are preventing the data from rolling up
            effectively.
          </p>
          <p>
            <Button
              text="Run rollup analysis"
              onClick={() => {
                analyzeQueryManager.runQuery({ expressions: dimensions, deep: false });
              }}
            />
          </p>
        </>
      )}
      {analyzeQueryState.isLoading() && <p>Loading analysis...</p>}
      {renderOverallResult()}
      {singleDimensionSuggestions.length === 0 && analysis && !analysis.deep && (
        <p>
          <Button
            text="Deep analysis"
            onClick={() => {
              analyzeQueryManager.runQuery({ expressions: dimensions, deep: true });
            }}
          />
        </p>
      )}
      {(singleDimensionSuggestions.length || pairDimensionSuggestions.length > 0) && (
        <>
          <p>Poor rollup is caused by:</p>
          <p>
            {singleDimensionSuggestions}
            {pairDimensionSuggestions}
          </p>
        </>
      )}
      {analysis?.deep &&
        !singleDimensionSuggestions.length &&
        !pairDimensionSuggestions.length &&
        implicationSuggestions.length > 0 && (
          <>
            <p>Possible actions to improve rollup:</p>
            <p>{implicationSuggestions}</p>
          </>
        )}
      {analyzeQueryState.isError() && <p>{analyzeQueryState.getErrorMessage()}</p>}
    </Callout>
  );
});
