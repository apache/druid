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

import { Button, Classes, Dialog, Tab, Tabs } from '@blueprintjs/core';
import * as echarts from 'echarts';
import * as JSONBig from 'json-bigint-native';
import type { JSX } from 'react';
import React, { useEffect, useRef } from 'react';

import { Loader } from '../../../components';
import type { DruidEngine, QueryContext, QueryWithContext } from '../../../druid-models';
import { isEmptyContext } from '../../../druid-models';
import { useQueryManager } from '../../../hooks';
// import { Api } from '../../../singletons';
import type { QueryExplanation } from '../../../utils';
import { nonEmptyArray } from '../../../utils';

import { debugData } from './debug-data.mock';

import './explain-and-analyse-dialog.scss';

// function isExplainQuery(query: string): boolean {
//   return /^\s*EXPLAIN\sAND\sANALYSE\sPLAN\sFOR/im.test(query);
// }

// function wrapInExplainIfNeeded(query: string): string {
//   if (isExplainQuery(query)) return query;
//   return `EXPLAIN AND ANALYSE PLAN FOR ${query}`;
// }

export interface QueryContextEngine extends QueryWithContext {
  engine: DruidEngine;
}

export interface ExplainAndAnalyseDialogProps {
  queryWithContext: QueryContextEngine;
  mandatoryQueryContext?: Record<string, any>;
  onClose: () => void;
  openQueryLabel: string | undefined;
  onOpenQuery?: (queryString: string) => void;
}

export const ExplainAndAnalyseDialog = React.memo(function ExplainAndAnalyseDialog(
  props: ExplainAndAnalyseDialogProps,
) {
  const { queryWithContext, onClose, mandatoryQueryContext } = props;

  const [explainState] = useQueryManager<QueryContextEngine, QueryExplanation[] | string>({
    processQuery: async queryWithContext => {
      const { queryContext, wrapQueryLimit } = queryWithContext;

      let context: QueryContext | undefined;
      if (!isEmptyContext(queryContext) || wrapQueryLimit || mandatoryQueryContext) {
        context = {
          ...queryContext,
          ...(mandatoryQueryContext || {}),
          useNativeQueryExplain: true,
        };
        if (typeof wrapQueryLimit !== 'undefined') {
          context.sqlOuterLimit = wrapQueryLimit + 1;
        }
      }

      // const payload: any = {
      //   query: wrapInExplainIfNeeded(queryString),
      //   context,
      // };

      // let result: any[];
      // try {
      //   result =
      //     engine === 'sql-msq-task'
      //       ? (await Api.instance.post(`/druid/v2/sql/task`, payload)).data
      //       : await queryDruidSql(payload);
      // } catch (e) {
      //   throw new Error(getDruidErrorMessage(e));
      // }

      // const plan = deepGet(result, '0.PLAN');
      // if (typeof plan !== 'string') {
      //   throw new Error(`unexpected result from ${engine} API`);
      // }

      // wait for 5 seconds to simulate loading time
      await new Promise(resolve => setTimeout(resolve, 1000));

      const stringifiedData = JSONBig.stringify(debugData, undefined, 2);

      try {
        return JSONBig.parse(stringifiedData);
      } catch {
        return {};
      }
    },
    initQuery: queryWithContext,
  });

  let content: JSX.Element;

  const { loading, error: explainError, data: explainResult } = explainState;

  function renderQueryExplanation(data: any) {
    return (
      <div className="query-explanation">
        <Dendrogram data={data} />
      </div>
    );
  }

  if (loading) {
    content = <Loader />;
  } else if (explainError) {
    content = <div>{explainError.message}</div>;
  } else if (!explainResult) {
    content = <div />;
  } else if (nonEmptyArray(explainResult)) {
    if (explainResult.length === 1) {
      content = renderQueryExplanation(explainResult[0]);
    } else {
      content = (
        <Tabs animate renderActiveTabPanelOnly vertical>
          {explainResult.map((queryExplanation, i) => (
            <Tab
              id={i}
              key={i}
              title={`Query ${i + 1}`}
              panel={renderQueryExplanation(queryExplanation)}
            />
          ))}
          <Tabs.Expander />
        </Tabs>
      );
    }
  } else {
    content = <div className="generic-result">{String(explainResult)}</div>;
  }

  return (
    <Dialog className="explain-and-analyse-dialog" isOpen onClose={onClose} title="Query analysis">
      <div className={Classes.DIALOG_BODY}>{content}</div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" onClick={onClose} />
        </div>
      </div>
    </Dialog>
  );
});

interface DendrogramProps {
  data: any;
}

const Dendrogram = (props: DendrogramProps) => {
  const { data } = props;

  const myChart = useRef<echarts.EChartsType | undefined>();
  const container = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!container.current) return;
    myChart.current = echarts.init(container.current);

    return () => {
      myChart.current?.dispose();
    };
  }, []);

  useEffect(() => {
    myChart.current?.setOption({
      tooltip: {
        trigger: 'item',
        triggerOn: 'mousemove',
        formatter: (params: any) => {
          const { data } = params;
          return `<pre style="font-size: 10px">${JSONBig.stringify(
            data?.debugInfo,
            undefined,
            2,
          )}</pre>`;
        },
        position: function (pos: any, _params: any, _dom: any, _rect: any, size: any) {
          // tooltip will be fixed on the right if mouse hovering on the left,
          // and on the left if hovering on the right.
          const obj: any = { top: 60 };
          obj[['left', 'right'][+(pos[0] < size.viewSize[0] / 2)]] = 5;
          return obj;
        },
      },
      series: [
        {
          type: 'tree',

          data: [data],

          top: '1%',
          left: '7%',
          bottom: '1%',
          right: '20%',

          symbolSize: 7,

          label: {
            position: 'left',
            verticalAlign: 'middle',
            align: 'right',
            fontSize: 9,
          },

          leaves: {
            label: {
              position: 'right',
              verticalAlign: 'middle',
              align: 'left',
            },
          },

          emphasis: {
            focus: 'descendant',
          },

          expandAndCollapse: true,
          animationDuration: 550,
          animationDurationUpdate: 750,
        },
      ],
    });
  }, [data]);

  // TODO handle resize (use resize observer if possible)

  return <div ref={container} style={{ width: '100%', height: '100%' }} />;
};
