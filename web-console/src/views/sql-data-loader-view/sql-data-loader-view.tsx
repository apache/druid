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

import { Card, Icon, IconName, Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { SqlQuery } from 'druid-query-toolkit';
import React, { useState } from 'react';

import {
  Execution,
  ExternalConfig,
  externalConfigToIngestQueryPattern,
  ingestQueryPatternToQuery,
  QueryWithContext,
} from '../../druid-models';
import { submitTaskQuery } from '../../helpers';
import { useLocalStorageState } from '../../hooks';
import { AppToaster } from '../../singletons';
import { deepDelete, LocalStorageKeys } from '../../utils';
import { InputFormatStep } from '../workbench-view/input-format-step/input-format-step';
import { InputSourceStep } from '../workbench-view/input-source-step/input-source-step';
import { MaxTasksButton } from '../workbench-view/max-tasks-button/max-tasks-button';

import { IngestionProgressDialog } from './ingestion-progress-dialog/ingestion-progress-dialog';
import { SchemaStep } from './schema-step/schema-step';
import { TitleFrame } from './title-frame/title-frame';

import './sql-data-loader-view.scss';

interface LoaderContent extends QueryWithContext {
  id?: string;
}

export interface SqlDataLoaderViewProps {
  goToQuery(queryWithContext: QueryWithContext): void;
  goToIngestion(taskId: string): void;
}

export const SqlDataLoaderView = React.memo(function SqlDataLoaderView(
  props: SqlDataLoaderViewProps,
) {
  const { goToQuery, goToIngestion } = props;
  const [externalConfigStep, setExternalConfigStep] = useState<Partial<ExternalConfig>>({});
  const [content, setContent] = useLocalStorageState<LoaderContent | undefined>(
    LocalStorageKeys.SQL_DATA_LOADER_CONTENT,
  );
  const [needVerify, setNeedVerify] = useState(Boolean(content && !content.id));

  const { inputSource, inputFormat } = externalConfigStep;

  function renderActionCard(icon: IconName, title: string, caption: string, onClick: () => void) {
    return (
      <Card className="spec-card" interactive onClick={onClick} elevation={1}>
        <Icon className="spec-card-icon" icon={icon} size={30} />
        <div className="spec-card-header">
          {title}
          <div className="spec-card-caption">{caption}</div>
        </div>
      </Card>
    );
  }

  return (
    <div className="sql-data-loader-view">
      {needVerify ? (
        <div className="resume-step">
          {renderActionCard(
            IconNames.ASTERISK,
            `Start a new flow`,
            `Begin a new SQL ingestion flow.`,
            () => {
              setContent(undefined);
              setNeedVerify(false);
            },
          )}
          {renderActionCard(
            IconNames.REPEAT,
            `Continue from previous flow`,
            `Go back to the most recent SQL ingestion flow you were working on.`,
            () => {
              setNeedVerify(false);
            },
          )}
        </div>
      ) : content ? (
        <SchemaStep
          queryString={content.queryString}
          onQueryStringChange={queryString => setContent({ ...content, queryString })}
          enableAnalyze={false}
          goToQuery={() => goToQuery(content)}
          onBack={() => setContent(undefined)}
          onDone={async () => {
            const ingestDatasource = SqlQuery.parse(content.queryString)
              .getIngestTable()
              ?.getTable();

            if (!ingestDatasource) {
              AppToaster.show({ message: `Must have an ingest datasource`, intent: Intent.DANGER });
              return;
            }

            try {
              const execution = await submitTaskQuery({
                query: content.queryString,
                context: content.queryContext,
              });

              const taskId = execution instanceof Execution ? execution.id : execution.state.id;

              setContent({ ...content, id: taskId });
            } catch (e) {
              AppToaster.show({
                message: `Error submitting task: ${e.message}`,
                intent: Intent.DANGER,
              });
            }
          }}
          extraCallout={
            <MaxTasksButton
              queryContext={content.queryContext || {}}
              changeQueryContext={queryContext => setContent({ ...content, queryContext })}
              minimal
            />
          }
        />
      ) : inputFormat && inputSource ? (
        <TitleFrame title="Load data" subtitle="Parse">
          <InputFormatStep
            inputSource={inputSource}
            initInputFormat={inputFormat}
            doneButton={false}
            onSet={({ inputFormat, signature, isArrays, timeExpression }) => {
              setContent({
                queryString: ingestQueryPatternToQuery(
                  externalConfigToIngestQueryPattern(
                    { inputSource, inputFormat, signature },
                    isArrays,
                    timeExpression,
                  ),
                ).toString(),
                queryContext: {
                  finalizeAggregations: false,
                  groupByEnableMultiValueUnnesting: false,
                },
              });
            }}
            altText="Skip the wizard and continue with custom SQL"
            onAltSet={({ inputFormat, signature, isArrays, timeExpression }) => {
              goToQuery({
                queryString: ingestQueryPatternToQuery(
                  externalConfigToIngestQueryPattern(
                    { inputSource, inputFormat, signature },
                    isArrays,
                    timeExpression,
                  ),
                ).toString(),
              });
            }}
            onBack={() => {
              setExternalConfigStep({ inputSource });
            }}
          />
        </TitleFrame>
      ) : (
        <TitleFrame title="Load data" subtitle="Select input type">
          <InputSourceStep
            initInputSource={inputSource}
            mode="sampler"
            onSet={(inputSource, inputFormat) => {
              setExternalConfigStep({ inputSource, inputFormat });
            }}
          />
        </TitleFrame>
      )}
      {content?.id && (
        <IngestionProgressDialog
          taskId={content.id}
          goToQuery={goToQuery}
          goToIngestion={goToIngestion}
          onReset={() => setContent(undefined)}
          onClose={() => setContent(deepDelete(content, 'id'))}
        />
      )}
    </div>
  );
});
