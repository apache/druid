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
  Callout,
  Card,
  FormGroup,
  Intent,
  ProgressBar,
  Radio,
  RadioGroup,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import type { QueryResult } from '@druid-toolkit/query';
import { SqlColumnDeclaration } from '@druid-toolkit/query';
import classNames from 'classnames';
import type { JSX } from 'react';
import React, { useEffect, useState } from 'react';

import { AutoForm, ExternalLink } from '../../../components';
import { ShowValueDialog } from '../../../dialogs/show-value-dialog/show-value-dialog';
import type { Execution, ExecutionError, InputFormat, InputSource } from '../../../druid-models';
import {
  externalConfigToTableExpression,
  getIngestionImage,
  getIngestionTitle,
  guessSimpleInputFormat,
  INPUT_SOURCE_FIELDS,
  issueWithSampleData,
  PLACEHOLDER_TIMESTAMP_SPEC,
} from '../../../druid-models';
import {
  executionBackgroundResultStatusCheck,
  extractResult,
  submitTaskQuery,
} from '../../../helpers';
import { useQueryManager } from '../../../hooks';
import { AppToaster, UrlBaser } from '../../../singletons';
import { filterMap, IntermediateQueryState } from '../../../utils';
import type { SampleSpec } from '../../../utils/sampler';
import { postToSampler } from '../../../utils/sampler';

import { EXAMPLE_INPUTS } from './example-inputs';
import { InputSourceInfo } from './input-source-info';

import './input-source-step.scss';

function resultToInputFormat(result: QueryResult): InputFormat {
  if (!result.rows.length) throw new Error('No data returned from sample query');
  return guessSimpleInputFormat(result.rows.map((r: any) => r[0]));
}

const BOGUS_LIST_DELIMITER = '56616469-6de2-9da4-efb8-8f416e6e6965'; // Just a UUID to disable the list delimiter, let's hope we do not see this UUID in the data
const ROWS_TO_SAMPLE = 50;

export interface InputSourceStepProps {
  initInputSource: Partial<InputSource> | undefined;
  mode: 'sampler' | 'msq';
  onSet(
    inputSource: InputSource,
    inputFormat: InputFormat,
    partitionedByHint: string | undefined,
  ): void;
}

export const InputSourceStep = React.memo(function InputSourceStep(props: InputSourceStepProps) {
  const { initInputSource, mode, onSet } = props;

  const [stackToShow, setStackToShow] = useState<string | undefined>();
  const [inputSource, setInputSource] = useState<Partial<InputSource> | string | undefined>(
    initInputSource,
  );
  const exampleInput = EXAMPLE_INPUTS.find(({ name }) => name === inputSource);

  const [guessedInputFormatState, connectQueryManager] = useQueryManager<
    { inputSource: InputSource; suggestedInputFormat?: InputFormat },
    InputFormat,
    Execution
  >({
    processQuery: async ({ inputSource, suggestedInputFormat }, cancelToken) => {
      let sampleLines: string[];
      if (mode === 'sampler') {
        const sampleSpec: SampleSpec = {
          type: 'index_parallel',
          spec: {
            ioConfig: {
              type: 'index_parallel',
              inputSource,
              inputFormat: {
                type: 'regex',
                pattern: '([\\s\\S]*)', // Match the entire line, every single character
                listDelimiter: BOGUS_LIST_DELIMITER,
                columns: ['raw'],
              },
            },
            dataSchema: {
              dataSource: 'sample',
              timestampSpec: PLACEHOLDER_TIMESTAMP_SPEC,
              dimensionsSpec: {},
              granularitySpec: {
                rollup: false,
              },
            },
          },
          samplerConfig: {
            numRows: ROWS_TO_SAMPLE,
            timeoutMs: 15000,
          },
        };

        const sampleResponse = await postToSampler(sampleSpec, 'input-source-step');

        sampleLines = filterMap(sampleResponse.data, l => (l.input ? l.input.raw : undefined));
      } else {
        const tableExpression = externalConfigToTableExpression({
          inputSource,
          inputFormat: {
            type: 'regex',
            pattern: '([\\s\\S]*)',
            listDelimiter: BOGUS_LIST_DELIMITER,
            columns: ['raw'],
          },
          signature: [SqlColumnDeclaration.create('raw', 'VARCHAR')],
        });

        const result = extractResult(
          await submitTaskQuery({
            query: `SELECT REPLACE(raw, U&'\\0000', '') AS "raw" FROM ${tableExpression}`, // Make sure to remove possible \u0000 chars as they are not allowed and will produce an InvalidNullByte error message
            context: {
              sqlOuterLimit: ROWS_TO_SAMPLE,
            },
            cancelToken,
          }),
        );

        if (result instanceof IntermediateQueryState) return result;
        sampleLines = result.rows.map((r: string[]) => r[0]);
      }

      if (!sampleLines.length) throw new Error('No data returned from sampler');

      const issue = issueWithSampleData(sampleLines, false);
      if (issue) {
        AppToaster.show({
          icon: IconNames.WARNING_SIGN,
          intent: Intent.WARNING,
          message: issue,
          timeout: 30000,
        });
        throw new Error(`Issue detected in sample data.`);
      }

      const guessedInputFormat = guessSimpleInputFormat(sampleLines);

      if (suggestedInputFormat?.type === guessedInputFormat.type) {
        return suggestedInputFormat;
      }

      return guessedInputFormat;
    },
    backgroundStatusCheck: async (execution, query, cancelToken) => {
      const result = await executionBackgroundResultStatusCheck(execution, query, cancelToken);
      if (result instanceof IntermediateQueryState) return result;
      return resultToInputFormat(result);
    },
  });

  useEffect(() => {
    const guessedInputFormat = guessedInputFormatState.data;
    if (!guessedInputFormat) return;
    onSet(
      exampleInput?.inputSource || (inputSource as any),
      guessedInputFormat,
      exampleInput?.partitionedByHint,
    );
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [guessedInputFormatState]);

  const effectiveType = typeof inputSource === 'string' ? 'example' : inputSource?.type;
  function renderIngestionCard(type: string): JSX.Element | undefined {
    const selected = type === effectiveType;
    return (
      <Card
        className={classNames('ingestion-card', { selected, disabled: false })}
        interactive
        elevation={1}
        onClick={() => {
          if (selected) {
            setInputSource(undefined);
          } else {
            setInputSource(type === 'example' ? '' : { type });
          }
        }}
      >
        <img
          src={UrlBaser.base(`/assets/${getIngestionImage(type as any)}.png`)}
          alt={`Ingestion tile for ${type}`}
        />
        <p>
          {getIngestionTitle(type === 'example' ? 'example' : (`index_parallel:${type}` as any))}
        </p>
      </Card>
    );
  }

  const connectResultError = guessedInputFormatState.error;
  return (
    <div className="input-source-step">
      <div className="ingestion-cards">
        {renderIngestionCard('s3')}
        {renderIngestionCard('azureStorage')}
        {renderIngestionCard('google')}
        {renderIngestionCard('hdfs')}
        {renderIngestionCard('http')}
        {renderIngestionCard('local')}
        {renderIngestionCard('inline')}
        {renderIngestionCard('example')}
      </div>
      <div className="config">
        <div className="top-controls">
          {typeof inputSource === 'string' ? (
            <>
              <FormGroup label="Select example dataset">
                <RadioGroup
                  selectedValue={inputSource}
                  onChange={e => setInputSource(e.currentTarget.value)}
                >
                  {EXAMPLE_INPUTS.map((e, i) => (
                    <Radio
                      key={i}
                      labelElement={
                        <div className="example-label">
                          <div className="name">{e.name}</div>
                          <div className="description">{e.description}</div>
                        </div>
                      }
                      value={e.name}
                    />
                  ))}
                </RadioGroup>
              </FormGroup>
            </>
          ) : inputSource ? (
            <>
              <FormGroup>
                <Callout>
                  <InputSourceInfo inputSource={inputSource} />
                </Callout>
              </FormGroup>
              <AutoForm
                fields={INPUT_SOURCE_FIELDS}
                model={inputSource}
                onChange={setInputSource}
              />
            </>
          ) : (
            <FormGroup>
              <Callout>
                <p>Please specify where your raw data is located.</p>
                <p>Your raw data can be in any of the following formats:</p>
                <ul>
                  <li>
                    <ExternalLink href="https://jsonlines.org">JSON Lines</ExternalLink>
                  </li>
                  <li>CSV</li>
                  <li>TSV</li>
                  <li>
                    <ExternalLink href="https://parquet.apache.org">Parquet</ExternalLink>
                  </li>
                  <li>
                    <ExternalLink href="https://orc.apache.org">ORC</ExternalLink>
                  </li>
                  <li>
                    <ExternalLink href="https://avro.apache.org">Avro</ExternalLink>
                  </li>
                  <li>
                    <ExternalLink href="https://protobuf.dev">Protobuf</ExternalLink>
                  </li>
                  <li>
                    Any line format that can be parsed with a custom regular expression (regex)
                  </li>
                </ul>
              </Callout>
            </FormGroup>
          )}
          {guessedInputFormatState.isLoading() && (
            <FormGroup>
              <ProgressBar intent={Intent.PRIMARY} />
            </FormGroup>
          )}
          {connectResultError && (
            <FormGroup>
              <Callout className="error-callout" intent={Intent.DANGER}>
                <p>{guessedInputFormatState.getErrorMessage()}</p>
                {(connectResultError as any).executionError && (
                  <p>
                    <a
                      onClick={() => {
                        setStackToShow(
                          ((connectResultError as any).executionError as ExecutionError)
                            .exceptionStackTrace,
                        );
                      }}
                    >
                      Stack trace
                    </a>
                  </p>
                )}
              </Callout>
            </FormGroup>
          )}
        </div>
        <div className="bottom-controls">
          {typeof inputSource === 'string' ? (
            <Button
              className="next"
              text={guessedInputFormatState.isLoading() ? 'Loading...' : 'Use example'}
              rightIcon={IconNames.ARROW_RIGHT}
              intent={Intent.PRIMARY}
              disabled={!exampleInput || guessedInputFormatState.isLoading()}
              onClick={() => {
                if (!exampleInput) return;
                connectQueryManager.runQuery({
                  inputSource: exampleInput.inputSource,
                  suggestedInputFormat: exampleInput.inputFormat,
                });
              }}
            />
          ) : inputSource ? (
            <Button
              className="next"
              text={guessedInputFormatState.isLoading() ? 'Loading...' : 'Connect data'}
              rightIcon={IconNames.ARROW_RIGHT}
              intent={Intent.PRIMARY}
              disabled={
                !AutoForm.isValidModel(inputSource, INPUT_SOURCE_FIELDS) ||
                guessedInputFormatState.isLoading()
              }
              onClick={() => {
                if (!AutoForm.isValidModel(inputSource, INPUT_SOURCE_FIELDS)) return;
                connectQueryManager.runQuery({ inputSource });
              }}
            />
          ) : undefined}
        </div>
      </div>
      {stackToShow && (
        <ShowValueDialog
          size="large"
          title="Full stack trace"
          onClose={() => setStackToShow(undefined)}
          str={stackToShow}
        />
      )}
    </div>
  );
});
