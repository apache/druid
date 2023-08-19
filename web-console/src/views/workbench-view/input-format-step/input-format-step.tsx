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

import { Button, Callout, FormGroup, Icon, Intent, Tag } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import type { SqlExpression } from '@druid-toolkit/query';
import { C, SqlColumnDeclaration, SqlType } from '@druid-toolkit/query';
import React, { useState } from 'react';

import { AutoForm, CenterMessage, LearnMore, Loader } from '../../../components';
import type { InputFormat, InputSource } from '../../../druid-models';
import {
  BATCH_INPUT_FORMAT_FIELDS,
  chooseByBestTimestamp,
  DETECTION_TIMESTAMP_SPEC,
  guessColumnTypeFromSampleResponse,
  guessIsArrayFromSampleResponse,
  inputFormatOutputsNumericStrings,
  possibleDruidFormatForValues,
  TIME_COLUMN,
} from '../../../druid-models';
import { useQueryManager } from '../../../hooks';
import { getLink } from '../../../links';
import {
  checkedCircleIcon,
  deepSet,
  EMPTY_ARRAY,
  filterMap,
  timeFormatToSql,
} from '../../../utils';
import type { SampleResponse, SampleSpec } from '../../../utils/sampler';
import { getHeaderNamesFromSampleResponse, postToSampler } from '../../../utils/sampler';
import { ParseDataTable } from '../../load-data-view/parse-data-table/parse-data-table';

import './input-format-step.scss';

export interface InputFormatAndMore {
  inputFormat: InputFormat;
  signature: SqlColumnDeclaration[];
  isArrays: boolean[];
  timeExpression: SqlExpression | undefined;
}

interface PossibleTimeExpression {
  column: string;
  format: string;
  timeExpression: SqlExpression;
}

export interface InputFormatStepProps {
  inputSource: InputSource;
  initInputFormat: Partial<InputFormat>;
  doneButton: boolean;
  onSet(inputFormatAndMore: InputFormatAndMore): void;
  onBack(): void;
  onAltSet?(inputFormatAndMore: InputFormatAndMore): void;
  altText?: string;
}

export const InputFormatStep = React.memo(function InputFormatStep(props: InputFormatStepProps) {
  const { inputSource, initInputFormat, doneButton, onSet, onBack, onAltSet, altText } = props;

  const [inputFormat, setInputFormat] = useState<Partial<InputFormat>>(initInputFormat);
  const [inputFormatToSample, setInputFormatToSample] = useState<InputFormat | undefined>(
    AutoForm.isValidModel(initInputFormat, BATCH_INPUT_FORMAT_FIELDS) ? initInputFormat : undefined,
  );
  const [selectTimestamp, setSelectTimestamp] = useState(true);

  const [previewState] = useQueryManager<InputFormat, SampleResponse>({
    query: inputFormatToSample,
    processQuery: async (inputFormat: InputFormat) => {
      const sampleSpec: SampleSpec = {
        type: 'index_parallel',
        spec: {
          ioConfig: {
            type: 'index_parallel',
            inputSource,
            inputFormat: deepSet(inputFormat, 'keepNullColumns', true),
          },
          dataSchema: {
            dataSource: 'sample',
            timestampSpec: DETECTION_TIMESTAMP_SPEC,
            dimensionsSpec: {
              useSchemaDiscovery: true,
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
      };

      return await postToSampler(sampleSpec, 'input-format-step');
    },
  });

  const previewSampleResponse = previewState.data;

  let possibleTimeExpression: PossibleTimeExpression | undefined;
  if (previewSampleResponse) {
    possibleTimeExpression = chooseByBestTimestamp(
      filterMap(getHeaderNamesFromSampleResponse(previewSampleResponse), column => {
        const values = filterMap(previewSampleResponse.data, d => d.input?.[column]);
        const possibleDruidFormat = possibleDruidFormatForValues(values);
        if (!possibleDruidFormat) return;

        // The __time column is special because it already is a TIMESTAMP so there is no need parse it in any way
        if (column === TIME_COLUMN) {
          return {
            column,
            format: '',
            timeExpression: C(column),
          };
        }

        const formatSql = timeFormatToSql(possibleDruidFormat);
        if (!formatSql) return;

        return {
          column,
          format: possibleDruidFormat,
          timeExpression: formatSql.fillPlaceholders([C(column)]),
        };
      }),
    );
  }

  const headerNames = previewSampleResponse
    ? getHeaderNamesFromSampleResponse(previewSampleResponse, 'ignoreIfZero')
    : undefined;

  const inputFormatAndMore =
    previewSampleResponse &&
    headerNames &&
    AutoForm.isValidModel(inputFormat, BATCH_INPUT_FORMAT_FIELDS)
      ? {
          inputFormat,
          signature: headerNames.map(name =>
            SqlColumnDeclaration.create(
              name,
              SqlType.fromNativeType(
                guessColumnTypeFromSampleResponse(
                  previewSampleResponse,
                  name,
                  inputFormatOutputsNumericStrings(inputFormat),
                ),
              ),
            ),
          ),
          isArrays: headerNames.map(name =>
            guessIsArrayFromSampleResponse(previewSampleResponse, name),
          ),
          timeExpression: selectTimestamp ? possibleTimeExpression?.timeExpression : undefined,
        }
      : undefined;

  return (
    <div className="input-format-step">
      <div className="preview">
        {previewState.isInit() && (
          <CenterMessage>
            Please fill out the fields on the right sidebar to get started{' '}
            <Icon icon={IconNames.ARROW_RIGHT} />
          </CenterMessage>
        )}
        {previewState.isLoading() && <Loader />}
        {previewState.error && (
          <CenterMessage>{`Error: ${previewState.getErrorMessage()}`}</CenterMessage>
        )}
        {previewSampleResponse && (
          <ParseDataTable
            sampleResponse={previewSampleResponse}
            columnFilter=""
            canFlatten={false}
            flattenedColumnsOnly={false}
            flattenFields={EMPTY_ARRAY}
          />
        )}
      </div>
      <div className="config">
        <div className="top-controls">
          <FormGroup>
            <Callout>
              <p>Ensure that your data appears correctly in a row/column orientation.</p>
              <LearnMore href={`${getLink('DOCS')}/ingestion/data-formats.html`} />
            </Callout>
          </FormGroup>
          <AutoForm
            fields={BATCH_INPUT_FORMAT_FIELDS}
            model={inputFormat}
            onChange={setInputFormat}
          />
          {inputFormatToSample !== inputFormat && (
            <FormGroup className="control-buttons">
              <Button
                text="Preview changes"
                intent={Intent.PRIMARY}
                disabled={!AutoForm.isValidModel(inputFormat, BATCH_INPUT_FORMAT_FIELDS)}
                onClick={() => {
                  if (!AutoForm.isValidModel(inputFormat, BATCH_INPUT_FORMAT_FIELDS)) return;
                  setInputFormatToSample(inputFormat);
                }}
              />
            </FormGroup>
          )}
        </div>
        <div className="bottom-controls">
          {possibleTimeExpression && (
            <FormGroup>
              <Callout>
                <Button
                  rightIcon={checkedCircleIcon(selectTimestamp)}
                  onClick={() => setSelectTimestamp(!selectTimestamp)}
                  minimal
                >
                  Select <Tag minimal>{possibleTimeExpression.column}</Tag> as the primary time
                  column
                </Button>
              </Callout>
            </FormGroup>
          )}
          {altText && onAltSet && (
            <FormGroup>
              <Callout>
                <Button
                  text={altText}
                  rightIcon={IconNames.ARROW_TOP_RIGHT}
                  minimal
                  disabled={!inputFormatAndMore}
                  onClick={() => {
                    if (!inputFormatAndMore) return;
                    onAltSet(inputFormatAndMore);
                  }}
                />
              </Callout>
            </FormGroup>
          )}
          <div className="prev-next-bar">
            <Button className="back" icon={IconNames.ARROW_LEFT} text="Back" onClick={onBack} />
            <Button
              className="next"
              text={doneButton ? 'Done' : 'Next'}
              rightIcon={doneButton ? IconNames.TICK : IconNames.ARROW_RIGHT}
              intent={Intent.PRIMARY}
              disabled={!inputFormatAndMore}
              onClick={() => {
                if (!inputFormatAndMore) return;
                onSet(inputFormatAndMore);
              }}
            />
          </div>
        </div>
      </div>
    </div>
  );
});
