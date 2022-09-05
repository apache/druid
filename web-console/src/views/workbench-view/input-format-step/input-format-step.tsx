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
import { SqlExpression, SqlRef } from 'druid-query-toolkit';
import React, { useState } from 'react';

import { AutoForm, CenterMessage, LearnMore, Loader } from '../../../components';
import {
  guessColumnTypeFromHeaderAndRows,
  guessIsArrayFromHeaderAndRows,
  INPUT_FORMAT_FIELDS,
  InputFormat,
  inputFormatOutputsNumericStrings,
  InputSource,
  PLACEHOLDER_TIMESTAMP_SPEC,
  possibleDruidFormatForValues,
  SignatureColumn,
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
import {
  headerAndRowsFromSampleResponse,
  postToSampler,
  SampleHeaderAndRows,
  SampleSpec,
} from '../../../utils/sampler';
import { ParseDataTable } from '../../load-data-view/parse-data-table/parse-data-table';

import './input-format-step.scss';

const noop = () => {};

export interface InputFormatAndMore {
  inputFormat: InputFormat;
  signature: SignatureColumn[];
  isArrays: boolean[];
  timeExpression: SqlExpression | undefined;
}

interface PossibleTimeExpression {
  column: string;
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
    AutoForm.isValidModel(initInputFormat, INPUT_FORMAT_FIELDS) ? initInputFormat : undefined,
  );
  const [selectTimestamp, setSelectTimestamp] = useState(true);

  const [previewState] = useQueryManager<InputFormat, SampleHeaderAndRows>({
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
            timestampSpec: PLACEHOLDER_TIMESTAMP_SPEC,
            dimensionsSpec: {},
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

      const sampleResponse = await postToSampler(sampleSpec, 'input-format-step');

      return headerAndRowsFromSampleResponse({
        sampleResponse,
        ignoreTimeColumn: true,
        useInput: true,
      });
    },
  });

  const previewData = previewState.data;

  let possibleTimeExpression: PossibleTimeExpression | undefined;
  if (previewData) {
    possibleTimeExpression = filterMap(previewData.header, column => {
      const values = previewData.rows.map(row => row.input[column]);
      const possibleDruidFormat = possibleDruidFormatForValues(values);
      if (!possibleDruidFormat) return;

      const formatSql = timeFormatToSql(possibleDruidFormat);
      if (!formatSql) return;

      return {
        column,
        timeExpression: formatSql.fillPlaceholders([SqlRef.column(column)]),
      };
    })[0];
  }

  const inputFormatAndMore =
    previewData && AutoForm.isValidModel(inputFormat, INPUT_FORMAT_FIELDS)
      ? {
          inputFormat,
          signature: previewData.header.map(name => ({
            name,
            type: guessColumnTypeFromHeaderAndRows(
              previewData,
              name,
              inputFormatOutputsNumericStrings(inputFormat),
            ),
          })),
          isArrays: previewData.header.map(name =>
            guessIsArrayFromHeaderAndRows(previewData, name),
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
        {previewData && (
          <ParseDataTable
            sampleData={previewData}
            columnFilter=""
            canFlatten={false}
            flattenedColumnsOnly={false}
            flattenFields={EMPTY_ARRAY}
            onFlattenFieldSelect={noop}
            useInput
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
          <AutoForm fields={INPUT_FORMAT_FIELDS} model={inputFormat} onChange={setInputFormat} />
          {inputFormatToSample !== inputFormat && (
            <FormGroup className="control-buttons">
              <Button
                text="Preview changes"
                intent={Intent.PRIMARY}
                disabled={!AutoForm.isValidModel(inputFormat, INPUT_FORMAT_FIELDS)}
                onClick={() => {
                  if (!AutoForm.isValidModel(inputFormat, INPUT_FORMAT_FIELDS)) return;
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
