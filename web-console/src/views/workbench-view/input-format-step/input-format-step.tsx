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

import { ArrayModeSwitch, AutoForm, CenterMessage, LearnMore, Loader } from '../../../components';
import type { ArrayMode, InputFormat, InputSource } from '../../../druid-models';
import {
  BATCH_INPUT_FORMAT_FIELDS,
  chooseByBestTimestamp,
  DETECTION_TIMESTAMP_SPEC,
  getPossibleSystemFieldsForInputSource,
  guessColumnTypeFromSampleResponse,
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

export interface InputSourceFormatAndMore {
  inputSource: InputSource;
  inputFormat: InputFormat;
  signature: SqlColumnDeclaration[];
  timeExpression: SqlExpression | undefined;
  arrayMode: ArrayMode;
}

interface InputSourceAndFormat {
  inputSource: InputSource;
  inputFormat: Partial<InputFormat>;
}

interface PossibleTimeExpression {
  column: string;
  format: string;
  timeExpression: SqlExpression;
}

export interface InputFormatStepProps {
  initInputSource: InputSource;
  initInputFormat: Partial<InputFormat>;
  doneButton: boolean;
  onSet(inputSourceFormatAndMore: InputSourceFormatAndMore): void;
  onBack(): void;
  onAltSet?(inputSourceFormatAndMore: InputSourceFormatAndMore): void;
  altText?: string;
}

function isValidInputFormat(inputFormat: Partial<InputFormat>): inputFormat is InputFormat {
  return AutoForm.isValidModel(inputFormat, BATCH_INPUT_FORMAT_FIELDS);
}

export const InputFormatStep = React.memo(function InputFormatStep(props: InputFormatStepProps) {
  const { initInputSource, initInputFormat, doneButton, onSet, onBack, onAltSet, altText } = props;

  const [inputSourceAndFormat, setInputSourceAndFormat] = useState<InputSourceAndFormat>({
    inputSource: initInputSource,
    inputFormat: initInputFormat,
  });
  const [inputSourceAndFormatToSample, setInputSourceAndFormatToSample] = useState<
    InputSourceAndFormat | undefined
  >(isValidInputFormat(initInputFormat) ? inputSourceAndFormat : undefined);
  const [selectTimestamp, setSelectTimestamp] = useState(true);
  const [arrayMode, setArrayMode] = useState<ArrayMode>('multi-values');

  const [previewState] = useQueryManager<InputSourceAndFormat, SampleResponse>({
    query: inputSourceAndFormatToSample,
    processQuery: async ({ inputSource, inputFormat }) => {
      if (!isValidInputFormat(inputFormat)) throw new Error('invalid input format');

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
              dimensions: inputSource.systemFields,
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

  const currentInputFormat = inputSourceAndFormat.inputFormat;
  const inputSourceFormatAndMore: InputSourceFormatAndMore | undefined =
    previewSampleResponse && headerNames && isValidInputFormat(currentInputFormat)
      ? {
          inputSource: inputSourceAndFormat.inputSource,
          inputFormat: currentInputFormat,
          signature: headerNames.map(name =>
            SqlColumnDeclaration.create(
              name,
              SqlType.fromNativeType(
                guessColumnTypeFromSampleResponse(
                  previewSampleResponse,
                  name,
                  inputFormatOutputsNumericStrings(currentInputFormat),
                ),
              ),
            ),
          ),
          timeExpression: selectTimestamp ? possibleTimeExpression?.timeExpression : undefined,
          arrayMode,
        }
      : undefined;

  const hasArrays = inputSourceFormatAndMore?.signature.some(d => d.columnType.isArray());
  const possibleSystemFields = getPossibleSystemFieldsForInputSource(
    inputSourceAndFormat.inputSource,
  );

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
            model={inputSourceAndFormat.inputFormat}
            onChange={inputFormat =>
              setInputSourceAndFormat({ ...inputSourceAndFormat, inputFormat })
            }
          />
          {possibleSystemFields.length > 0 && (
            <AutoForm
              fields={[
                {
                  name: 'inputSource.systemFields',
                  label: 'System fields',
                  type: 'string-array',
                  suggestions: possibleSystemFields,
                  info: 'JSON array of system fields to return as part of input rows.',
                },
              ]}
              model={inputSourceAndFormat}
              onChange={setInputSourceAndFormat as any}
            />
          )}
          {inputSourceAndFormatToSample !== inputSourceAndFormat && (
            <FormGroup className="control-buttons">
              <Button
                text="Preview changes"
                intent={Intent.PRIMARY}
                disabled={!isValidInputFormat(inputSourceAndFormat.inputFormat)}
                onClick={() => {
                  if (!isValidInputFormat(inputSourceAndFormat.inputFormat)) return;
                  setInputSourceAndFormatToSample(inputSourceAndFormat);
                }}
              />
            </FormGroup>
          )}
        </div>
        <div className="bottom-controls">
          {hasArrays && <ArrayModeSwitch arrayMode={arrayMode} changeArrayMode={setArrayMode} />}
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
                  disabled={!inputSourceFormatAndMore}
                  onClick={() => {
                    if (!inputSourceFormatAndMore) return;
                    onAltSet(inputSourceFormatAndMore);
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
              disabled={!inputSourceFormatAndMore}
              onClick={() => {
                if (!inputSourceFormatAndMore) return;
                onSet(inputSourceFormatAndMore);
              }}
            />
          </div>
        </div>
      </div>
    </div>
  );
});
