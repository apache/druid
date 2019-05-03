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
  Alert,
  AnchorButton,
  Button,
  ButtonGroup, Callout, Card,
  Classes, Code,
  FormGroup, H5,
  Icon, Intent, Popover, Switch, TextArea
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import axios from 'axios';
import 'brace/mode/json';
import 'brace/theme/solarized_dark';
import * as classNames from 'classnames';
import * as React from 'react';
import ReactTable from 'react-table';

import { AutoForm } from '../components/auto-form';
import { CenterMessage } from '../components/center-message';
import { ClearableInput } from '../components/clearable-input';
import { ExternalLink } from '../components/external-link';
import { JSONInput } from '../components/json-input';
import { Loader } from '../components/loader';
import { NullTableCell } from '../components/null-table-cell';
import { AsyncActionDialog } from '../dialogs/async-action-dialog';
import { AppToaster } from '../singletons/toaster';
import {
  filterMap,
  getDruidErrorMessage,
  localStorageGet,
  LocalStorageKeys,
  localStorageSet, parseJson,
  QueryState, sortWithPrefixSuffix
} from '../utils';
import { escapeColumnName } from '../utils/druid-expression';
import { possibleDruidFormatForValues } from '../utils/druid-time';
import { updateSchemaWithSample } from '../utils/druid-type';
import {
  changeParallel, DimensionMode,
  DimensionSpec, DimensionsSpec, DruidFilter,
  fillDataSourceName,
  fillParser,
  FlattenField, getBlankSpec, getDimensionMode,
  getDimensionSpecFormFields,
  getDimensionSpecName, getDimensionSpecType, getEmptyTimestampSpec, getFilterFormFields, getFlattenFieldFormFields,
  getIngestionComboType, getIoConfigFormFields, getIoConfigTuningFormFields, getMetricSpecFormFields,
  getMetricSpecName, getParseSpecFormFields, getRollup, getTimestampSpecColumn, getTimestampSpecFormFields,
  getTransformFormFields,
  getTuningSpecFormFields, GranularitySpec, hasParallelAbility, inflateDimensionSpec, IngestionSpec,
  IngestionType, IoConfig,
  isColumnTimestampSpec, isParallel, issueWithIoConfig, issueWithParser, joinFilter,
  MetricSpec, Parser, ParseSpec,
  parseSpecHasFlatten, splitFilter, TimestampSpec, Transform, TuningConfig
} from '../utils/ingestion-spec';
import { deepDelete, deepGet, deepSet } from '../utils/object-change';
import {
  HeaderAndRows,
  headerAndRowsFromSampleResponse,
  SampleEntry,
  sampleForConnect,
  sampleForFilter,
  sampleForParser, sampleForSchema,
  sampleForTimestamp,
  sampleForTransform,
  SampleResponse
} from '../utils/sampler';
import { computeFlattenPathsForData } from '../utils/spec-utils';

import './load-data-view.scss';

export interface LoadDataViewSeed {
  type?: IngestionType;
  firehoseType?: string;
  initSpec?: IngestionSpec;
}

function filterMatch(testString: string, searchString: string): boolean {
  if (!searchString) return true;
  return testString.toLowerCase().includes(searchString.toLowerCase());
}

function getTimestampSpec(headerAndRows: HeaderAndRows | null): TimestampSpec {
  if (!headerAndRows) return getEmptyTimestampSpec();

  const timestampSpecs = headerAndRows.header.map(sampleHeader => {
    const possibleFormat = possibleDruidFormatForValues(filterMap(headerAndRows.rows, d => d.parsed ? d.parsed[sampleHeader] : null));
    if (!possibleFormat) return null;
    return {
      column: sampleHeader,
      format: possibleFormat
    };
  }).filter(Boolean);

  return timestampSpecs[0] || getEmptyTimestampSpec();
}

type Stage = 'connect' | 'parser' | 'timestamp' | 'transform' | 'filter' | 'schema' | 'partition' | 'tuning' | 'publish' | 'json-spec';
const STAGES: Stage[] = ['connect', 'parser', 'timestamp', 'transform', 'filter', 'schema', 'partition', 'tuning', 'publish', 'json-spec'];

const SECTIONS: { name: string, stages: Stage[] }[] = [
  { name: 'Connect and parse raw data', stages: ['connect', 'parser', 'timestamp'] },
  { name: 'Transform and configure schema', stages: ['transform', 'filter', 'schema'] },
  { name: 'Tune parameters', stages: ['partition', 'tuning', 'publish'] },
  { name: 'Verify and submit', stages: ['json-spec'] }
];

const VIEW_TITLE: Record<Stage, string> = {
  'connect': 'Connect',
  'parser': 'Parse data',
  'timestamp': 'Parse time',
  'transform': 'Transform',
  'filter': 'Filter',
  'schema': 'Configure schema',
  'partition': 'Partition',
  'tuning': 'Tune',
  'publish': 'Publish',
  'json-spec': 'Edit JSON spec'
};

export interface LoadDataViewProps extends React.Props<any> {
  seed: LoadDataViewSeed | null;
  goToTask: (taskId: string | null) => void;
}

export interface LoadDataViewState {
  stage: Stage;
  spec: IngestionSpec;
  cacheKey: string | undefined;

  // dialogs / modals
  showResetConfirm: boolean;
  newRollup: boolean | null;
  newDimensionMode: DimensionMode | null;

  // general
  columnFilter: string;
  specialColumnsOnly: boolean;

  // for ioConfig
  inputQueryState: QueryState<string[]>;

  // for parser
  parserQueryState: QueryState<HeaderAndRows>;

  // for flatten
  flattenQueryState: QueryState<HeaderAndRows>;
  selectedFlattenFieldIndex: number;
  selectedFlattenField: FlattenField | null;

  // for timestamp
  timestampQueryState: QueryState<HeaderAndRows>;

  // for transform
  transformQueryState: QueryState<HeaderAndRows>;
  selectedTransformIndex: number;
  selectedTransform: Transform | null;

  // for filter
  filterQueryState: QueryState<HeaderAndRows>;
  selectedFilterIndex: number;
  selectedFilter: DruidFilter | null;
  showGlobalFilter: boolean;

  // for schema
  schemaQueryState: QueryState<HeaderAndRows>;
  selectedDimensionSpecIndex: number;
  selectedDimensionSpec: DimensionSpec | null;
  selectedMetricSpecIndex: number;
  selectedMetricSpec: MetricSpec | null;
}

export class LoadDataView extends React.Component<LoadDataViewProps, LoadDataViewState> {
  constructor(props: LoadDataViewProps) {
    super(props);

    let spec = parseJson(String(localStorageGet(LocalStorageKeys.INGESTION_SPEC)));
    if (!spec || typeof spec !== 'object') spec = {};

    this.state = {
      stage: 'connect',
      spec,
      cacheKey: undefined,

      // dialogs / modals
      showResetConfirm: false,
      newRollup: null,
      newDimensionMode: null,

      // general
      columnFilter: '',
      specialColumnsOnly: false,

      // for firehose
      inputQueryState: QueryState.INIT,

      // for parser
      parserQueryState: QueryState.INIT,

      // for flatten
      flattenQueryState: QueryState.INIT,
      selectedFlattenFieldIndex: -1,
      selectedFlattenField: null,

      // for timestamp
      timestampQueryState: QueryState.INIT,

      // for transform
      transformQueryState: QueryState.INIT,
      selectedTransformIndex: -1,
      selectedTransform: null,

      // for filter
      filterQueryState: QueryState.INIT,
      selectedFilterIndex: -1,
      selectedFilter: null,
      showGlobalFilter: false,

      // for dimensions
      schemaQueryState: QueryState.INIT,
      selectedDimensionSpecIndex: -1,
      selectedDimensionSpec: null,
      selectedMetricSpecIndex: -1,
      selectedMetricSpec: null
    };
  }

  componentDidMount(): void {
    this.updateStage('connect');
  }

  private updateStage = (newStage: Stage) => {
    this.doQueryForStage(newStage);
    this.setState({ stage: newStage });
  }

  doQueryForStage(stage: Stage): any {
    switch (stage) {
      case 'connect': return this.queryForConnect(true);
      case 'parser': return this.queryForParser(true);
      case 'timestamp': return this.queryForTimestamp(true);
      case 'transform': return this.queryForTransform(true);
      case 'filter': return this.queryForFilter(true);
      case 'schema': return this.queryForSchema(true);
    }
  }

  private updateSpec = (newSpec: IngestionSpec) => {
    if (!newSpec || typeof newSpec !== 'object') {
      // This does not match the type of IngestionSpec but this dialog is robust enough to deal with anything but spec must be an object
      newSpec = {} as any;
    }
    this.setState({ spec: newSpec });
    localStorageSet(LocalStorageKeys.INGESTION_SPEC, JSON.stringify(newSpec));
  }

  render() {
    const { stage, spec } = this.state;

    if (!Object.keys(spec).length) {
      return <div className={classNames('load-data-view', 'app-view', 'init')}>
        {this.renderInitStage()}
      </div>;
    }

    return <div className={classNames('load-data-view', 'app-view', stage)}>
      {this.renderStepNav()}

      {stage === 'connect' && this.renderConnectStage()}
      {stage === 'parser' && this.renderParserStage()}
      {stage === 'timestamp' && this.renderTimestampStage()}

      {stage === 'transform' && this.renderTransformStage()}
      {stage === 'filter' && this.renderFilterStage()}
      {stage === 'schema' && this.renderSchemaStage()}

      {stage === 'partition' && this.renderPartitionStage()}
      {stage === 'tuning' && this.renderTuningStage()}
      {stage === 'publish' && this.renderPublishStage()}

      {stage === 'json-spec' && this.renderJsonSpecStage()}

      {this.renderResetConfirm()}
    </div>;
  }

  renderStepNav() {
    const { stage } = this.state;

    return <div className={classNames(Classes.TABS, 'stage-nav')}>
      {SECTIONS.map(section => (
        <div className="stage-section" key={section.name}>
          <div className="stage-nav-l1">
            {section.name}
          </div>
          <ButtonGroup className="stage-nav-l2">
            {section.stages.map((s) => (
              <Button
                className={s}
                key={s}
                active={s === stage}
                onClick={() => this.updateStage(s)}
                icon={s === 'json-spec' && IconNames.EYE_OPEN}
                text={VIEW_TITLE[s]}
              />
            ))}
          </ButtonGroup>
        </div>
      ))}
    </div>;
  }

  renderNextBar(options: { nextStage?: Stage, disabled?: boolean; onNextStage?: () => void, onPrevStage?: () => void, prevLabel?: string }) {
    const { disabled, onNextStage, onPrevStage, prevLabel } = options;
    const { stage } = this.state;
    const nextStage = options.nextStage || STAGES[STAGES.indexOf(stage) + 1] || STAGES[0];

    return <div className="next-bar">
      {
        onPrevStage &&
        <Button
          className="prev"
          icon={IconNames.ARROW_LEFT}
          text={prevLabel}
          onClick={onPrevStage}
        />
      }
      <Button
        text={`Next: ${VIEW_TITLE[nextStage]}`}
        intent={Intent.PRIMARY}
        disabled={disabled}
        onClick={() => {
          if (disabled) return;
          if (onNextStage) onNextStage();

          setTimeout(() => {
            this.updateStage(nextStage);
          }, 10);
        }}
      />
    </div>;
  }

  // ==================================================================

  initWith(seed: LoadDataViewSeed) {
    this.setState({
      spec: getBlankSpec(seed.type, seed.firehoseType)
    });
    setTimeout(() => {
      this.updateStage('connect');
    }, 10);
  }

  renderInitStage() {
    const showStreaming = false;

    return <>
      <div className="intro">
        Please specify where your raw data is located
      </div>

      <Callout intent={Intent.SUCCESS} icon={IconNames.INFO_SIGN}>
        Welcome to the Druid data loader.
        This project is under active development and we plan to support many other sources of raw data, including stream hubs such as Apache Kafka and AWS Kinesis, in the next few releases.
      </Callout>

      {
        showStreaming &&
        <div className="section">
          <div className="section-title">Stream hub</div>
          <div className="cards">
            <Card interactive onClick={() => this.initWith({ type: 'kafka' })}>Apache Kafka</Card>
            <Card interactive onClick={() => this.initWith({ type: 'kinesis' })}>AWS Kinesis</Card>
          </div>
        </div>
      }

      <div className="section">
        <div className="section-title">Batch load</div>
        <div className="cards">
          <Card interactive onClick={() => this.initWith({ type: 'index_parallel', firehoseType: 'http' })}>HTTP(s)</Card>
          <Card interactive onClick={() => this.initWith({ type: 'index_parallel', firehoseType: 'static-s3' })}>AWS S3</Card>
          <Card interactive onClick={() => this.initWith({ type: 'index_parallel', firehoseType: 'static-google-blobstore' })}>Google Blobstore</Card>
          <Card interactive onClick={() => this.initWith({ type: 'index_parallel', firehoseType: 'local' })}>Local disk</Card>
        </div>
      </div>
    </>;
  }

  renderResetConfirm() {
    const { showResetConfirm } = this.state;
    if (!showResetConfirm) return null;

    return <Alert
      cancelButtonText="Cancel"
      confirmButtonText="Reset spec"
      icon="trash"
      intent={Intent.DANGER}
      isOpen
      onCancel={() => this.setState({ showResetConfirm: false })}
      onConfirm={() => {
        this.setState({ showResetConfirm: false });
        this.updateSpec({} as any);
      }}
    >
      <p>
        This will discard the current progress in the spec.
      </p>
    </Alert>;
  }

  // ==================================================================

  async queryForConnect(initRun = false) {
    const { spec } = this.state;
    const ioConfig: IoConfig = deepGet(spec, 'ioConfig') || {};

    let issue: string | undefined;
    if (issueWithIoConfig(ioConfig)) {
      issue = `IoConfig not ready, ${issueWithIoConfig(ioConfig)}`;
    }

    if (issue) {
      this.setState({
        inputQueryState: initRun ? QueryState.INIT : new QueryState({ error: issue })
      });
      return;
    }

    this.setState({
      inputQueryState: new QueryState({ loading: true })
    });

    let sampleResponse: SampleResponse;
    try {
      sampleResponse = await sampleForConnect(spec);
    } catch (e) {
      this.setState({
        inputQueryState: new QueryState({ error: e.message })
      });
      return;
    }

    this.setState({
      cacheKey: sampleResponse.cacheKey,
      inputQueryState: new QueryState({ data: sampleResponse.data.map((d: any) => d.raw) })
    });
  }

  renderConnectStage() {
    const { spec, inputQueryState } = this.state;
    const ioConfig: IoConfig = deepGet(spec, 'ioConfig') || {};
    const isBlank = !ioConfig.type;

    let mainFill: JSX.Element | string = '';
    if (inputQueryState.isInit()) {
      mainFill = <CenterMessage>
        Please fill out the fields on the right sidebar to get started.
      </CenterMessage>;

    } else if (inputQueryState.isLoading()) {
      mainFill = <Loader loading/>;

    } else if (inputQueryState.error) {
      mainFill = <CenterMessage>
        {`Error: ${inputQueryState.error}`}
      </CenterMessage>;

    } else if (inputQueryState.data) {
      const inputData = inputQueryState.data;
      mainFill = <TextArea
        className="raw-lines"
        value={(inputData.every(l => !l) ? inputData.map(_ => '[Binary data]') : inputData).join('\n')}
        readOnly
      />;
    }

    const ingestionComboType = getIngestionComboType(spec);
    return <>
      <div className="main">{mainFill}</div>
      <div className="control">
        <Callout className="intro">
          <p>
            Druid ingests raw data and converts it into a custom, <ExternalLink href="http://druid.io/docs/latest/design/segments.html">indexed</ExternalLink> format that is optimized for analytic queries.
          </p>
          <p>
            To get started, please specify where your raw data is stored and what data you want to ingest.
          </p>
          <p>
            Click "Preview" to look at the sampled raw data.
          </p>
        </Callout>
        {
          ingestionComboType ?
          <AutoForm
            fields={getIoConfigFormFields(ingestionComboType)}
            model={ioConfig}
            onChange={c => this.updateSpec(deepSet(spec, 'ioConfig', c))}
          /> :
          <FormGroup label="IO Config">
            <JSONInput
              value={ioConfig}
              onChange={c => this.updateSpec(deepSet(spec, 'ioConfig', c))}
              height="300px"
            />
          </FormGroup>
        }
        {
          deepGet(spec, 'ioConfig.firehose.type') === 'local' &&
          <FormGroup>
            <Callout intent={Intent.WARNING}>
              This path must be available on the local filesystem of all Druid servers.
            </Callout>
          </FormGroup>
        }
        <Button
          text="Preview"
          disabled={isBlank}
          onClick={() => this.queryForConnect()}
        />
      </div>
      {this.renderNextBar({
        disabled: !inputQueryState.data,
        onNextStage: () => {
          if (!inputQueryState.data) return;
          this.updateSpec(fillDataSourceName(fillParser(spec, inputQueryState.data)));
        },
        prevLabel: 'Restart',
        onPrevStage: () => this.setState({ showResetConfirm: true })
      })}
    </>;
  }

  // ==================================================================

  async queryForParser(initRun = false) {
    const { spec, cacheKey } = this.state;
    const ioConfig: IoConfig = deepGet(spec, 'ioConfig') || {};
    const parser: Parser = deepGet(spec, 'dataSchema.parser') || {};

    let issue: string | null = null;
    if (issueWithIoConfig(ioConfig)) {
      issue = `IoConfig not ready, ${issueWithIoConfig(ioConfig)}`;
    } else if (issueWithParser(parser)) {
      issue = `Parser not ready, ${issueWithParser(parser)}`;
    }

    if (issue) {
      this.setState({
        parserQueryState: initRun ? QueryState.INIT : new QueryState({ error: issue })
      });
      return;
    }

    this.setState({
      parserQueryState: new QueryState({ loading: true })
    });

    let sampleResponse: SampleResponse;
    try {
      sampleResponse = await sampleForParser(spec, cacheKey);
    } catch (e) {
      this.setState({
        parserQueryState: new QueryState({ error: e.message })
      });
      return;
    }

    this.setState({
      cacheKey: sampleResponse.cacheKey,
      parserQueryState: new QueryState({
        data: headerAndRowsFromSampleResponse(sampleResponse, '__time')
      })
    });
  }

  renderParserStage() {
    const { spec, columnFilter, specialColumnsOnly, parserQueryState, selectedFlattenField } = this.state;
    const parseSpec: ParseSpec = deepGet(spec, 'dataSchema.parser.parseSpec') || {};
    const flattenFields: FlattenField[] = deepGet(spec, 'dataSchema.parser.parseSpec.flattenSpec.fields') || [];

    const isBlank = !parseSpec.format;
    const canFlatten = parseSpec.format === 'json';

    let mainFill: JSX.Element | string = '';
    if (parserQueryState.isInit()) {
      mainFill = <CenterMessage>
        Please enter the parser details on the right
      </CenterMessage>;

    } else if (parserQueryState.isLoading()) {
      mainFill = <Loader loading/>;

    } else if (parserQueryState.error) {
      mainFill = <CenterMessage>
        {`Error: ${parserQueryState.error}`}
      </CenterMessage>;

    } else if (parserQueryState.data) {
      mainFill = <div className="table-with-control">
        <div className="table-control">
          <ClearableInput
            value={columnFilter}
            onChange={(columnFilter) => this.setState({ columnFilter })}
            placeholder="Search columns"
          />
          {
            canFlatten &&
            <Switch
              checked={specialColumnsOnly}
              label="Flattened columns only"
              onChange={() => this.setState({ specialColumnsOnly: !specialColumnsOnly })}
              disabled={!flattenFields.length}
            />
          }
        </div>
        <ReactTable
          data={parserQueryState.data.rows}
          columns={filterMap(parserQueryState.data.header, (columnName, i) => {
            if (!filterMatch(columnName, columnFilter)) return null;
            const flattenFieldIndex = flattenFields.findIndex(f => f.name === columnName);
            if (flattenFieldIndex === -1 && specialColumnsOnly) return null;
            const flattenField = flattenFields[flattenFieldIndex];
            return {
              Header: (
                <div
                  className={classNames({ clickable: flattenField })}
                  onClick={() => {
                    this.setState({
                      selectedFlattenFieldIndex: flattenFieldIndex,
                      selectedFlattenField: flattenField
                    });
                  }}
                >
                  <div className="column-name">{columnName}</div>
                  <div className="column-detail">
                    {flattenField ? `${flattenField.type}: ${flattenField.expr}` : ''}&nbsp;
                  </div>
                </div>
              ),
              id: String(i),
              accessor: (row: SampleEntry) => row.parsed ? row.parsed[columnName] : null,
              Cell: row => {
                if (row.original.unparseable) {
                  return <NullTableCell unparseable/>;
                }
                return <NullTableCell value={row.value}/>;
              },
              headerClassName: classNames({
                flattened: flattenField
              })
            };
          })}
          SubComponent={rowInfo => {
            const { raw, error } = rowInfo.original;
            const parsedJson: any = parseJson(raw);

            if (!error && parsedJson && canFlatten) {
              return <pre className="parse-detail">
                {'Original row: ' + JSON.stringify(parsedJson, null, 2)}
              </pre>;
            } else {
              return <div className="parse-detail">
                {error && <div className="parse-error">{error}</div>}
                <div>{'Original row: ' + rowInfo.original.raw}</div>
              </div>;
            }
          }}
          defaultPageSize={50}
          showPagination={false}
          sortable={false}
          className="-striped -highlight"
        />
      </div>;
    }

    let sugestedFlattenFields: FlattenField[] | null = null;
    if (canFlatten && !flattenFields.length && parserQueryState.data) {
      sugestedFlattenFields = computeFlattenPathsForData(filterMap(parserQueryState.data.rows, r => parseJson(r.raw)), 'path', 'ignore-arrays');
    }

    return <>
      <div className="main">{mainFill}</div>
      <div className="control">
        <Callout className="intro">
          <p>
            Druid requires flat data (non-nested, non-hierarchical).
            Each row should represent a discrete event.
          </p>
          {
            canFlatten &&
            <p>
              If you have nested data, you can <ExternalLink href="http://druid.io/docs/latest/ingestion/flatten-json.html">flatten</ExternalLink> it here.
              If the provided flattening capabilities are not sufficient, please pre-process your data before ingesting it into Druid.
            </p>
          }
          <p>
            Click "Preview" to ensure that your data appears correctly in a row/column orientation.
          </p>
        </Callout>
        <AutoForm
          fields={getParseSpecFormFields()}
          model={parseSpec}
          onChange={p => this.updateSpec(deepSet(spec, 'dataSchema.parser.parseSpec', p))}
        />
        {this.renderFlattenControls()}
        {
          Boolean(sugestedFlattenFields && sugestedFlattenFields.length) &&
          <FormGroup>
            <Button
              icon={IconNames.LIGHTBULB}
              text="Auto add flatten specs"
              onClick={() => {
                this.updateSpec(deepSet(spec, 'dataSchema.parser.parseSpec.flattenSpec.fields', sugestedFlattenFields));
                setTimeout(() => {
                  this.queryForParser();
                }, 10);
              }}
            />
          </FormGroup>
        }
        {
          !selectedFlattenField &&
          <Button
            text="Preview"
            disabled={isBlank}
            onClick={() => this.queryForParser()}
          />
        }
      </div>
      {this.renderNextBar({
        disabled: !parserQueryState.data,
        onNextStage: () => {
          if (!parserQueryState.data) return;
          const possibleTimestampSpec = getTimestampSpec(parserQueryState.data);
          if (possibleTimestampSpec) {
            const newSpec: IngestionSpec = deepSet(spec, 'dataSchema.parser.parseSpec.timestampSpec', possibleTimestampSpec);
            this.updateSpec(newSpec);
          }
        }
      })}
    </>;
  }

  renderFlattenControls() {
    const { spec, selectedFlattenField, selectedFlattenFieldIndex } = this.state;
    const parseSpec: ParseSpec = deepGet(spec, 'dataSchema.parser.parseSpec') || {};
    if (!parseSpecHasFlatten(parseSpec)) return null;

    const close = () => {
      this.setState({
        selectedFlattenFieldIndex: -1,
        selectedFlattenField: null
      });
    };

    const closeAndQuery = () => {
      close();
      setTimeout(() => {
        this.queryForParser();
      }, 10);
    };

    if (selectedFlattenField) {
      return <div className="edit-controls">
        <AutoForm
          fields={getFlattenFieldFormFields()}
          model={selectedFlattenField}
          onChange={(f) => this.setState({ selectedFlattenField: f })}
        />
        <div className="controls-buttons">
          <Button
            className="add-update"
            text={selectedFlattenFieldIndex === -1 ? 'Add' : 'Update'}
            intent={Intent.PRIMARY}
            onClick={() => {
              this.updateSpec(deepSet(spec, `dataSchema.parser.parseSpec.flattenSpec.fields.${selectedFlattenFieldIndex}`, selectedFlattenField));
              closeAndQuery();
            }}
          />
          {
            selectedFlattenFieldIndex !== -1 &&
            <Button
              icon={IconNames.TRASH}
              intent={Intent.DANGER}
              onClick={() => {
                this.updateSpec(deepDelete(spec, `dataSchema.parser.parseSpec.flattenSpec.fields.${selectedFlattenFieldIndex}`));
                closeAndQuery();
              }}
            />
          }
          <Button className="cancel" text="Cancel" onClick={close}/>
        </div>
      </div>;
    } else {
      return <FormGroup>
        <Button
          text="Add column flattening"
          onClick={() => {
            this.setState({
              selectedFlattenField: { type: 'path', name: '', expr: '' },
              selectedFlattenFieldIndex: -1
            });
          }}
        />
        <AnchorButton
          icon={IconNames.INFO_SIGN}
          href="http://druid.io/docs/latest/ingestion/flatten-json.html"
          target="_blank"
          minimal
        />
      </FormGroup>;
    }
  }

  // ==================================================================

  async queryForTimestamp(initRun = false) {
    const { spec, cacheKey } = this.state;
    const ioConfig: IoConfig = deepGet(spec, 'ioConfig') || {};
    const parser: Parser = deepGet(spec, 'dataSchema.parser') || {};

    let issue: string | null = null;
    if (issueWithIoConfig(ioConfig)) {
      issue = `IoConfig not ready, ${issueWithIoConfig(ioConfig)}`;
    } else if (issueWithParser(parser)) {
      issue = `Parser not ready, ${issueWithParser(parser)}`;
    }

    if (issue) {
      this.setState({
        timestampQueryState: initRun ? QueryState.INIT : new QueryState({ error: issue })
      });
      return;
    }

    this.setState({
      timestampQueryState: new QueryState({ loading: true })
    });

    let sampleResponse: SampleResponse;
    try {
      sampleResponse = await sampleForTimestamp(spec, cacheKey);
    } catch (e) {
      this.setState({
        timestampQueryState: new QueryState({ error: e.message })
      });
      return;
    }

    this.setState({
      cacheKey: sampleResponse.cacheKey,
      timestampQueryState: new QueryState({
        data: headerAndRowsFromSampleResponse(sampleResponse)
      })
    });
  }

  renderTimestampStage() {
    const { spec, columnFilter, specialColumnsOnly, timestampQueryState } = this.state;
    const parseSpec: ParseSpec = deepGet(spec, 'dataSchema.parser.parseSpec') || {};
    const timestampSpec: TimestampSpec = deepGet(spec, 'dataSchema.parser.parseSpec.timestampSpec') || {};
    const timestampSpecColumn = getTimestampSpecColumn(timestampSpec);
    const timestampSpecFromColumn = isColumnTimestampSpec(timestampSpec);

    const isBlank = !parseSpec.format;

    let mainFill: JSX.Element | string = '';
    if (timestampQueryState.isInit()) {
      mainFill = <CenterMessage>
        Please enter the timestamp column details on the right
      </CenterMessage>;

    } else  if (timestampQueryState.isLoading()) {
      mainFill = <Loader loading/>;

    } else if (timestampQueryState.error) {
      mainFill = <CenterMessage>
        {`Error: ${timestampQueryState.error}`}
      </CenterMessage>;

    } else if (timestampQueryState.data) {
      const timestampData = timestampQueryState.data;
      mainFill = <div className="table-with-control">
        <div className="table-control">
          <ClearableInput
            value={columnFilter}
            onChange={(columnFilter) => this.setState({ columnFilter })}
            placeholder="Search columns"
          />
          <Switch
            checked={specialColumnsOnly}
            label="Suggested columns only"
            onChange={() => this.setState({ specialColumnsOnly: !specialColumnsOnly })}
          />
        </div>
        <ReactTable
          data={timestampData.rows}
          columns={filterMap(timestampData.header.length ? timestampData.header : ['__error__'], (columnName, i) => {
            const timestamp = columnName === '__time';
            if (!timestamp && !filterMatch(columnName, columnFilter)) return null;
            const selected = timestampSpec.column === columnName;
            const possibleFormat = timestamp ? null : possibleDruidFormatForValues(filterMap(timestampData.rows, d => d.parsed ? d.parsed[columnName] : null));
            if (specialColumnsOnly && !timestamp && !possibleFormat) return null;

            const columnClassName = classNames({
              timestamp,
              selected
            });
            return {
              Header: (
                <div
                  className={classNames({ clickable: !timestamp })}
                  onClick={timestamp ? undefined : () => {
                    const newTimestampSpec = {
                      column: columnName,
                      format: possibleFormat || '!!! Could not auto detect a format !!!'
                    };
                    this.updateSpec(deepSet(spec, 'dataSchema.parser.parseSpec.timestampSpec', newTimestampSpec));
                  }}
                >
                  <div className="column-name">{columnName}</div>
                  <div className="column-detail">
                    {
                      timestamp ?
                        (timestampSpecFromColumn ? `from: '${timestampSpecColumn}'` : `mv: ${timestampSpec.missingValue}`) :
                        (possibleFormat || '')
                    }&nbsp;
                  </div>
                </div>
              ),
              headerClassName: columnClassName,
              className: columnClassName,
              id: String(i),
              accessor: (row: SampleEntry) => row.parsed ? row.parsed[columnName] : null,
              Cell: row => {
                if (columnName === '__error__') {
                  return <NullTableCell value={row.original.error}/>;
                }
                if (row.original.unparseable) {
                  return <NullTableCell unparseable/>;
                }
                return <NullTableCell value={row.value} timestamp={timestamp}/>;
              },
              minWidth: timestamp ? 200 : 100,
              resizable: !timestamp
          };
          })}
          defaultPageSize={50}
          showPagination={false}
          sortable={false}
          className="-striped -highlight"
        />
      </div>;
    }

    return <>
      <div className="main">{mainFill}</div>
      <div className="control">
        <Callout className="intro">
          <p>
            Druid partitions data based on the primary time column of your data.
            This column is stored internally in Druid as <Code>__time</Code>.
            Please specify the primary time column.
            If you do not have any time columns, you can choose "Constant Value" to create a default one.
          </p>
          <p>
            Click "Preview" to check if Druid can properly parse your time values.
          </p>
        </Callout>
        <FormGroup label="Timestamp spec">
          <ButtonGroup>
            <Button
              text="From column"
              active={timestampSpecFromColumn}
              onClick={() => {
                const timestampSpec = {
                  column: 'timestamp',
                  format: 'auto'
                };
                this.updateSpec(deepSet(spec, 'dataSchema.parser.parseSpec.timestampSpec', timestampSpec));
                setTimeout(() => {
                  this.queryForTimestamp();
                }, 10);
              }}
            />
            <Button
              text="Constant value"
              active={!timestampSpecFromColumn}
              onClick={() => {
                this.updateSpec(deepSet(spec, 'dataSchema.parser.parseSpec.timestampSpec', getEmptyTimestampSpec()));
                setTimeout(() => {
                  this.queryForTimestamp();
                }, 10);
              }}
            />
          </ButtonGroup>
        </FormGroup>
        <AutoForm
          fields={getTimestampSpecFormFields()}
          model={timestampSpec}
          onChange={(timestampSpec) => {
            this.updateSpec(deepSet(spec, 'dataSchema.parser.parseSpec.timestampSpec', timestampSpec));
          }}
        />
        <Button
          text="Preview"
          disabled={isBlank}
          onClick={() => this.queryForTimestamp()}
        />
      </div>
      {this.renderNextBar({
        disabled: !timestampQueryState.data
      })}
    </>;
  }

  // ==================================================================

  async queryForTransform(initRun = false) {
    const { spec, cacheKey } = this.state;
    const ioConfig: IoConfig = deepGet(spec, 'ioConfig') || {};
    const parser: Parser = deepGet(spec, 'dataSchema.parser') || {};

    let issue: string | null = null;
    if (issueWithIoConfig(ioConfig)) {
      issue = `IoConfig not ready, ${issueWithIoConfig(ioConfig)}`;
    } else if (issueWithParser(parser)) {
      issue = `Parser not ready, ${issueWithParser(parser)}`;
    }

    if (issue) {
      this.setState({
        transformQueryState: initRun ? QueryState.INIT : new QueryState({ error: issue })
      });
      return;
    }

    this.setState({
      transformQueryState: new QueryState({ loading: true })
    });

    let sampleResponse: SampleResponse;
    try {
      sampleResponse = await sampleForTransform(spec, cacheKey);
    } catch (e) {
      this.setState({
        transformQueryState: new QueryState({ error: e.message })
      });
      return;
    }

    this.setState({
      cacheKey: sampleResponse.cacheKey,
      transformQueryState: new QueryState({
        data: headerAndRowsFromSampleResponse(sampleResponse)
      })
    });
  }

  renderTransformStage() {
    const { spec, columnFilter, specialColumnsOnly, transformQueryState, selectedTransformIndex } = this.state;
    const transforms: Transform[] = deepGet(spec, 'dataSchema.transformSpec.transforms') || [];

    let mainFill: JSX.Element | string = '';
    if (transformQueryState.isInit()) {
      mainFill = <CenterMessage>
        {`Please fill in the previous steps`}
      </CenterMessage>;

    } else  if (transformQueryState.isLoading()) {
      mainFill = <Loader loading/>;

    } else if (transformQueryState.error) {
      mainFill = <CenterMessage>
        {`Error: ${transformQueryState.error}`}
      </CenterMessage>;

    } else if (transformQueryState.data) {
      mainFill = <div className="table-with-control">
        <div className="table-control">
          <ClearableInput
            value={columnFilter}
            onChange={(columnFilter) => this.setState({ columnFilter })}
            placeholder="Search columns"
          />
          <Switch
            checked={specialColumnsOnly}
            label="Transformed columns only"
            onChange={() => this.setState({ specialColumnsOnly: !specialColumnsOnly })}
            disabled={!transforms.length}
          />
        </div>
        <ReactTable
          data={transformQueryState.data.rows}
          columns={filterMap(transformQueryState.data.header, (columnName, i) => {
            if (!filterMatch(columnName, columnFilter)) return null;
            const timestamp = columnName === '__time';
            const transformIndex = transforms.findIndex(f => f.name === columnName);
            if (transformIndex === -1 && specialColumnsOnly) return null;
            const transform = transforms[transformIndex];

            const columnClassName = classNames({
              transformed: transform,
              selected: transform && transformIndex === selectedTransformIndex
            });
            return {
              Header: (
                <div
                  className={classNames('clickable')}
                  onClick={() => {
                    if (transform) {
                      this.setState({
                        selectedTransformIndex: transformIndex,
                        selectedTransform: transform
                      });
                    } else {
                      this.setState({
                        selectedTransformIndex: -1,
                        selectedTransform: {
                          type: 'expression',
                          name: columnName,
                          expression: escapeColumnName(columnName)
                        }
                      });
                    }
                  }}
                >
                  <div className="column-name">{columnName}</div>
                  <div className="column-detail">
                    {transform ? `= ${transform.expression}` : ''}&nbsp;
                  </div>
                </div>
              ),
              headerClassName: columnClassName,
              className: columnClassName,
              id: String(i),
              accessor: row => row.parsed ? row.parsed[columnName] : null,
              Cell: row => <NullTableCell value={row.value} timestamp={timestamp}/>
            };
          })}
          defaultPageSize={50}
          showPagination={false}
          sortable={false}
          className="-striped -highlight"
        />
      </div>;
    }

    return <>
      <div className="main">{mainFill}</div>
      <div className="control">
        <Callout className="intro">
          <p className="optional">
            Optional
          </p>
          <p>
            Druid can perform simple <ExternalLink href="http://druid.io/docs/latest/ingestion/transform-spec.html#transforms">transforms</ExternalLink> of column values.
          </p>
          <p>
            Click "Preview" to see the result of any specified transforms.
          </p>
        </Callout>
        {this.renderTransformControls()}
        <Button
          text="Preview"
          onClick={() => this.queryForTransform()}
        />
      </div>
      {this.renderNextBar({
        disabled: !transformQueryState.data,
        onNextStage: () => {
          if (!transformQueryState.data) return;
          this.updateSpec(updateSchemaWithSample(spec, transformQueryState.data, 'specific', true));
        }
      })}
    </>;
  }

  renderTransformControls() {
    const { spec, selectedTransform, selectedTransformIndex } = this.state;

    const close = () => {
      this.setState({
        selectedTransformIndex: -1,
        selectedTransform: null
      });
    };

    const closeAndQuery = () => {
      close();
      setTimeout(() => {
        this.queryForTransform();
      }, 10);
    };

    if (selectedTransform) {
      return <div className="edit-controls">
        <AutoForm
          fields={getTransformFormFields()}
          model={selectedTransform}
          onChange={(selectedTransform) => this.setState({ selectedTransform })}
        />
        <div className="controls-buttons">
          <Button
            className="add-update"
            text={selectedTransformIndex === -1 ? 'Add' : 'Update'}
            intent={Intent.PRIMARY}
            onClick={() => {
              this.updateSpec(deepSet(spec, `dataSchema.transformSpec.transforms.${selectedTransformIndex}`, selectedTransform));
              closeAndQuery();
            }}
          />
          {
            selectedTransformIndex !== -1 &&
            <Button
              icon={IconNames.TRASH}
              intent={Intent.DANGER}
              onClick={() => {
                this.updateSpec(deepDelete(spec, `dataSchema.transformSpec.transforms.${selectedTransformIndex}`));
                closeAndQuery();
              }}
            />
          }
          <Button className="cancel" text="Cancel" onClick={close}/>
        </div>
      </div>;

    } else {
      return <FormGroup>
        <Button
          text="Add column transform"
          onClick={() => {
            this.setState({
              selectedTransformIndex: -1,
              selectedTransform: { type: 'expression', name: '', expression: '' }
            });
          }}
        />
      </FormGroup>;
    }
  }

  // ==================================================================

  async queryForFilter(initRun = false) {
    const { spec, cacheKey } = this.state;
    const ioConfig: IoConfig = deepGet(spec, 'ioConfig') || {};
    const parser: Parser = deepGet(spec, 'dataSchema.parser') || {};

    let issue: string | null = null;
    if (issueWithIoConfig(ioConfig)) {
      issue = `IoConfig not ready, ${issueWithIoConfig(ioConfig)}`;
    } else if (issueWithParser(parser)) {
      issue = `Parser not ready, ${issueWithParser(parser)}`;
    }

    if (issue) {
      this.setState({
        filterQueryState: initRun ? QueryState.INIT : new QueryState({ error: issue })
      });
      return;
    }

    this.setState({
      filterQueryState: new QueryState({ loading: true })
    });

    let sampleResponse: SampleResponse;
    try {
      sampleResponse = await sampleForFilter(spec, cacheKey);
    } catch (e) {
      this.setState({
        filterQueryState: new QueryState({ error: e.message })
      });
      return;
    }

    this.setState({
      cacheKey: sampleResponse.cacheKey,
      filterQueryState: new QueryState({
        data: headerAndRowsFromSampleResponse(sampleResponse, undefined, true)
      })
    });
  }

  renderFilterStage() {
    const { spec, columnFilter, filterQueryState, selectedFilter, selectedFilterIndex, showGlobalFilter } = this.state;
    const parseSpec: ParseSpec = deepGet(spec, 'dataSchema.parser.parseSpec') || {};
    const { dimensionFilters } = splitFilter(deepGet(spec, 'dataSchema.transformSpec.filter'));

    const isBlank = !parseSpec.format;

    let mainFill: JSX.Element | string = '';
    if (filterQueryState.isInit()) {
      mainFill = <CenterMessage>
        Please enter more details for the previous steps
      </CenterMessage>;

    } else if (filterQueryState.isLoading()) {
      mainFill = <Loader loading/>;

    } else if (filterQueryState.error) {
      mainFill = <CenterMessage>
        {`Error: ${filterQueryState.error}`}
      </CenterMessage>;

    } else if (filterQueryState.data) {
      mainFill = <div className="table-with-control">
        <div className="table-control">
          <ClearableInput
            value={columnFilter}
            onChange={(columnFilter) => this.setState({ columnFilter })}
            placeholder="Search columns"
          />
        </div>
        <ReactTable
          data={filterQueryState.data.rows}
          columns={filterMap(filterQueryState.data.header, (columnName, i) => {
            if (!filterMatch(columnName, columnFilter)) return null;
            const timestamp = columnName === '__time';
            const filterIndex = dimensionFilters.findIndex(f => f.dimension === columnName);
            const filter = dimensionFilters[filterIndex];

            const columnClassName = classNames({
              filtered: filter,
              selected: filter && filterIndex === selectedFilterIndex
            });
            return {
              Header: (
                <div
                  className={classNames('clickable')}
                  onClick={() => {
                    if (timestamp) {
                      this.setState({
                        showGlobalFilter: true
                      });
                    } else if (filter) {
                      this.setState({
                        selectedFilterIndex: filterIndex,
                        selectedFilter: filter
                      });
                    } else {
                      this.setState({
                        selectedFilterIndex: -1,
                        selectedFilter: { type: 'selector', dimension: columnName, value: '' }
                      });
                    }
                  }}
                >
                  <div className="column-name">{columnName}</div>
                  <div className="column-detail">
                    {filter ? `(filtered)` : ''}&nbsp;
                  </div>
                </div>
              ),
              headerClassName: columnClassName,
              className: columnClassName,
              id: String(i),
              accessor: row => row.parsed ? row.parsed[columnName] : null,
              Cell: row => <NullTableCell value={row.value} timestamp={timestamp}/>
            };
          })}
          defaultPageSize={50}
          showPagination={false}
          sortable={false}
          className="-striped -highlight"
        />
      </div>;
    }

    return <>
      <div className="main">{mainFill}</div>
      <div className="control">
        <Callout className="intro">
          <p className="optional">
            Optional
          </p>
          <p>
            Druid can <ExternalLink href="http://druid.io/docs/latest/querying/filters.html">filter</ExternalLink> out unwanted data.
          </p>
          <p>
            Click "Preview" to see the impact of any specified filters.
          </p>
        </Callout>
        {!showGlobalFilter && this.renderColumnFilterControls()}
        {!selectedFilter && this.renderGlobalFilterControls()}
        {
          (!selectedFilter && !showGlobalFilter) &&
          <Button
            text="Preview"
            disabled={isBlank}
            onClick={() => this.queryForFilter()}
          />
        }
      </div>
      {this.renderNextBar({})}
    </>;
  }

  renderColumnFilterControls() {
    const { spec, selectedFilter, selectedFilterIndex } = this.state;

    const close = () => {
      this.setState({
        selectedFilterIndex: -1,
        selectedFilter: null
      });
    };

    const closeAndQuery = () => {
      close();
      setTimeout(() => {
        this.queryForFilter();
      }, 10);
    };

    if (selectedFilter) {
      return <div className="edit-controls">
        <AutoForm
          fields={getFilterFormFields()}
          model={selectedFilter}
          onChange={(f) => this.setState({ selectedFilter: f })}
        />
        <div className="controls-buttons">
          <Button
            className="add-update"
            text={selectedFilterIndex === -1 ? 'Add' : 'Update'}
            intent={Intent.PRIMARY}
            onClick={() => {
              const curFilter = splitFilter(deepGet(spec, 'dataSchema.transformSpec.filter'));
              const newFilter = joinFilter(deepSet(curFilter, `dimensionFilters.${selectedFilterIndex}`, selectedFilter));
              this.updateSpec(deepSet(spec, 'dataSchema.transformSpec.filter', newFilter));
              closeAndQuery();
            }}
          />
          {
            selectedFilterIndex !== -1 &&
            <Button
              icon={IconNames.TRASH}
              intent={Intent.DANGER}
              onClick={() => {
                const curFilter = splitFilter(deepGet(spec, 'dataSchema.transformSpec.filter'));
                const newFilter = joinFilter(deepDelete(curFilter, `dimensionFilters.${selectedFilterIndex}`));
                this.updateSpec(deepSet(spec, 'dataSchema.transformSpec.filter', newFilter));
                closeAndQuery();
              }}
            />
          }
          <Button className="cancel" text="Cancel" onClick={close}/>
        </div>
      </div>;
    } else {
      return <FormGroup>
        <Button
          text="Add column filter"
          onClick={() => {
            this.setState({
              selectedFilter: { type: 'selector', dimension: '', value: '' },
              selectedFilterIndex: -1
            });
          }}
        />
      </FormGroup>;
    }
  }

  renderGlobalFilterControls() {
    const { spec, showGlobalFilter } = this.state;
    const intervals: string[] = deepGet(spec, 'dataSchema.granularitySpec.intervals');
    const { restFilter } = splitFilter(deepGet(spec, 'dataSchema.transformSpec.filter'));
    const hasGlobalFilter = Boolean(intervals || restFilter);

    if (showGlobalFilter) {
      return <div className="edit-controls">
        <AutoForm
          fields={[
            {
              name: 'dataSchema.granularitySpec.intervals',
              label: 'Time intervals',
              type: 'string-array',
              placeholder: 'ex: 2018-01-01/2018-06-01',
              info: <>
                A comma separated list of intervals for the raw data being ingested.
                Ignored for real-time ingestion.
              </>
            }
          ]}
          model={spec}
          onChange={s => this.updateSpec(s)}
        />
        <FormGroup label="Extra filter">
          <JSONInput
            value={restFilter}
            onChange={f => {
              const curFilter = splitFilter(deepGet(spec, 'dataSchema.transformSpec.filter'));
              const newFilter = joinFilter(deepSet(curFilter, `restFilter`, f));
              this.updateSpec(deepSet(spec, 'dataSchema.transformSpec.filter', newFilter));
            }}
            height="200px"
          />
        </FormGroup>
        <div className="controls-buttons">
          <Button
            className="add-update"
            text="Preview"
            intent={Intent.PRIMARY}
            onClick={() => this.queryForFilter()}
          />
          <Button
            className="cancel"
            text="Close"
            onClick={() => this.setState({ showGlobalFilter: false })}
          />
        </div>
      </div>;
    } else {
      return <FormGroup>
        <Button
          text={`${hasGlobalFilter ? 'Edit' : 'Add'} global filter`}
          onClick={() => this.setState({ showGlobalFilter: true })}
        />
      </FormGroup>;
    }
  }

  // ==================================================================

  async queryForSchema(initRun = false) {
    const { spec, cacheKey } = this.state;
    const ioConfig: IoConfig = deepGet(spec, 'ioConfig') || {};
    const parser: Parser = deepGet(spec, 'dataSchema.parser') || {};

    let issue: string | null = null;
    if (issueWithIoConfig(ioConfig)) {
      issue = `IoConfig not ready, ${issueWithIoConfig(ioConfig)}`;
    } else if (issueWithParser(parser)) {
      issue = `Parser not ready, ${issueWithParser(parser)}`;
    }

    if (issue) {
      this.setState({
        schemaQueryState: initRun ? QueryState.INIT : new QueryState({ error: issue })
      });
      return;
    }

    this.setState({
      schemaQueryState: new QueryState({ loading: true })
    });

    let sampleResponse: SampleResponse;
    try {
      sampleResponse = await sampleForSchema(spec, cacheKey);
    } catch (e) {
      this.setState({
        schemaQueryState: new QueryState({ error: e.message })
      });
      return;
    }

    this.setState({
      cacheKey: sampleResponse.cacheKey,
      schemaQueryState: new QueryState({
        data: headerAndRowsFromSampleResponse(sampleResponse)
      })
    });
  }

  renderSchemaStage() {
    const { spec, columnFilter, schemaQueryState, selectedDimensionSpec, selectedDimensionSpecIndex, selectedMetricSpec, selectedMetricSpecIndex } = this.state;
    const metricsSpec: MetricSpec[] = deepGet(spec, 'dataSchema.metricsSpec') || [];
    const dimensionsSpec: DimensionsSpec = deepGet(spec, 'dataSchema.parser.parseSpec.dimensionsSpec') || {};
    const rollup: boolean = Boolean(deepGet(spec, 'dataSchema.granularitySpec.rollup'));
    const somethingSelected = Boolean(selectedDimensionSpec || selectedMetricSpec);
    const dimensionMode = getDimensionMode(spec);

    let mainFill: JSX.Element | string = '';
    if (schemaQueryState.isInit()) {
      mainFill = <CenterMessage>
        Please enter more details for the previous steps
      </CenterMessage>;

    } else if (schemaQueryState.isLoading()) {
      mainFill = <Loader loading/>;

    } else if (schemaQueryState.error) {
      mainFill = <CenterMessage>
        {`Error: ${schemaQueryState.error}`}
      </CenterMessage>;

    } else if (schemaQueryState.data) {
      const dimensionMetricSortedHeader = sortWithPrefixSuffix(schemaQueryState.data.header, ['__time'], metricsSpec.map(getMetricSpecName));
      mainFill = <div className="table-with-control">
        <div className="table-control">
          <ClearableInput
            value={columnFilter}
            onChange={(columnFilter) => this.setState({ columnFilter })}
            placeholder="Search columns"
          />
        </div>
        <ReactTable
          data={schemaQueryState.data.rows}
          columns={filterMap(dimensionMetricSortedHeader, (columnName, i) => {
            if (!filterMatch(columnName, columnFilter)) return null;

            const metricSpecIndex = metricsSpec.findIndex(m => getMetricSpecName(m) === columnName);
            const metricSpec = metricsSpec[metricSpecIndex];

            if (metricSpec) {
              const columnClassName = classNames('metric', {
                selected: metricSpec && metricSpecIndex === selectedMetricSpecIndex
              });
              return {
                Header: (
                  <div
                    className="clickable"
                    onClick={() => {
                      this.setState({
                        selectedMetricSpecIndex: metricSpecIndex,
                        selectedMetricSpec: metricSpec,
                        selectedDimensionSpecIndex: -1,
                        selectedDimensionSpec: null
                      });
                    }}
                  >
                    <div className="column-name">{columnName}</div>
                    <div className="column-detail">
                      {metricSpec.type}&nbsp;
                    </div>
                  </div>
                ),
                headerClassName: columnClassName,
                className: columnClassName,
                id: String(i),
                accessor: row => row.parsed ? row.parsed[columnName] : null,
                Cell: row => <NullTableCell value={row.value}/>
              };
            } else {
              const timestamp = columnName === '__time';
              const dimensionSpecIndex = dimensionsSpec.dimensions ? dimensionsSpec.dimensions.findIndex(d => getDimensionSpecName(d) === columnName) : -1;
              const dimensionSpec = dimensionsSpec.dimensions ? dimensionsSpec.dimensions[dimensionSpecIndex] : null;
              const dimensionSpecType = dimensionSpec ? getDimensionSpecType(dimensionSpec) : null;

              const columnClassName = classNames(timestamp ? 'timestamp' : 'dimension', dimensionSpecType || 'string', {
                selected: dimensionSpec && dimensionSpecIndex === selectedDimensionSpecIndex
              });
              return {
                Header: (
                  <div
                    className="clickable"
                    onClick={() => {
                      if (timestamp) {
                        this.setState({
                          selectedDimensionSpecIndex: -1,
                          selectedDimensionSpec: null,
                          selectedMetricSpecIndex: -1,
                          selectedMetricSpec: null
                        });
                        return;
                      }

                      if (!dimensionSpec) return;
                      this.setState({
                        selectedDimensionSpecIndex: dimensionSpecIndex,
                        selectedDimensionSpec: inflateDimensionSpec(dimensionSpec),
                        selectedMetricSpecIndex: -1,
                        selectedMetricSpec: null
                      });
                    }}
                  >
                    <div className="column-name">{columnName}</div>
                    <div className="column-detail">
                      {timestamp ? 'long (time column)' : (dimensionSpecType || 'string (auto)')}&nbsp;
                    </div>
                  </div>
                ),
                headerClassName: columnClassName,
                className: columnClassName,
                id: String(i),
                accessor: (row: SampleEntry) => row.parsed ? row.parsed[columnName] : null,
                Cell: row => <NullTableCell value={row.value} timestamp={timestamp}/>
              };
            }
          })}
          defaultPageSize={50}
          showPagination={false}
          sortable={false}
          className="-striped -highlight"
        />
      </div>;
    }

    return <>
      <div className="main">{mainFill}</div>
      <div className="control">
        <Callout className="intro">
          <p>
            Each column in Druid must have an assigned type (string, long, float, complex, etc).
            Default primitive types have been automatically assigned to your columns.
            If you want to change the type, click on the column header.
          </p>
          <p>
            Select whether or not you want to <ExternalLink href="http://druid.io/docs/latest/tutorials/tutorial-rollup.html">roll-up</ExternalLink> your data.
          </p>
        </Callout>
        {
          !somethingSelected &&
          <>
            <FormGroup>
              <Switch
                checked={dimensionMode === 'specific'}
                onChange={() => this.setState({ newDimensionMode: dimensionMode === 'specific' ? 'auto-detect' : 'specific' })}
                label="Set dimensions and metrics"
              />
              <Popover
                content={
                  <div className="label-info-text">
                    <p>
                      Select whether or not you want to set an explicit list of <ExternalLink href="http://druid.io/docs/latest/ingestion/ingestion-spec.html#dimensionsspec">dimensions</ExternalLink> and <ExternalLink href="http://druid.io/docs/latest/querying/aggregations.html">metrics</ExternalLink>.
                      Explicitly setting dimensions and metrics can lead to better compression and performance.
                      If you disable this option, Druid will try to auto-detect fields in your data and treat them as individual columns.
                    </p>
                  </div>
                }
                position="left-bottom"
              >
                <Icon icon={IconNames.INFO_SIGN} iconSize={14}/>
              </Popover>
            </FormGroup>
            {
              dimensionMode === 'auto-detect' &&
              <AutoForm
                fields={[
                  {
                    name: 'dataSchema.parser.parseSpec.dimensionsSpec.dimensionExclusions',
                    label: 'Exclusions',
                    type: 'string-array',
                    info: <>
                      Provide a comma separated list of columns (use the column name from the raw data) you do not want Druid to ingest.
                    </>
                  }
                ]}
                model={spec}
                onChange={s => this.updateSpec(s)}
              />
            }
            <FormGroup>
              <Switch
                checked={rollup}
                onChange={() => this.setState({ newRollup: !rollup })}
                labelElement="Rollup"
              />
              <Popover
                content={
                  <div className="label-info-text">
                    <p>
                      If you enable roll-up, Druid will try to pre-aggregate data before indexing it to conserve storage.
                      The primary timestamp will be truncated to the specified query granularity, and rows containing the same string field values will be aggregated together.
                    </p>
                    <p>
                      If you enable rollup, you must specify which columns are <a href="http://druid.io/docs/latest/ingestion/ingestion-spec.html#dimensionsspec">dimensions</a> (fields you want to group and filter on), and which are <a href="http://druid.io/docs/latest/querying/aggregations.html">metrics</a> (fields you want to aggregate on).
                    </p>
                  </div>
                }
                position="left-bottom"
              >
                <Icon icon={IconNames.INFO_SIGN} iconSize={14}/>
              </Popover>
            </FormGroup>
            <AutoForm
              fields={[
                {
                  name: 'dataSchema.granularitySpec.queryGranularity',
                  label: 'Query granularity',
                  type: 'string',
                  suggestions: ['NONE', 'MINUTE', 'HOUR', 'DAY'],
                  info: <>
                    This granularity determines how timestamps will be truncated (not at all, to the minute, hour, day, etc).
                    After data is rolled up, this granularity becomes the minimum granularity you can query data at.
                  </>
                }
              ]}
              model={spec}
              onChange={s => this.updateSpec(s)}
            />
          </>
        }
        {!selectedMetricSpec && this.renderDimensionSpecControls()}
        {!selectedDimensionSpec && this.renderMetricSpecControls()}
        {this.renderChangeRollupAction()}
        {this.renderChangeDimensionModeAction()}
      </div>
      {this.renderNextBar({
        disabled: !schemaQueryState.data
      })}
    </>;
  }

  renderChangeRollupAction() {
    const { newRollup, spec, cacheKey } = this.state;
    if (newRollup === null) return;

    return <AsyncActionDialog
      action={async () => {
        const sampleResponse = await sampleForTransform(spec, cacheKey);
        this.updateSpec(updateSchemaWithSample(spec, headerAndRowsFromSampleResponse(sampleResponse), getDimensionMode(spec), newRollup));
        setTimeout(() => {
          this.queryForSchema();
        }, 10);
      }}
      confirmButtonText={`Yes - ${newRollup ? 'enable' : 'disable'} rollup`}
      successText={`Rollup was ${newRollup ? 'enabled' : 'disabled'}. Schema has been updated.`}
      failText="Could change rollup"
      intent={Intent.WARNING}
      onClose={() => this.setState({ newRollup: null })}
    >
      <p>
        {`Are you sure you want to ${newRollup ? 'enable' : 'disable'} rollup?`}
      </p>
      <p>
        Making this change will reset any work you have done in this section.
      </p>
    </AsyncActionDialog>;
  }

  renderChangeDimensionModeAction() {
    const { newDimensionMode, spec, cacheKey } = this.state;
    if (newDimensionMode === null) return;
    const autoDetect = newDimensionMode === 'auto-detect';

    return <AsyncActionDialog
      action={async () => {
        const sampleResponse = await sampleForTransform(spec, cacheKey);
        this.updateSpec(updateSchemaWithSample(spec, headerAndRowsFromSampleResponse(sampleResponse), newDimensionMode, getRollup(spec)));
        setTimeout(() => {
          this.queryForSchema();
        }, 10);
      }}
      confirmButtonText={`Yes - ${autoDetect ? 'auto detect' : 'explicitly set'} columns`}
      successText={`Dimension mode changes to ${autoDetect ? 'auto detect' : 'specific list'}. Schema has been updated.`}
      failText="Could change dimension mode"
      intent={Intent.WARNING}
      onClose={() => this.setState({ newDimensionMode: null })}
    >
      <p>
        {
          autoDetect ?
          'Are you sure you don’t want to set the dimensions and metrics explicitly?' :
          'Are you sure you want to set dimensions and metrics explicitly?'
        }
      </p>
      <p>
        Making this change will reset any work you have done in this section.
      </p>
    </AsyncActionDialog>;
  }

  renderDimensionSpecControls() {
    const { spec, selectedDimensionSpec, selectedDimensionSpecIndex } = this.state;

    const close = () => {
      this.setState({
        selectedDimensionSpecIndex: -1,
        selectedDimensionSpec: null
      });
    };

    const closeAndQuery = () => {
      close();
      setTimeout(() => {
        this.queryForSchema();
      }, 10);
    };

    if (selectedDimensionSpec) {
      return <div className="edit-controls">
        <AutoForm
          fields={getDimensionSpecFormFields()}
          model={selectedDimensionSpec}
          onChange={(selectedDimensionSpec) => this.setState({ selectedDimensionSpec })}
        />
        <div className="controls-buttons">
          <Button
            className="add-update"
            text={selectedDimensionSpecIndex === -1 ? 'Add' : 'Update'}
            intent={Intent.PRIMARY}
            onClick={() => {
              this.updateSpec(deepSet(spec, `dataSchema.parser.parseSpec.dimensionsSpec.dimensions.${selectedDimensionSpecIndex}`, selectedDimensionSpec));
              closeAndQuery();
            }}
          />
          {
            selectedDimensionSpecIndex !== -1 &&
            <Button
              icon={IconNames.TRASH}
              intent={Intent.DANGER}
              onClick={() => {
                const curDimensions = deepGet(spec, `dataSchema.parser.parseSpec.dimensionsSpec.dimensions`) || [];
                if (curDimensions.length <= 1) return; // Guard against removing the last dimension, ToDo: some better feedback here would be good

                this.updateSpec(deepDelete(spec, `dataSchema.parser.parseSpec.dimensionsSpec.dimensions.${selectedDimensionSpecIndex}`));
                closeAndQuery();
              }}
            />
          }
          <Button className="cancel" text="Cancel" onClick={close}/>
        </div>
      </div>;

    } else {
      return <FormGroup>
        <Button
          text="Add dimension"
          disabled={getDimensionMode(spec) !== 'specific'}
          onClick={() => {
            this.setState({
              selectedDimensionSpecIndex: -1,
              selectedDimensionSpec: {
                name: 'new_dimension',
                type: 'string'
              }
            });
          }}
        />
      </FormGroup>;
    }
  }

  renderMetricSpecControls() {
    const { spec, selectedMetricSpec, selectedMetricSpecIndex } = this.state;

    const close = () => {
      this.setState({
        selectedMetricSpecIndex: -1,
        selectedMetricSpec: null
      });
    };

    const closeAndQuery = () => {
      close();
      setTimeout(() => {
        this.queryForSchema();
      }, 10);
    };

    if (selectedMetricSpec) {
      return <div className="edit-controls">
        <AutoForm
          fields={getMetricSpecFormFields()}
          model={selectedMetricSpec}
          onChange={(selectedMetricSpec) => this.setState({ selectedMetricSpec })}
        />
        <div className="controls-buttons">
          <Button
            className="add-update"
            text={selectedMetricSpecIndex === -1 ? 'Add' : 'Update'}
            intent={Intent.PRIMARY}
            onClick={() => {
              this.updateSpec(deepSet(spec, `dataSchema.metricsSpec.${selectedMetricSpecIndex}`, selectedMetricSpec));
              closeAndQuery();
            }}
          />
          {
            selectedMetricSpecIndex !== -1 &&
            <Button
              icon={IconNames.TRASH}
              intent={Intent.DANGER}
              onClick={() => {
                this.updateSpec(deepDelete(spec, `dataSchema.metricsSpec.${selectedMetricSpecIndex}`));
                closeAndQuery();
              }}
            />
          }
          <Button className="cancel" text="Cancel" onClick={close}/>
        </div>
      </div>;

    } else {
      return <FormGroup>
        <Button
          text="Add metric"
          onClick={() => {
            this.setState({
              selectedMetricSpecIndex: -1,
              selectedMetricSpec: {
                name: 'sum_blah',
                type: 'doubleSum',
                fieldName: ''
              }
            });
          }}
        />
      </FormGroup>;
    }
  }

  // ==================================================================

  renderPartitionStage() {
    const { spec } = this.state;
    const tuningConfig: TuningConfig = deepGet(spec, 'tuningConfig') || {};
    const granularitySpec: GranularitySpec = deepGet(spec, 'dataSchema.granularitySpec') || {};
    const myIsParallel = isParallel(spec);

    return <>
      <div className="main">
        <H5>Primary partitioning (by time)</H5>
        <AutoForm
          fields={[
            {
              name: 'type',
              type: 'string',
              suggestions: ['uniform', 'arbitrary'],
              info: <>
                This spec is used to generated segments with uniform intervals.
              </>
            },
            {
              name: 'segmentGranularity',
              type: 'string',
              suggestions: ['HOUR', 'DAY', 'WEEK', 'MONTH', 'YEAR'],
              isDefined: (g: GranularitySpec) => g.type === 'uniform',
              info: <>
                The granularity to create time chunks at.
                Multiple segments can be created per time chunk.
                For example, with 'DAY' segmentGranularity, the events of the same day fall into the same time chunk which can be optionally further partitioned into multiple segments based on other configurations and input size.
              </>
            }
          ]}
          model={granularitySpec}
          onChange={g => this.updateSpec(deepSet(spec, 'dataSchema.granularitySpec', g))}
        />
      </div>
      <div className="other">
        <H5>Secondary partitioning</H5>
        <AutoForm
          fields={[
            {
              name: 'partitionDimensions',
              type: 'string-array',
              disabled: myIsParallel,
              info: <>
                <p>
                  Does not currently work with parallel ingestion
                </p>
                <p>
                  The dimensions to partition on.
                  Leave blank to select all dimensions. Only used with forceGuaranteedRollup = true, will be ignored otherwise.
                </p>
              </>
            },
            {
              name: 'forceGuaranteedRollup',
              type: 'boolean',
              disabled: myIsParallel,
              info: <>
                <p>
                  Does not currently work with parallel ingestion
                </p>
                <p>
                  Forces guaranteeing the perfect rollup.
                  The perfect rollup optimizes the total size of generated segments and querying time while indexing time will be increased.
                  If this is set to true, the index task will read the entire input data twice: one for finding the optimal number of partitions per time chunk and one for generating segments.
                </p>
              </>
            },
            {
              name: 'targetPartitionSize',
              type: 'number',
              info: <>
                Target number of rows to include in a partition, should be a number that targets segments of 500MB~1GB.
              </>
            },
            {
              name: 'numShards',
              type: 'number',
              info: <>
                Directly specify the number of shards to create.
                If this is specified and 'intervals' is specified in the granularitySpec, the index task can skip the determine intervals/partitions pass through the data. numShards cannot be specified if maxRowsPerSegment is set.
              </>
            },
            {
              name: 'maxRowsPerSegment',
              type: 'number',
              defaultValue: 5000000,
              info: <>
                Determines how many rows are in each segment.
              </>
            },
            {
              name: 'maxTotalRows',
              type: 'number',
              defaultValue: 20000000,
              info: <>
                Total number of rows in segments waiting for being pushed.
              </>
            }
          ]}
          model={tuningConfig}
          onChange={t => this.updateSpec(deepSet(spec, 'tuningConfig', t))}
        />
      </div>
      <div className="control">
        <Callout className="intro">
          <p className="optional">
            Optional
          </p>
          <p>
            Configure how Druid will partition data.
          </p>
        </Callout>
        {this.renderParallelPickerIfNeeded()}
      </div>
      {this.renderNextBar({})}
    </>;
  }

  // ==================================================================

  renderTuningStage() {
    const { spec } = this.state;
    const ioConfig: IoConfig = deepGet(spec, 'ioConfig') || {};
    const tuningConfig: TuningConfig = deepGet(spec, 'tuningConfig') || {};

    const ingestionComboType = getIngestionComboType(spec);
    const inputTuningFields = ingestionComboType ? getIoConfigTuningFormFields(ingestionComboType) : null;
    return <>
      <div className="main">
        <H5>Input tuning</H5>
        {
          inputTuningFields ?
          (
            inputTuningFields.length ?
            <AutoForm
              fields={inputTuningFields}
              model={ioConfig}
              onChange={c => this.updateSpec(deepSet(spec, 'ioConfig', c))}
            /> :
            <div>
              {
                ioConfig.firehose ?
                  `No specific tuning configs for firehose of type '${deepGet(ioConfig, 'firehose.type')}'.` :
                  `No specific tuning configs.`
              }
            </div>
          ) :
          <JSONInput
            value={ioConfig}
            onChange={c => this.updateSpec(deepSet(spec, 'ioConfig', c))}
            height="300px"
          />
        }
      </div>
      <div className="other">
        <H5>General tuning</H5>
        <AutoForm
          fields={getTuningSpecFormFields()}
          model={tuningConfig}
          onChange={t => this.updateSpec(deepSet(spec, 'tuningConfig', t))}
        />
      </div>
      <div className="control">
        <Callout className="intro">
          <p className="optional">
            Optional
          </p>
          <p>
            Fine tune how Druid will ingest data.
          </p>
        </Callout>
        {this.renderParallelPickerIfNeeded()}
      </div>
      {this.renderNextBar({})}
    </>;
  }

  renderParallelPickerIfNeeded() {
    const { spec } = this.state;
    if (!hasParallelAbility(spec)) return null;

    return <FormGroup>
      <Switch
        large
        checked={isParallel(spec)}
        onChange={() => this.updateSpec(changeParallel(spec, !isParallel(spec)))}
        labelElement={<>
          {'Parallel indexing '}
          <Popover
            content={
              <div className="label-info-text">
                Druid currently has two types of native batch indexing tasks, <Code>index_parallel</Code> which runs tasks in parallel on multiple MiddleManager processes, and <Code>index</Code> which will run a single indexing task locally on a single MiddleManager.
              </div>
            }
            position="left-bottom"
          >
            <Icon icon={IconNames.INFO_SIGN} iconSize={16}/>
          </Popover>
        </>}
      />
    </FormGroup>;
  }

  // ==================================================================

  renderPublishStage() {
    const { spec } = this.state;

    return <>
      <div className="main">
        <H5>Publish configuration</H5>
        <AutoForm
          fields={[
            {
              name: 'dataSchema.dataSource',
              label: 'Datasource name',
              type: 'string',
              info: <>
                This is the name of the data source (table) in Druid.
              </>
            },
            {
              name: 'ioConfig.appendToExisting',
              label: 'Append to existing',
              type: 'boolean',
              info: <>
                Creates segments as additional shards of the latest version, effectively appending to the segment set instead of replacing it.
              </>
            }
          ]}
          model={spec}
          onChange={s => this.updateSpec(s)}
        />
      </div>
      <div className="other"/>
      <div className="control">
        <Callout className="intro">
          <p>
            Configure behavior of indexed data once it reaches Druid.
          </p>
        </Callout>
      </div>
      {this.renderNextBar({})}
    </>;
  }

  // ==================================================================

  renderJsonSpecStage() {
    const { goToTask } = this.props;
    const { spec } = this.state;

    return <>
      <div className="main">
        <JSONInput
          value={spec}
          onChange={(s) => {
            if (!s) return;
            this.updateSpec(s);
          }}
          height="100%"
        />
      </div>
      <div className="control">
        <Callout className="intro">
          <p className="optional">
            Optional
          </p>
          <p>
            Druid begins ingesting data once you submit a JSON ingestion spec.
            If you modify any values in this view, the values entered in previous sections will update accordingly.
            If you modify any values in previous sections, this spec will automatically update.
          </p>
          <p>
            Submit the spec to begin loading data into Druid.
          </p>
        </Callout>
      </div>
      <div className="next-bar">
        <Button
          text="Submit"
          intent={Intent.PRIMARY}
          onClick={async () => {
            if (['index', 'index_parallel'].includes(deepGet(spec, 'type'))) {
              let taskResp: any;
              try {
                taskResp = await axios.post('/druid/indexer/v1/task', {
                  type: spec.type,
                  spec
                });
              } catch (e) {
                AppToaster.show({
                  message: `Failed to submit task: ${getDruidErrorMessage(e)}`,
                  intent: Intent.DANGER
                });
                return;
              }

              AppToaster.show({
                message: 'Task submitted successfully. Going to task view...',
                intent: Intent.SUCCESS
              });

              setTimeout(() => {
                goToTask(taskResp.data.task);
              }, 1000);

            } else {
              try {
                await axios.post('/druid/indexer/v1/supervisor', spec);
              } catch (e) {
                AppToaster.show({
                  message: `Failed to submit supervisor: ${getDruidErrorMessage(e)}`,
                  intent: Intent.DANGER
                });
                return;
              }

              AppToaster.show({
                message: 'Supervisor submitted successfully. Going to task view...',
                intent: Intent.SUCCESS
              });

              setTimeout(() => {
                goToTask(null);
              }, 1000);

            }
          }}
        />
      </div>
    </>;
  }

}
