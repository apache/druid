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
  ButtonGroup,
  Callout,
  Card,
  Classes,
  Code,
  FormGroup,
  H5,
  HTMLSelect,
  Icon,
  IconName,
  Intent,
  Popover,
  Switch,
  TextArea,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import axios from 'axios';
import classNames from 'classnames';
import memoize from 'memoize-one';
import React from 'react';

import {
  AutoForm,
  CenterMessage,
  ClearableInput,
  ExternalLink,
  JsonInput,
  Loader,
} from '../../components';
import { AsyncActionDialog } from '../../dialogs';
import { AppToaster } from '../../singletons/toaster';
import { UrlBaser } from '../../singletons/url-baser';
import {
  filterMap,
  getDruidErrorMessage,
  localStorageGet,
  LocalStorageKeys,
  localStorageSet,
  parseJson,
  QueryState,
} from '../../utils';
import { NUMERIC_TIME_FORMATS, possibleDruidFormatForValues } from '../../utils/druid-time';
import { updateSchemaWithSample } from '../../utils/druid-type';
import {
  changeParallel,
  DimensionMode,
  DimensionSpec,
  DimensionsSpec,
  DruidFilter,
  EMPTY_ARRAY,
  EMPTY_OBJECT,
  fillDataSourceNameIfNeeded,
  fillParser,
  FlattenField,
  getDimensionMode,
  getDimensionSpecFormFields,
  getEmptyTimestampSpec,
  getFilterFormFields,
  getFlattenFieldFormFields,
  getIngestionComboType,
  getIngestionImage,
  getIngestionTitle,
  getIoConfigFormFields,
  getIoConfigTuningFormFields,
  getMetricSpecFormFields,
  getParseSpecFormFields,
  getPartitionRelatedTuningSpecFormFields,
  getRequiredModule,
  getRollup,
  getSpecType,
  getTimestampSpecFormFields,
  getTransformFormFields,
  getTuningSpecFormFields,
  GranularitySpec,
  hasParallelAbility,
  IngestionComboTypeWithExtra,
  IngestionSpec,
  invalidIoConfig,
  invalidTuningConfig,
  IoConfig,
  isColumnTimestampSpec,
  isEmptyIngestionSpec,
  isIngestSegment,
  isParallel,
  issueWithIoConfig,
  issueWithParser,
  isTask,
  joinFilter,
  MAX_INLINE_DATA_LENGTH,
  MetricSpec,
  normalizeSpec,
  Parser,
  ParseSpec,
  parseSpecHasFlatten,
  splitFilter,
  TimestampSpec,
  Transform,
  TuningConfig,
  updateIngestionType,
} from '../../utils/ingestion-spec';
import { deepDelete, deepGet, deepSet } from '../../utils/object-change';
import {
  ExampleManifest,
  getOverlordModules,
  HeaderAndRows,
  headerAndRowsFromSampleResponse,
  SampleEntry,
  sampleForConnect,
  sampleForExampleManifests,
  sampleForFilter,
  sampleForParser,
  sampleForSchema,
  sampleForTimestamp,
  sampleForTransform,
  SampleResponse,
  SampleResponseWithExtraInfo,
  SampleStrategy,
} from '../../utils/sampler';
import { computeFlattenPathsForData } from '../../utils/spec-utils';

import { ExamplePicker } from './example-picker/example-picker';
import { FilterTable } from './filter-table/filter-table';
import { ParseDataTable } from './parse-data-table/parse-data-table';
import { ParseTimeTable } from './parse-time-table/parse-time-table';
import { SchemaTable } from './schema-table/schema-table';
import { TransformTable } from './transform-table/transform-table';

import './load-data-view.scss';

function showRawLine(line: SampleEntry): string {
  const raw = line.raw;
  if (raw.includes('\n')) {
    return `[Multi-line row, length: ${raw.length}]`;
  }
  if (raw.length > 1000) {
    return raw.substr(0, 1000) + '...';
  }
  return raw;
}

function showBlankLine(line: SampleEntry): string {
  return line.parsed ? `[Row: ${JSON.stringify(line.parsed)}]` : '[Binary data]';
}

function getTimestampSpec(headerAndRows: HeaderAndRows | null): TimestampSpec {
  if (!headerAndRows) return getEmptyTimestampSpec();

  const timestampSpecs = filterMap(headerAndRows.header, sampleHeader => {
    const possibleFormat = possibleDruidFormatForValues(
      filterMap(headerAndRows.rows, d => (d.parsed ? d.parsed[sampleHeader] : undefined)),
    );
    if (!possibleFormat) return;
    return {
      column: sampleHeader,
      format: possibleFormat,
    };
  });

  return (
    timestampSpecs.find(ts => /time/i.test(ts.column)) || // Use a suggestion that has time in the name if possible
    timestampSpecs.find(ts => !NUMERIC_TIME_FORMATS.includes(ts.format)) || // Use a suggestion that is not numeric
    timestampSpecs[0] || // Fall back to the first one
    getEmptyTimestampSpec() // Ok, empty it is...
  );
}

type Step =
  | 'welcome'
  | 'connect'
  | 'parser'
  | 'timestamp'
  | 'transform'
  | 'filter'
  | 'schema'
  | 'partition'
  | 'tuning'
  | 'publish'
  | 'spec'
  | 'loading';

const STEPS: Step[] = [
  'welcome',
  'connect',
  'parser',
  'timestamp',
  'transform',
  'filter',
  'schema',
  'partition',
  'tuning',
  'publish',
  'spec',
  'loading',
];

const SECTIONS: { name: string; steps: Step[] }[] = [
  { name: 'Connect and parse raw data', steps: ['welcome', 'connect', 'parser', 'timestamp'] },
  { name: 'Transform and configure schema', steps: ['transform', 'filter', 'schema'] },
  { name: 'Tune parameters', steps: ['partition', 'tuning', 'publish'] },
  { name: 'Verify and submit', steps: ['spec'] },
];

const VIEW_TITLE: Record<Step, string> = {
  welcome: 'Start',
  connect: 'Connect',
  parser: 'Parse data',
  timestamp: 'Parse time',
  transform: 'Transform',
  filter: 'Filter',
  schema: 'Configure schema',
  partition: 'Partition',
  tuning: 'Tune',
  publish: 'Publish',
  spec: 'Edit spec',
  loading: 'Loading',
};

export interface LoadDataViewProps {
  initSupervisorId?: string;
  initTaskId?: string;
  exampleManifestsUrl?: string;
  goToTask: (taskId: string | undefined, supervisor?: string) => void;
}

export interface LoadDataViewState {
  step: Step;
  spec: IngestionSpec;
  cacheKey?: string;
  // dialogs / modals
  continueToSpec: boolean;
  showResetConfirm: boolean;
  newRollup?: boolean;
  newDimensionMode?: DimensionMode;

  // welcome
  overlordModules?: string[];
  selectedComboType?: IngestionComboTypeWithExtra;
  exampleManifests?: ExampleManifest[];

  // general
  sampleStrategy: SampleStrategy;
  columnFilter: string;
  specialColumnsOnly: boolean;

  // for ioConfig
  inputQueryState: QueryState<SampleResponseWithExtraInfo>;

  // for parser
  parserQueryState: QueryState<HeaderAndRows>;

  // for flatten
  selectedFlattenFieldIndex: number;
  selectedFlattenField?: FlattenField;

  // for timestamp
  timestampQueryState: QueryState<{
    headerAndRows: HeaderAndRows;
    timestampSpec: TimestampSpec;
  }>;

  // for transform
  transformQueryState: QueryState<HeaderAndRows>;
  selectedTransformIndex: number;
  selectedTransform?: Transform;

  // for filter
  filterQueryState: QueryState<HeaderAndRows>;
  selectedFilterIndex: number;
  selectedFilter?: DruidFilter;
  showGlobalFilter: boolean;

  // for schema
  schemaQueryState: QueryState<{
    headerAndRows: HeaderAndRows;
    dimensionsSpec: DimensionsSpec;
    metricsSpec: MetricSpec[];
  }>;
  selectedDimensionSpecIndex: number;
  selectedDimensionSpec?: DimensionSpec;
  selectedMetricSpecIndex: number;
  selectedMetricSpec?: MetricSpec;

  // for final step
  submitting: boolean;
}

export class LoadDataView extends React.PureComponent<LoadDataViewProps, LoadDataViewState> {
  constructor(props: LoadDataViewProps) {
    super(props);

    let spec = parseJson(String(localStorageGet(LocalStorageKeys.INGESTION_SPEC)));
    if (!spec || typeof spec !== 'object') spec = {};
    this.state = {
      step: 'welcome',
      spec,

      // dialogs / modals
      showResetConfirm: false,
      continueToSpec: false,

      // general
      sampleStrategy: 'start',
      columnFilter: '',
      specialColumnsOnly: false,

      // for firehose
      inputQueryState: QueryState.INIT,

      // for parser
      parserQueryState: QueryState.INIT,

      // for flatten
      selectedFlattenFieldIndex: -1,

      // for timestamp
      timestampQueryState: QueryState.INIT,

      // for transform
      transformQueryState: QueryState.INIT,
      selectedTransformIndex: -1,

      // for filter
      filterQueryState: QueryState.INIT,
      selectedFilterIndex: -1,
      showGlobalFilter: false,

      // for dimensions
      schemaQueryState: QueryState.INIT,
      selectedDimensionSpecIndex: -1,
      selectedMetricSpecIndex: -1,

      // for final step
      submitting: false,
    };
  }

  componentDidMount(): void {
    const { initTaskId, initSupervisorId } = this.props;
    const { spec } = this.state;

    this.getOverlordModules();
    if (initTaskId) {
      this.updateStep('loading');
      this.getTaskJson();
    } else if (initSupervisorId) {
      this.updateStep('loading');
      this.getSupervisorJson();
    } else if (isEmptyIngestionSpec(spec)) {
      this.updateStep('welcome');
    } else {
      this.updateStep('connect');
    }

    if (isEmptyIngestionSpec(spec)) {
      this.setState({ continueToSpec: true });
    }
  }

  async getOverlordModules() {
    let overlordModules: string[];
    try {
      overlordModules = await getOverlordModules();
    } catch (e) {
      AppToaster.show({
        message: `Failed to get overlord modules: ${e.message}`,
        intent: Intent.DANGER,
      });
      this.setState({ overlordModules: [] });
      return;
    }

    this.setState({ overlordModules });
  }

  isStepEnabled(step: Step): boolean {
    const { spec } = this.state;
    const ioConfig: IoConfig = deepGet(spec, 'ioConfig') || EMPTY_OBJECT;
    const parser: Parser = deepGet(spec, 'dataSchema.parser') || EMPTY_OBJECT;

    switch (step) {
      case 'connect':
        return Boolean(spec.type);

      case 'parser':
      case 'timestamp':
      case 'transform':
      case 'filter':
      case 'schema':
      case 'partition':
      case 'tuning':
      case 'publish':
        return Boolean(spec.type && !issueWithIoConfig(ioConfig) && !issueWithParser(parser));

      default:
        return true;
    }
  }

  private updateStep = (newStep: Step) => {
    this.doQueryForStep(newStep);
    this.setState({ step: newStep });
  };

  doQueryForStep(step: Step): any {
    switch (step) {
      case 'welcome':
        return this.queryForWelcome();

      case 'connect':
        return this.queryForConnect(true);

      case 'parser':
        return this.queryForParser(true);

      case 'timestamp':
        return this.queryForTimestamp(true);

      case 'transform':
        return this.queryForTransform(true);

      case 'filter':
        return this.queryForFilter(true);

      case 'schema':
        return this.queryForSchema(true);
    }
  }

  private updateSpec = (newSpec: IngestionSpec) => {
    newSpec = normalizeSpec(newSpec);
    this.setState({ spec: newSpec });
    localStorageSet(LocalStorageKeys.INGESTION_SPEC, JSON.stringify(newSpec));
  };

  renderActionCard(icon: IconName, title: string, caption: string, onClick: () => void) {
    return (
      <Card className={'spec-card'} interactive onClick={onClick}>
        <Icon className="spec-card-icon" icon={icon} iconSize={30} />
        <div className={'spec-card-header'}>
          {title}
          <div className={'spec-card-caption'}>{caption}</div>
        </div>
      </Card>
    );
  }

  render(): JSX.Element {
    const { step, continueToSpec } = this.state;

    if (!continueToSpec) {
      return (
        <div className={classNames('load-data-continue-view load-data-view')}>
          {this.renderActionCard(
            IconNames.ASTERISK,
            'Start a new spec',
            'Begin a new ingestion flow',
            this.handleResetSpec,
          )}
          {this.renderActionCard(
            IconNames.REPEAT,
            'Continue from previous spec',
            'Go back to the most recent spec you were working on',
            this.handleContinueSpec,
          )}
        </div>
      );
    }

    return (
      <div className={classNames('load-data-view', 'app-view', step)}>
        {this.renderStepNav()}
        {step === 'welcome' && this.renderWelcomeStep()}
        {step === 'connect' && this.renderConnectStep()}
        {step === 'parser' && this.renderParserStep()}
        {step === 'timestamp' && this.renderTimestampStep()}

        {step === 'transform' && this.renderTransformStep()}
        {step === 'filter' && this.renderFilterStep()}
        {step === 'schema' && this.renderSchemaStep()}

        {step === 'partition' && this.renderPartitionStep()}
        {step === 'tuning' && this.renderTuningStep()}
        {step === 'publish' && this.renderPublishStep()}

        {step === 'spec' && this.renderSpecStep()}
        {step === 'loading' && this.renderLoading()}

        {this.renderResetConfirm()}
      </div>
    );
  }

  renderStepNav() {
    const { step } = this.state;

    return (
      <div className={classNames(Classes.TABS, 'step-nav')}>
        {SECTIONS.map(section => (
          <div className="step-section" key={section.name}>
            <div className="step-nav-l1">{section.name}</div>
            <ButtonGroup className="step-nav-l2">
              {section.steps.map(s => (
                <Button
                  className={s}
                  key={s}
                  active={s === step}
                  onClick={() => this.updateStep(s)}
                  icon={s === 'spec' && IconNames.MANUALLY_ENTERED_DATA}
                  text={VIEW_TITLE[s]}
                  disabled={!this.isStepEnabled(s)}
                />
              ))}
            </ButtonGroup>
          </div>
        ))}
      </div>
    );
  }

  renderNextBar(options: {
    nextStep?: Step;
    disabled?: boolean;
    onNextStep?: () => void;
    onPrevStep?: () => void;
    prevLabel?: string;
  }) {
    const { disabled, onNextStep, onPrevStep, prevLabel } = options;
    const { step } = this.state;
    const nextStep = options.nextStep || STEPS[STEPS.indexOf(step) + 1] || STEPS[0];

    return (
      <div className="next-bar">
        {onPrevStep && (
          <Button className="prev" icon={IconNames.UNDO} text={prevLabel} onClick={onPrevStep} />
        )}
        <Button
          text={`Next: ${VIEW_TITLE[nextStep]}`}
          rightIcon={IconNames.ARROW_RIGHT}
          intent={Intent.PRIMARY}
          disabled={disabled}
          onClick={() => {
            if (disabled) return;
            if (onNextStep) onNextStep();

            setTimeout(() => {
              this.updateStep(nextStep);
            }, 10);
          }}
        />
      </div>
    );
  }

  // ==================================================================

  async queryForWelcome() {
    const { exampleManifestsUrl } = this.props;
    if (!exampleManifestsUrl) return;

    let exampleManifests: ExampleManifest[] | undefined;
    try {
      exampleManifests = await sampleForExampleManifests(exampleManifestsUrl);
    } catch (e) {
      this.setState({
        exampleManifests: undefined,
      });
      return;
    }

    this.setState({
      exampleManifests,
    });
  }

  renderIngestionCard(
    comboType: IngestionComboTypeWithExtra,
    disabled?: boolean,
  ): JSX.Element | undefined {
    const { overlordModules, selectedComboType } = this.state;
    if (!overlordModules) return;
    const requiredModule = getRequiredModule(comboType);
    const goodToGo = !disabled && (!requiredModule || overlordModules.includes(requiredModule));

    return (
      <Card
        className={classNames({ disabled: !goodToGo, active: selectedComboType === comboType })}
        interactive
        onClick={() => {
          this.setState({
            selectedComboType: selectedComboType !== comboType ? comboType : undefined,
          });
        }}
      >
        <img src={UrlBaser.base(`/assets/${getIngestionImage(comboType)}.png`)} />
        <p>{getIngestionTitle(comboType)}</p>
      </Card>
    );
  }

  renderWelcomeStep() {
    const { exampleManifestsUrl } = this.props;
    const { spec, exampleManifests } = this.state;
    const noExamples = Boolean(!exampleManifests || !exampleManifests.length);

    const welcomeMessage = this.renderWelcomeStepMessage();
    return (
      <>
        <div className="main bp3-input">
          {this.renderIngestionCard('kafka')}
          {this.renderIngestionCard('kinesis')}
          {this.renderIngestionCard('index:static-s3')}
          {this.renderIngestionCard('index:static-google-blobstore')}
          {this.renderIngestionCard('hadoop')}
          {this.renderIngestionCard('index:ingestSegment')}
          {this.renderIngestionCard('index:http')}
          {this.renderIngestionCard('index:local')}
          {this.renderIngestionCard('index:inline')}
          {exampleManifestsUrl && this.renderIngestionCard('example', noExamples)}
          {this.renderIngestionCard('other')}
        </div>
        <div className="control">
          {welcomeMessage && <Callout className="intro">{welcomeMessage}</Callout>}
          {this.renderWelcomeStepControls()}
          {!isEmptyIngestionSpec(spec) && (
            <Button icon={IconNames.RESET} text="Reset spec" onClick={this.handleResetConfirm} />
          )}
        </div>
      </>
    );
  }

  renderWelcomeStepMessage(): JSX.Element | undefined {
    const { selectedComboType, exampleManifests } = this.state;

    if (!selectedComboType) {
      return <p>Please specify where your raw data is located</p>;
    }

    const issue = this.selectedIngestionTypeIssue();
    if (issue) return issue;

    switch (selectedComboType) {
      case 'index:http':
        return (
          <>
            <p>Load data accessible through HTTP(s).</p>
            <p>
              Data must be in a text format and the HTTP(s) endpoint must be reachable by every
              Druid process in the cluster.
            </p>
          </>
        );

      case 'index:local':
        return (
          <>
            <p>
              <em>Recommended only in single server deployments.</em>
            </p>
            <p>Load data directly from a local file.</p>
            <p>
              Files must be in a text format and must be accessible to all the Druid processes in
              the cluster.
            </p>
          </>
        );

      case 'index:ingestSegment':
        return (
          <>
            <p>Reindex data from existing Druid segments.</p>
            <p>
              Reindexing data allows you to filter rows, add, transform, and delete columns, as well
              as change the partitioning of the data.
            </p>
          </>
        );

      case 'index:inline':
        return (
          <>
            <p>Ingest a small amount of data directly from the clipboard.</p>
          </>
        );

      case 'index:static-s3':
        return <p>Load text based data from Amazon S3.</p>;

      case 'index:static-google-blobstore':
        return <p>Load text based data from the Google Blobstore.</p>;

      case 'kafka':
        return <p>Load streaming data in real-time from Apache Kafka.</p>;

      case 'kinesis':
        return <p>Load streaming data in real-time from Amazon Kinesis.</p>;

      case 'hadoop':
        return (
          <>
            <p>
              <em>Data loader support coming soon!</em>
            </p>
            <p>
              You can not ingest data from HDFS via the data loader at this time, however you can
              ingest it through a Druid task.
            </p>
            <p>
              Please follow{' '}
              <ExternalLink href="https://druid.apache.org/docs/latest/ingestion/hadoop.html">
                the hadoop docs
              </ExternalLink>{' '}
              and submit a JSON spec to start the task.
            </p>
          </>
        );

      case 'example':
        if (exampleManifests && exampleManifests.length) {
          return; // Yield to example picker controls
        } else {
          return <p>Could not load examples.</p>;
        }

      case 'other':
        return (
          <p>
            If you do not see your source of raw data here, you can try to ingest it by submitting a{' '}
            <ExternalLink href="https://druid.apache.org/docs/latest/ingestion/index.html">
              JSON task or supervisor spec
            </ExternalLink>
            .
          </p>
        );

      default:
        return <p>Unknown ingestion type.</p>;
    }
  }

  renderWelcomeStepControls(): JSX.Element | undefined {
    const { goToTask } = this.props;
    const { spec, selectedComboType, exampleManifests } = this.state;

    const issue = this.selectedIngestionTypeIssue();
    if (issue) return;

    switch (selectedComboType) {
      case 'index:http':
      case 'index:local':
      case 'index:ingestSegment':
      case 'index:inline':
      case 'index:static-s3':
      case 'index:static-google-blobstore':
      case 'kafka':
      case 'kinesis':
        return (
          <FormGroup>
            <Button
              text="Connect data"
              rightIcon={IconNames.ARROW_RIGHT}
              intent={Intent.PRIMARY}
              onClick={() => {
                this.updateSpec(updateIngestionType(spec, selectedComboType as any));
                setTimeout(() => {
                  this.updateStep('connect');
                }, 10);
              }}
            />
          </FormGroup>
        );

      case 'hadoop':
        return (
          <FormGroup>
            <Button
              text="Submit task"
              rightIcon={IconNames.ARROW_RIGHT}
              intent={Intent.PRIMARY}
              onClick={() => goToTask(undefined, 'task')}
            />
          </FormGroup>
        );

      case 'example':
        if (!exampleManifests) return;
        return (
          <ExamplePicker
            exampleManifests={exampleManifests}
            onSelectExample={exampleManifest => {
              this.updateSpec(exampleManifest.spec);
              setTimeout(() => {
                this.updateStep('connect');
              }, 10);
            }}
          />
        );

      case 'other':
        return (
          <>
            <FormGroup>
              <Button
                text="Submit supervisor"
                rightIcon={IconNames.ARROW_RIGHT}
                intent={Intent.PRIMARY}
                onClick={() => goToTask(undefined, 'supervisor')}
              />
            </FormGroup>
            <FormGroup>
              <Button
                text="Submit task"
                rightIcon={IconNames.ARROW_RIGHT}
                intent={Intent.PRIMARY}
                onClick={() => goToTask(undefined, 'task')}
              />
            </FormGroup>
          </>
        );

      default:
        return;
    }
  }

  selectedIngestionTypeIssue(): JSX.Element | undefined {
    const { selectedComboType, overlordModules } = this.state;
    if (!selectedComboType || !overlordModules) return;

    const requiredModule = getRequiredModule(selectedComboType);
    if (!requiredModule || overlordModules.includes(requiredModule)) return;

    return (
      <>
        <p>
          {`${getIngestionTitle(selectedComboType)} ingestion requires the `}
          <strong>{requiredModule}</strong>
          {` extension to be loaded.`}
        </p>
        <p>
          Please make sure that the
          <Code>"{requiredModule}"</Code> extension is included in the <Code>loadList</Code>.
        </p>
        <p>
          For more information please refer to the{' '}
          <ExternalLink href="https://druid.apache.org/docs/latest/operations/including-extensions">
            documentation on loading extensions
          </ExternalLink>
          .
        </p>
      </>
    );
  }

  private handleResetConfirm = () => {
    this.setState({ showResetConfirm: true });
  };

  private handleResetSpec = () => {
    this.setState({ showResetConfirm: false, continueToSpec: true });
    this.updateSpec({} as any);
    this.updateStep('welcome');
  };

  private handleContinueSpec = () => {
    this.setState({ continueToSpec: true });
  };

  renderResetConfirm(): JSX.Element | undefined {
    const { showResetConfirm } = this.state;
    if (!showResetConfirm) return;

    return (
      <Alert
        cancelButtonText="Cancel"
        confirmButtonText="Reset spec"
        icon="trash"
        intent={Intent.DANGER}
        isOpen
        onCancel={() => this.setState({ showResetConfirm: false })}
        onConfirm={this.handleResetSpec}
      >
        <p>This will discard the current progress in the spec.</p>
      </Alert>
    );
  }

  // ==================================================================

  async queryForConnect(initRun = false) {
    const { spec, sampleStrategy } = this.state;
    const ioConfig: IoConfig = deepGet(spec, 'ioConfig') || EMPTY_OBJECT;

    let issue: string | undefined;
    if (issueWithIoConfig(ioConfig)) {
      issue = `IoConfig not ready, ${issueWithIoConfig(ioConfig)}`;
    }

    if (issue) {
      this.setState({
        inputQueryState: initRun ? QueryState.INIT : new QueryState({ error: issue }),
      });
      return;
    }

    this.setState({
      inputQueryState: new QueryState({ loading: true }),
    });

    let sampleResponse: SampleResponse;
    try {
      sampleResponse = await sampleForConnect(spec, sampleStrategy);
    } catch (e) {
      this.setState({
        inputQueryState: new QueryState({ error: e.message }),
      });
      return;
    }

    this.setState({
      cacheKey: sampleResponse.cacheKey,
      inputQueryState: new QueryState({ data: sampleResponse }),
    });
  }

  renderConnectStep() {
    const { spec, inputQueryState, sampleStrategy } = this.state;
    const specType = getSpecType(spec);
    const ioConfig: IoConfig = deepGet(spec, 'ioConfig') || EMPTY_OBJECT;
    const isBlank = !ioConfig.type;
    const inlineMode = deepGet(spec, 'ioConfig.firehose.type') === 'inline';

    let mainFill: JSX.Element | string = '';
    if (inlineMode) {
      mainFill = (
        <TextArea
          className="inline-data"
          placeholder="Paste your data here"
          value={deepGet(spec, 'ioConfig.firehose.data')}
          onChange={(e: any) => {
            const stringValue = e.target.value.substr(0, MAX_INLINE_DATA_LENGTH);
            this.updateSpec(deepSet(spec, 'ioConfig.firehose.data', stringValue));
          }}
        />
      );
    } else if (inputQueryState.isInit()) {
      mainFill = (
        <CenterMessage>
          Please fill out the fields on the right sidebar to get started{' '}
          <Icon icon={IconNames.ARROW_RIGHT} />
        </CenterMessage>
      );
    } else if (inputQueryState.isLoading()) {
      mainFill = <Loader loading />;
    } else if (inputQueryState.error) {
      mainFill = <CenterMessage>{`Error: ${inputQueryState.error}`}</CenterMessage>;
    } else if (inputQueryState.data) {
      const inputData = inputQueryState.data.data;
      mainFill = (
        <TextArea
          className="raw-lines"
          value={
            inputData.length
              ? (inputData.every(l => !l.raw)
                  ? inputData.map(showBlankLine)
                  : inputData.map(showRawLine)
                ).join('\n')
              : 'No data returned from sampler'
          }
          readOnly
        />
      );
    }

    const ingestionComboType = getIngestionComboType(spec);
    return (
      <>
        <div className="main">{mainFill}</div>
        <div className="control">
          <Callout className="intro">
            <p>
              Druid ingests raw data and converts it into a custom,{' '}
              <ExternalLink href="https://druid.apache.org/docs/latest/design/segments.html">
                indexed
              </ExternalLink>{' '}
              format that is optimized for analytic queries.
            </p>
            {inlineMode ? (
              <>
                <p>To get started, please paste some data in the box to the left.</p>
                <p>Click "Register" to verify your data with Druid.</p>
              </>
            ) : (
              <>
                <p>
                  To get started, please specify where your raw data is stored and what data you
                  want to ingest.
                </p>
                <p>Click "Preview" to look at the sampled raw data.</p>
              </>
            )}
          </Callout>
          {ingestionComboType ? (
            <AutoForm
              fields={getIoConfigFormFields(ingestionComboType)}
              model={ioConfig}
              onChange={c => this.updateSpec(deepSet(spec, 'ioConfig', c))}
            />
          ) : (
            <FormGroup label="IO Config">
              <JsonInput
                value={ioConfig}
                onChange={c => this.updateSpec(deepSet(spec, 'ioConfig', c))}
                height="300px"
              />
            </FormGroup>
          )}
          {deepGet(spec, 'ioConfig.firehose.type') === 'local' && (
            <FormGroup>
              <Callout intent={Intent.WARNING}>
                This path must be available on the local filesystem of all Druid servers.
              </Callout>
            </FormGroup>
          )}
          {(specType === 'kafka' || specType === 'kinesis') && (
            <FormGroup label="Where should the data be sampled from?">
              <HTMLSelect
                value={sampleStrategy}
                onChange={e => this.setState({ sampleStrategy: e.target.value as any })}
              >
                <option value="start">Start of stream</option>
                <option value="end">End of the stream</option>
              </HTMLSelect>
            </FormGroup>
          )}
          <Button
            text={inlineMode ? 'Register data' : 'Preview'}
            disabled={isBlank}
            intent={inputQueryState.data ? undefined : Intent.PRIMARY}
            onClick={() => this.queryForConnect()}
          />
        </div>
        {this.renderNextBar({
          disabled: !inputQueryState.data,
          onNextStep: () => {
            if (!inputQueryState.data) return;
            const inputData = inputQueryState.data;

            if (isIngestSegment(spec)) {
              let newSpec = fillParser(spec, []);

              if (typeof inputData.rollup === 'boolean') {
                newSpec = deepSet(newSpec, 'dataSchema.granularitySpec.rollup', inputData.rollup);
              }

              if (inputData.timestampSpec) {
                newSpec = deepSet(
                  newSpec,
                  'dataSchema.parser.parseSpec.timestampSpec',
                  inputData.timestampSpec,
                );
              }

              if (inputData.queryGranularity) {
                newSpec = deepSet(
                  newSpec,
                  'dataSchema.granularitySpec.queryGranularity',
                  inputData.queryGranularity,
                );
              }

              if (inputData.columns) {
                const aggregators = inputData.aggregators || {};
                newSpec = deepSet(
                  newSpec,
                  'dataSchema.parser.parseSpec.dimensionsSpec.dimensions',
                  Object.keys(inputData.columns)
                    .filter(k => k !== '__time' && !aggregators[k])
                    .map(k => ({
                      name: k,
                      type: String(inputData.columns![k].type || 'string').toLowerCase(),
                    })),
                );
              }

              if (inputData.aggregators) {
                newSpec = deepSet(
                  newSpec,
                  'dataSchema.metricsSpec',
                  Object.values(inputData.aggregators),
                );
              }

              this.updateSpec(fillDataSourceNameIfNeeded(newSpec));
            } else {
              this.updateSpec(
                fillDataSourceNameIfNeeded(
                  fillParser(spec, inputQueryState.data.data.map(l => l.raw)),
                ),
              );
            }
          },
        })}
      </>
    );
  }

  // ==================================================================

  async queryForParser(initRun = false) {
    const { spec, sampleStrategy, cacheKey } = this.state;
    const ioConfig: IoConfig = deepGet(spec, 'ioConfig') || EMPTY_OBJECT;
    const parser: Parser = deepGet(spec, 'dataSchema.parser') || EMPTY_OBJECT;
    const parserColumns: string[] = deepGet(parser, 'parseSpec.columns') || [];

    let issue: string | undefined;
    if (issueWithIoConfig(ioConfig)) {
      issue = `IoConfig not ready, ${issueWithIoConfig(ioConfig)}`;
    } else if (issueWithParser(parser)) {
      issue = `Parser not ready, ${issueWithParser(parser)}`;
    }

    if (issue) {
      this.setState({
        parserQueryState: initRun ? QueryState.INIT : new QueryState({ error: issue }),
      });
      return;
    }

    this.setState({
      parserQueryState: new QueryState({ loading: true }),
    });

    let sampleResponse: SampleResponse;
    try {
      sampleResponse = await sampleForParser(spec, sampleStrategy, cacheKey);
    } catch (e) {
      this.setState({
        parserQueryState: new QueryState({ error: e.message }),
      });
      return;
    }

    this.setState({
      cacheKey: sampleResponse.cacheKey,
      parserQueryState: new QueryState({
        data: headerAndRowsFromSampleResponse(sampleResponse, '__time', parserColumns),
      }),
    });
  }

  renderParserStep() {
    const {
      spec,
      columnFilter,
      specialColumnsOnly,
      parserQueryState,
      selectedFlattenField,
    } = this.state;
    const parseSpec: ParseSpec = deepGet(spec, 'dataSchema.parser.parseSpec') || EMPTY_OBJECT;
    const flattenFields: FlattenField[] =
      deepGet(spec, 'dataSchema.parser.parseSpec.flattenSpec.fields') || EMPTY_ARRAY;

    const isBlank = !parseSpec.format;
    const canFlatten = parseSpec.format === 'json';

    let mainFill: JSX.Element | string = '';
    if (parserQueryState.isInit()) {
      mainFill = (
        <CenterMessage>
          Please enter the parser details on the right <Icon icon={IconNames.ARROW_RIGHT} />
        </CenterMessage>
      );
    } else if (parserQueryState.isLoading()) {
      mainFill = <Loader loading />;
    } else if (parserQueryState.error) {
      mainFill = <CenterMessage>{`Error: ${parserQueryState.error}`}</CenterMessage>;
    } else if (parserQueryState.data) {
      mainFill = (
        <div className="table-with-control">
          <div className="table-control">
            <ClearableInput
              value={columnFilter}
              onChange={columnFilter => this.setState({ columnFilter })}
              placeholder="Search columns"
            />
            {canFlatten && (
              <Switch
                checked={specialColumnsOnly}
                label="Flattened columns only"
                onChange={() => this.setState({ specialColumnsOnly: !specialColumnsOnly })}
                disabled={!flattenFields.length}
              />
            )}
          </div>
          <ParseDataTable
            sampleData={parserQueryState.data}
            columnFilter={columnFilter}
            canFlatten={canFlatten}
            flattenedColumnsOnly={specialColumnsOnly}
            flattenFields={flattenFields}
            onFlattenFieldSelect={this.onFlattenFieldSelect}
          />
        </div>
      );
    }

    let sugestedFlattenFields: FlattenField[] | null = null;
    if (canFlatten && !flattenFields.length && parserQueryState.data) {
      sugestedFlattenFields = computeFlattenPathsForData(
        filterMap(parserQueryState.data.rows, r => parseJson(r.raw)),
        'path',
        'ignore-arrays',
      );
    }

    return (
      <>
        <div className="main">{mainFill}</div>
        <div className="control">
          <Callout className="intro">
            <p>
              Druid requires flat data (non-nested, non-hierarchical). Each row should represent a
              discrete event.
            </p>
            {canFlatten && (
              <p>
                If you have nested data, you can{' '}
                <ExternalLink href="https://druid.apache.org/docs/latest/ingestion/flatten-json.html">
                  flatten
                </ExternalLink>{' '}
                it here. If the provided flattening capabilities are not sufficient, please
                pre-process your data before ingesting it into Druid.
              </p>
            )}
            <p>
              Click "Preview" to ensure that your data appears correctly in a row/column
              orientation.
            </p>
          </Callout>
          <AutoForm
            fields={getParseSpecFormFields()}
            model={parseSpec}
            onChange={p => this.updateSpec(deepSet(spec, 'dataSchema.parser.parseSpec', p))}
          />
          {this.renderFlattenControls()}
          {Boolean(sugestedFlattenFields && sugestedFlattenFields.length) && (
            <FormGroup>
              <Button
                icon={IconNames.LIGHTBULB}
                text="Auto add flatten specs"
                onClick={() => {
                  this.updateSpec(
                    deepSet(
                      spec,
                      'dataSchema.parser.parseSpec.flattenSpec.fields',
                      sugestedFlattenFields,
                    ),
                  );
                  setTimeout(() => {
                    this.queryForParser();
                  }, 10);
                }}
              />
            </FormGroup>
          )}
          {!selectedFlattenField && (
            <Button text="Preview" disabled={isBlank} onClick={() => this.queryForParser()} />
          )}
        </div>
        {this.renderNextBar({
          disabled: !parserQueryState.data,
          onNextStep: () => {
            if (!parserQueryState.data) return;
            let possibleTimestampSpec: TimestampSpec;
            if (isIngestSegment(spec)) {
              possibleTimestampSpec = {
                column: '__time',
                format: 'auto',
              };
            } else {
              possibleTimestampSpec = getTimestampSpec(parserQueryState.data);
            }

            if (possibleTimestampSpec) {
              const newSpec: IngestionSpec = deepSet(
                spec,
                'dataSchema.parser.parseSpec.timestampSpec',
                possibleTimestampSpec,
              );
              this.updateSpec(newSpec);
            }
          },
        })}
      </>
    );
  }

  private onFlattenFieldSelect = (field: FlattenField, index: number) => {
    this.setState({
      selectedFlattenFieldIndex: index,
      selectedFlattenField: field,
    });
  };

  renderFlattenControls(): JSX.Element | undefined {
    const { spec, selectedFlattenField, selectedFlattenFieldIndex } = this.state;
    const parseSpec: ParseSpec = deepGet(spec, 'dataSchema.parser.parseSpec') || EMPTY_OBJECT;
    if (!parseSpecHasFlatten(parseSpec)) return;

    const close = () => {
      this.setState({
        selectedFlattenFieldIndex: -1,
        selectedFlattenField: undefined,
      });
    };

    const closeAndQuery = () => {
      close();
      setTimeout(() => {
        this.queryForParser();
      }, 10);
    };

    if (selectedFlattenField) {
      return (
        <div className="edit-controls">
          <AutoForm
            fields={getFlattenFieldFormFields()}
            model={selectedFlattenField}
            onChange={f => this.setState({ selectedFlattenField: f })}
          />
          <div className="controls-buttons">
            <Button
              className="add-update"
              text={selectedFlattenFieldIndex === -1 ? 'Add' : 'Update'}
              intent={Intent.PRIMARY}
              onClick={() => {
                this.updateSpec(
                  deepSet(
                    spec,
                    `dataSchema.parser.parseSpec.flattenSpec.fields.${selectedFlattenFieldIndex}`,
                    selectedFlattenField,
                  ),
                );
                closeAndQuery();
              }}
            />
            {selectedFlattenFieldIndex !== -1 && (
              <Button
                icon={IconNames.TRASH}
                intent={Intent.DANGER}
                onClick={() => {
                  this.updateSpec(
                    deepDelete(
                      spec,
                      `dataSchema.parser.parseSpec.flattenSpec.fields.${selectedFlattenFieldIndex}`,
                    ),
                  );
                  closeAndQuery();
                }}
              />
            )}
            <Button className="cancel" text="Cancel" onClick={close} />
          </div>
        </div>
      );
    } else {
      return (
        <FormGroup>
          <Button
            text="Add column flattening"
            onClick={() => {
              this.setState({
                selectedFlattenField: { type: 'path', name: '', expr: '' },
                selectedFlattenFieldIndex: -1,
              });
            }}
          />
          <AnchorButton
            icon={IconNames.INFO_SIGN}
            href="https://druid.apache.org/docs/latest/ingestion/flatten-json.html"
            target="_blank"
            minimal
          />
        </FormGroup>
      );
    }
  }

  // ==================================================================

  async queryForTimestamp(initRun = false) {
    const { spec, sampleStrategy, cacheKey } = this.state;
    const ioConfig: IoConfig = deepGet(spec, 'ioConfig') || EMPTY_OBJECT;
    const parser: Parser = deepGet(spec, 'dataSchema.parser') || EMPTY_OBJECT;
    const parserColumns: string[] = deepGet(parser, 'parseSpec.columns') || [];
    const timestampSpec =
      deepGet(spec, 'dataSchema.parser.parseSpec.timestampSpec') || EMPTY_OBJECT;

    let issue: string | undefined;
    if (issueWithIoConfig(ioConfig)) {
      issue = `IoConfig not ready, ${issueWithIoConfig(ioConfig)}`;
    } else if (issueWithParser(parser)) {
      issue = `Parser not ready, ${issueWithParser(parser)}`;
    }

    if (issue) {
      this.setState({
        timestampQueryState: initRun ? QueryState.INIT : new QueryState({ error: issue }),
      });
      return;
    }

    this.setState({
      timestampQueryState: new QueryState({ loading: true }),
    });

    let sampleResponse: SampleResponse;
    try {
      sampleResponse = await sampleForTimestamp(spec, sampleStrategy, cacheKey);
    } catch (e) {
      this.setState({
        timestampQueryState: new QueryState({ error: e.message }),
      });
      return;
    }

    this.setState({
      cacheKey: sampleResponse.cacheKey,
      timestampQueryState: new QueryState({
        data: {
          headerAndRows: headerAndRowsFromSampleResponse(
            sampleResponse,
            undefined,
            ['__time'].concat(parserColumns),
          ),
          timestampSpec,
        },
      }),
    });
  }

  renderTimestampStep() {
    const { spec, columnFilter, specialColumnsOnly, timestampQueryState } = this.state;
    const parseSpec: ParseSpec = deepGet(spec, 'dataSchema.parser.parseSpec') || EMPTY_OBJECT;
    const timestampSpec: TimestampSpec =
      deepGet(spec, 'dataSchema.parser.parseSpec.timestampSpec') || EMPTY_OBJECT;
    const timestampSpecFromColumn = isColumnTimestampSpec(timestampSpec);

    const isBlank = !parseSpec.format;

    let mainFill: JSX.Element | string = '';
    if (timestampQueryState.isInit()) {
      mainFill = (
        <CenterMessage>
          Please enter the timestamp column details on the right{' '}
          <Icon icon={IconNames.ARROW_RIGHT} />
        </CenterMessage>
      );
    } else if (timestampQueryState.isLoading()) {
      mainFill = <Loader loading />;
    } else if (timestampQueryState.error) {
      mainFill = <CenterMessage>{`Error: ${timestampQueryState.error}`}</CenterMessage>;
    } else if (timestampQueryState.data) {
      mainFill = (
        <div className="table-with-control">
          <div className="table-control">
            <ClearableInput
              value={columnFilter}
              onChange={columnFilter => this.setState({ columnFilter })}
              placeholder="Search columns"
            />
            <Switch
              checked={specialColumnsOnly}
              label="Suggested columns only"
              onChange={() => this.setState({ specialColumnsOnly: !specialColumnsOnly })}
            />
          </div>
          <ParseTimeTable
            sampleBundle={timestampQueryState.data}
            columnFilter={columnFilter}
            possibleTimestampColumnsOnly={specialColumnsOnly}
            onTimestampColumnSelect={this.onTimestampColumnSelect}
          />
        </div>
      );
    }

    return (
      <>
        <div className="main">{mainFill}</div>
        <div className="control">
          <Callout className="intro">
            <p>
              Druid partitions data based on the primary time column of your data. This column is
              stored internally in Druid as <Code>__time</Code>. Please specify the primary time
              column. If you do not have any time columns, you can choose "Constant Value" to create
              a default one.
            </p>
            <p>Click "Preview" to check if Druid can properly parse your time values.</p>
          </Callout>
          <FormGroup label="Timestamp spec">
            <ButtonGroup>
              <Button
                text="From column"
                active={timestampSpecFromColumn}
                onClick={() => {
                  const timestampSpec = {
                    column: 'timestamp',
                    format: 'auto',
                  };
                  this.updateSpec(
                    deepSet(spec, 'dataSchema.parser.parseSpec.timestampSpec', timestampSpec),
                  );
                  setTimeout(() => {
                    this.queryForTimestamp();
                  }, 10);
                }}
              />
              <Button
                text="Constant value"
                active={!timestampSpecFromColumn}
                onClick={() => {
                  this.updateSpec(
                    deepSet(
                      spec,
                      'dataSchema.parser.parseSpec.timestampSpec',
                      getEmptyTimestampSpec(),
                    ),
                  );
                  setTimeout(() => {
                    this.queryForTimestamp();
                  }, 10);
                }}
              />
            </ButtonGroup>
          </FormGroup>
          <AutoForm
            fields={getTimestampSpecFormFields(timestampSpec)}
            model={timestampSpec}
            onChange={timestampSpec => {
              this.updateSpec(
                deepSet(spec, 'dataSchema.parser.parseSpec.timestampSpec', timestampSpec),
              );
            }}
          />
          <Button text="Preview" disabled={isBlank} onClick={() => this.queryForTimestamp()} />
        </div>
        {this.renderNextBar({
          disabled: !timestampQueryState.data,
        })}
      </>
    );
  }

  private onTimestampColumnSelect = (newTimestampSpec: TimestampSpec) => {
    const { spec } = this.state;
    this.updateSpec(deepSet(spec, 'dataSchema.parser.parseSpec.timestampSpec', newTimestampSpec));
  };

  // ==================================================================

  async queryForTransform(initRun = false) {
    const { spec, sampleStrategy, cacheKey } = this.state;
    const ioConfig: IoConfig = deepGet(spec, 'ioConfig') || EMPTY_OBJECT;
    const parser: Parser = deepGet(spec, 'dataSchema.parser') || EMPTY_OBJECT;
    const parserColumns: string[] = deepGet(parser, 'parseSpec.columns') || [];

    let issue: string | undefined;
    if (issueWithIoConfig(ioConfig)) {
      issue = `IoConfig not ready, ${issueWithIoConfig(ioConfig)}`;
    } else if (issueWithParser(parser)) {
      issue = `Parser not ready, ${issueWithParser(parser)}`;
    }

    if (issue) {
      this.setState({
        transformQueryState: initRun ? QueryState.INIT : new QueryState({ error: issue }),
      });
      return;
    }

    this.setState({
      transformQueryState: new QueryState({ loading: true }),
    });

    let sampleResponse: SampleResponse;
    try {
      sampleResponse = await sampleForTransform(spec, sampleStrategy, cacheKey);
    } catch (e) {
      this.setState({
        transformQueryState: new QueryState({ error: e.message }),
      });
      return;
    }

    this.setState({
      cacheKey: sampleResponse.cacheKey,
      transformQueryState: new QueryState({
        data: headerAndRowsFromSampleResponse(
          sampleResponse,
          undefined,
          ['__time'].concat(parserColumns),
        ),
      }),
    });
  }

  renderTransformStep() {
    const {
      spec,
      columnFilter,
      specialColumnsOnly,
      transformQueryState,
      selectedTransformIndex,
    } = this.state;
    const transforms: Transform[] =
      deepGet(spec, 'dataSchema.transformSpec.transforms') || EMPTY_ARRAY;

    let mainFill: JSX.Element | string = '';
    if (transformQueryState.isInit()) {
      mainFill = <CenterMessage>{`Please fill in the previous steps`}</CenterMessage>;
    } else if (transformQueryState.isLoading()) {
      mainFill = <Loader loading />;
    } else if (transformQueryState.error) {
      mainFill = <CenterMessage>{`Error: ${transformQueryState.error}`}</CenterMessage>;
    } else if (transformQueryState.data) {
      mainFill = (
        <div className="table-with-control">
          <div className="table-control">
            <ClearableInput
              value={columnFilter}
              onChange={columnFilter => this.setState({ columnFilter })}
              placeholder="Search columns"
            />
            <Switch
              checked={specialColumnsOnly}
              label="Transformed columns only"
              onChange={() => this.setState({ specialColumnsOnly: !specialColumnsOnly })}
              disabled={!transforms.length}
            />
          </div>
          <TransformTable
            sampleData={transformQueryState.data}
            columnFilter={columnFilter}
            transformedColumnsOnly={specialColumnsOnly}
            transforms={transforms}
            selectedTransformIndex={selectedTransformIndex}
            onTransformSelect={this.onTransformSelect}
          />
        </div>
      );
    }

    return (
      <>
        <div className="main">{mainFill}</div>
        <div className="control">
          <Callout className="intro">
            <p className="optional">Optional</p>
            <p>
              Druid can perform simple{' '}
              <ExternalLink href="https://druid.apache.org/docs/latest/ingestion/transform-spec.html#transforms">
                transforms
              </ExternalLink>{' '}
              of column values.
            </p>
            <p>Click "Preview" to see the result of any specified transforms.</p>
          </Callout>
          {Boolean(transformQueryState.error && transforms.length) && (
            <FormGroup>
              <Button
                icon={IconNames.EDIT}
                text="Edit last added transform"
                intent={Intent.PRIMARY}
                onClick={() => {
                  this.setState({
                    selectedTransformIndex: transforms.length - 1,
                    selectedTransform: transforms[transforms.length - 1],
                  });
                }}
              />
            </FormGroup>
          )}
          {this.renderTransformControls()}
          <Button text="Preview" onClick={() => this.queryForTransform()} />
        </div>
        {this.renderNextBar({
          disabled: !transformQueryState.data,
          onNextStep: () => {
            if (!transformQueryState.data) return;
            if (!isIngestSegment(spec)) {
              this.updateSpec(
                updateSchemaWithSample(spec, transformQueryState.data, 'specific', true),
              );
            }
          },
        })}
      </>
    );
  }

  private onTransformSelect = (transform: Transform, index: number) => {
    this.setState({
      selectedTransformIndex: index,
      selectedTransform: transform,
    });
  };

  renderTransformControls() {
    const { spec, selectedTransform, selectedTransformIndex } = this.state;

    const close = () => {
      this.setState({
        selectedTransformIndex: -1,
        selectedTransform: undefined,
      });
    };

    const closeAndQuery = () => {
      close();
      setTimeout(() => {
        this.queryForTransform();
      }, 10);
    };

    if (selectedTransform) {
      return (
        <div className="edit-controls">
          <AutoForm
            fields={getTransformFormFields()}
            model={selectedTransform}
            onChange={selectedTransform => this.setState({ selectedTransform })}
          />
          <div className="controls-buttons">
            <Button
              className="add-update"
              text={selectedTransformIndex === -1 ? 'Add' : 'Update'}
              intent={Intent.PRIMARY}
              onClick={() => {
                this.updateSpec(
                  deepSet(
                    spec,
                    `dataSchema.transformSpec.transforms.${selectedTransformIndex}`,
                    selectedTransform,
                  ),
                );
                closeAndQuery();
              }}
            />
            {selectedTransformIndex !== -1 && (
              <Button
                icon={IconNames.TRASH}
                intent={Intent.DANGER}
                onClick={() => {
                  this.updateSpec(
                    deepDelete(
                      spec,
                      `dataSchema.transformSpec.transforms.${selectedTransformIndex}`,
                    ),
                  );
                  closeAndQuery();
                }}
              />
            )}
            <Button className="cancel" text="Cancel" onClick={close} />
          </div>
        </div>
      );
    } else {
      return (
        <FormGroup>
          <Button
            text="Add column transform"
            onClick={() => {
              this.setState({
                selectedTransformIndex: -1,
                selectedTransform: { type: 'expression', name: '', expression: '' },
              });
            }}
          />
        </FormGroup>
      );
    }
  }

  // ==================================================================

  async queryForFilter(initRun = false) {
    const { spec, sampleStrategy, cacheKey } = this.state;
    const ioConfig: IoConfig = deepGet(spec, 'ioConfig') || EMPTY_OBJECT;
    const parser: Parser = deepGet(spec, 'dataSchema.parser') || EMPTY_OBJECT;
    const parserColumns: string[] = deepGet(parser, 'parseSpec.columns') || [];

    let issue: string | undefined;
    if (issueWithIoConfig(ioConfig)) {
      issue = `IoConfig not ready, ${issueWithIoConfig(ioConfig)}`;
    } else if (issueWithParser(parser)) {
      issue = `Parser not ready, ${issueWithParser(parser)}`;
    }

    if (issue) {
      this.setState({
        filterQueryState: initRun ? QueryState.INIT : new QueryState({ error: issue }),
      });
      return;
    }

    this.setState({
      filterQueryState: new QueryState({ loading: true }),
    });

    let sampleResponse: SampleResponse;
    try {
      sampleResponse = await sampleForFilter(spec, sampleStrategy, cacheKey);
    } catch (e) {
      this.setState({
        filterQueryState: new QueryState({ error: e.message }),
      });
      return;
    }

    if (sampleResponse.data.length) {
      this.setState({
        cacheKey: sampleResponse.cacheKey,
        filterQueryState: new QueryState({
          data: headerAndRowsFromSampleResponse(
            sampleResponse,
            undefined,
            ['__time'].concat(parserColumns),
            true,
          ),
        }),
      });
      return;
    }

    // The filters matched no data
    let sampleResponseNoFilter: SampleResponse;
    try {
      const specNoFilter = deepSet(spec, 'dataSchema.transformSpec.filter', null);
      sampleResponseNoFilter = await sampleForFilter(specNoFilter, sampleStrategy, cacheKey);
    } catch (e) {
      this.setState({
        filterQueryState: new QueryState({ error: e.message }),
      });
      return;
    }

    const headerAndRowsNoFilter = headerAndRowsFromSampleResponse(
      sampleResponseNoFilter,
      undefined,
      ['__time'].concat(parserColumns),
      true,
    );

    this.setState({
      cacheKey: sampleResponseNoFilter.cacheKey,
      filterQueryState: new QueryState({
        data: deepSet(headerAndRowsNoFilter, 'rows', []),
      }),
    });
  }

  private getMemoizedDimensionFiltersFromSpec = memoize(spec => {
    const { dimensionFilters } = splitFilter(deepGet(spec, 'dataSchema.transformSpec.filter'));
    return dimensionFilters;
  });

  renderFilterStep() {
    const {
      spec,
      columnFilter,
      filterQueryState,
      selectedFilter,
      selectedFilterIndex,
      showGlobalFilter,
    } = this.state;
    const parseSpec: ParseSpec = deepGet(spec, 'dataSchema.parser.parseSpec') || EMPTY_OBJECT;
    const dimensionFilters = this.getMemoizedDimensionFiltersFromSpec(spec);

    const isBlank = !parseSpec.format;

    let mainFill: JSX.Element | string = '';
    if (filterQueryState.isInit()) {
      mainFill = <CenterMessage>Please enter more details for the previous steps</CenterMessage>;
    } else if (filterQueryState.isLoading()) {
      mainFill = <Loader loading />;
    } else if (filterQueryState.error) {
      mainFill = <CenterMessage>{`Error: ${filterQueryState.error}`}</CenterMessage>;
    } else if (filterQueryState.data) {
      mainFill = (
        <div className="table-with-control">
          <div className="table-control">
            <ClearableInput
              value={columnFilter}
              onChange={columnFilter => this.setState({ columnFilter })}
              placeholder="Search columns"
            />
          </div>
          <FilterTable
            sampleData={filterQueryState.data}
            columnFilter={columnFilter}
            dimensionFilters={dimensionFilters}
            selectedFilterIndex={selectedFilterIndex}
            onShowGlobalFilter={this.onShowGlobalFilter}
            onFilterSelect={this.onFilterSelect}
          />
        </div>
      );
    }

    return (
      <>
        <div className="main">{mainFill}</div>
        <div className="control">
          <Callout className="intro">
            <p className="optional">Optional</p>
            <p>
              Druid can{' '}
              <ExternalLink href="https://druid.apache.org/docs/latest/querying/filters.html">
                filter
              </ExternalLink>{' '}
              out unwanted data.
            </p>
            <p>Click "Preview" to see the impact of any specified filters.</p>
          </Callout>
          {!showGlobalFilter && this.renderColumnFilterControls()}
          {!selectedFilter && this.renderGlobalFilterControls()}
          {!selectedFilter && !showGlobalFilter && (
            <Button text="Preview" disabled={isBlank} onClick={() => this.queryForFilter()} />
          )}
        </div>
        {this.renderNextBar({})}
      </>
    );
  }

  private onShowGlobalFilter = () => {
    this.setState({ showGlobalFilter: true });
  };

  private onFilterSelect = (filter: DruidFilter, index: number) => {
    this.setState({
      selectedFilterIndex: index,
      selectedFilter: filter,
    });
  };

  renderColumnFilterControls() {
    const { spec, selectedFilter, selectedFilterIndex } = this.state;

    const close = () => {
      this.setState({
        selectedFilterIndex: -1,
        selectedFilter: undefined,
      });
    };

    const closeAndQuery = () => {
      close();
      setTimeout(() => {
        this.queryForFilter();
      }, 10);
    };

    if (selectedFilter) {
      return (
        <div className="edit-controls">
          <AutoForm
            fields={getFilterFormFields()}
            model={selectedFilter}
            onChange={f => this.setState({ selectedFilter: f })}
            showCustom={f => !['selector', 'in', 'regex', 'like', 'not'].includes(f.type)}
          />
          <div className="controls-buttons">
            <Button
              className="add-update"
              text={selectedFilterIndex === -1 ? 'Add' : 'Update'}
              intent={Intent.PRIMARY}
              onClick={() => {
                const curFilter = splitFilter(deepGet(spec, 'dataSchema.transformSpec.filter'));
                const newFilter = joinFilter(
                  deepSet(curFilter, `dimensionFilters.${selectedFilterIndex}`, selectedFilter),
                );
                this.updateSpec(deepSet(spec, 'dataSchema.transformSpec.filter', newFilter));
                closeAndQuery();
              }}
            />
            {selectedFilterIndex !== -1 && (
              <Button
                icon={IconNames.TRASH}
                intent={Intent.DANGER}
                onClick={() => {
                  const curFilter = splitFilter(deepGet(spec, 'dataSchema.transformSpec.filter'));
                  const newFilter = joinFilter(
                    deepDelete(curFilter, `dimensionFilters.${selectedFilterIndex}`),
                  );
                  this.updateSpec(deepSet(spec, 'dataSchema.transformSpec.filter', newFilter));
                  closeAndQuery();
                }}
              />
            )}
            <Button className="cancel" text="Cancel" onClick={close} />
          </div>
        </div>
      );
    } else {
      return (
        <FormGroup>
          <Button
            text="Add column filter"
            onClick={() => {
              this.setState({
                selectedFilter: { type: 'selector', dimension: '', value: '' },
                selectedFilterIndex: -1,
              });
            }}
          />
        </FormGroup>
      );
    }
  }

  renderGlobalFilterControls() {
    const { spec, showGlobalFilter } = this.state;
    const intervals: string[] = deepGet(spec, 'dataSchema.granularitySpec.intervals');
    const { restFilter } = splitFilter(deepGet(spec, 'dataSchema.transformSpec.filter'));
    const hasGlobalFilter = Boolean(intervals || restFilter);

    if (showGlobalFilter) {
      return (
        <div className="edit-controls">
          <AutoForm
            fields={[
              {
                name: 'dataSchema.granularitySpec.intervals',
                label: 'Time intervals',
                type: 'string-array',
                placeholder: 'ex: 2018-01-01/2018-06-01',
                info: (
                  <>
                    A comma separated list of intervals for the raw data being ingested. Ignored for
                    real-time ingestion.
                  </>
                ),
              },
            ]}
            model={spec}
            onChange={s => this.updateSpec(s)}
          />
          <FormGroup label="Extra filter">
            <JsonInput
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
        </div>
      );
    } else {
      return (
        <FormGroup>
          <Button
            text={`${hasGlobalFilter ? 'Edit' : 'Add'} global filter`}
            onClick={() => this.setState({ showGlobalFilter: true })}
          />
        </FormGroup>
      );
    }
  }

  // ==================================================================

  async queryForSchema(initRun = false) {
    const { spec, sampleStrategy, cacheKey } = this.state;
    const ioConfig: IoConfig = deepGet(spec, 'ioConfig') || EMPTY_OBJECT;
    const parser: Parser = deepGet(spec, 'dataSchema.parser') || EMPTY_OBJECT;
    const parserColumns: string[] = deepGet(parser, 'parseSpec.columns') || [];
    const metricsSpec: MetricSpec[] = deepGet(spec, 'dataSchema.metricsSpec') || EMPTY_ARRAY;
    const dimensionsSpec: DimensionsSpec =
      deepGet(spec, 'dataSchema.parser.parseSpec.dimensionsSpec') || EMPTY_OBJECT;

    let issue: string | undefined;
    if (issueWithIoConfig(ioConfig)) {
      issue = `IoConfig not ready, ${issueWithIoConfig(ioConfig)}`;
    } else if (issueWithParser(parser)) {
      issue = `Parser not ready, ${issueWithParser(parser)}`;
    }

    if (issue) {
      this.setState({
        schemaQueryState: initRun ? QueryState.INIT : new QueryState({ error: issue }),
      });
      return;
    }

    this.setState({
      schemaQueryState: new QueryState({ loading: true }),
    });

    let sampleResponse: SampleResponse;
    try {
      sampleResponse = await sampleForSchema(spec, sampleStrategy, cacheKey);
    } catch (e) {
      this.setState({
        schemaQueryState: new QueryState({ error: e.message }),
      });
      return;
    }

    this.setState({
      cacheKey: sampleResponse.cacheKey,
      schemaQueryState: new QueryState({
        data: {
          headerAndRows: headerAndRowsFromSampleResponse(
            sampleResponse,
            undefined,
            ['__time'].concat(parserColumns),
          ),
          dimensionsSpec,
          metricsSpec,
        },
      }),
    });
  }

  renderSchemaStep() {
    const {
      spec,
      columnFilter,
      schemaQueryState,
      selectedDimensionSpec,
      selectedDimensionSpecIndex,
      selectedMetricSpec,
      selectedMetricSpecIndex,
    } = this.state;
    const rollup: boolean = Boolean(deepGet(spec, 'dataSchema.granularitySpec.rollup'));
    const somethingSelected = Boolean(selectedDimensionSpec || selectedMetricSpec);
    const dimensionMode = getDimensionMode(spec);

    let mainFill: JSX.Element | string = '';
    if (schemaQueryState.isInit()) {
      mainFill = <CenterMessage>Please enter more details for the previous steps</CenterMessage>;
    } else if (schemaQueryState.isLoading()) {
      mainFill = <Loader loading />;
    } else if (schemaQueryState.error) {
      mainFill = <CenterMessage>{`Error: ${schemaQueryState.error}`}</CenterMessage>;
    } else if (schemaQueryState.data) {
      mainFill = (
        <div className="table-with-control">
          <div className="table-control">
            <ClearableInput
              value={columnFilter}
              onChange={columnFilter => this.setState({ columnFilter })}
              placeholder="Search columns"
            />
          </div>
          <SchemaTable
            sampleBundle={schemaQueryState.data}
            columnFilter={columnFilter}
            selectedDimensionSpecIndex={selectedDimensionSpecIndex}
            selectedMetricSpecIndex={selectedMetricSpecIndex}
            onDimensionOrMetricSelect={this.onDimensionOrMetricSelect}
          />
        </div>
      );
    }

    return (
      <>
        <div className="main">{mainFill}</div>
        <div className="control">
          <Callout className="intro">
            <p>
              Each column in Druid must have an assigned type (string, long, float, complex, etc).
              Default primitive types have been automatically assigned to your columns. If you want
              to change the type, click on the column header.
            </p>
            <p>
              Select whether or not you want to{' '}
              <ExternalLink href="https://druid.apache.org/docs/latest/tutorials/tutorial-rollup.html">
                roll-up
              </ExternalLink>{' '}
              your data.
            </p>
          </Callout>
          {!somethingSelected && (
            <>
              <FormGroup>
                <Switch
                  checked={dimensionMode === 'specific'}
                  onChange={() =>
                    this.setState({
                      newDimensionMode: dimensionMode === 'specific' ? 'auto-detect' : 'specific',
                    })
                  }
                  label="Explicitly specify dimension list"
                />
                <Popover
                  content={
                    <div className="label-info-text">
                      <p>
                        Select whether or not you want to set an explicit list of{' '}
                        <ExternalLink href="https://druid.apache.org/docs/latest/ingestion/ingestion-spec.html#dimensionsspec">
                          dimensions
                        </ExternalLink>{' '}
                        and{' '}
                        <ExternalLink href="https://druid.apache.org/docs/latest/querying/aggregations.html">
                          metrics
                        </ExternalLink>
                        . Explicitly setting dimensions and metrics can lead to better compression
                        and performance. If you disable this option, Druid will try to auto-detect
                        fields in your data and treat them as individual columns.
                      </p>
                    </div>
                  }
                  position="left-bottom"
                >
                  <Icon icon={IconNames.INFO_SIGN} iconSize={14} />
                </Popover>
              </FormGroup>
              {dimensionMode === 'auto-detect' && (
                <AutoForm
                  fields={[
                    {
                      name: 'dataSchema.parser.parseSpec.dimensionsSpec.dimensionExclusions',
                      label: 'Exclusions',
                      type: 'string-array',
                      info: (
                        <>
                          Provide a comma separated list of columns (use the column name from the
                          raw data) you do not want Druid to ingest.
                        </>
                      ),
                    },
                  ]}
                  model={spec}
                  onChange={s => this.updateSpec(s)}
                />
              )}
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
                        If you enable roll-up, Druid will try to pre-aggregate data before indexing
                        it to conserve storage. The primary timestamp will be truncated to the
                        specified query granularity, and rows containing the same string field
                        values will be aggregated together.
                      </p>
                      <p>
                        If you enable rollup, you must specify which columns are{' '}
                        <a href="https://druid.apache.org/docs/latest/ingestion/ingestion-spec.html#dimensionsspec">
                          dimensions
                        </a>{' '}
                        (fields you want to group and filter on), and which are{' '}
                        <a href="https://druid.apache.org/docs/latest/querying/aggregations.html">
                          metrics
                        </a>{' '}
                        (fields you want to aggregate on).
                      </p>
                    </div>
                  }
                  position="left-bottom"
                >
                  <Icon icon={IconNames.INFO_SIGN} iconSize={14} />
                </Popover>
              </FormGroup>
              <AutoForm
                fields={[
                  {
                    name: 'dataSchema.granularitySpec.queryGranularity',
                    label: 'Query granularity',
                    type: 'string',
                    suggestions: ['NONE', 'MINUTE', 'HOUR', 'DAY'],
                    info: (
                      <>
                        This granularity determines how timestamps will be truncated (not at all, to
                        the minute, hour, day, etc). After data is rolled up, this granularity
                        becomes the minimum granularity you can query data at.
                      </>
                    ),
                  },
                ]}
                model={spec}
                onChange={s => this.updateSpec(s)}
                onFinalize={() => {
                  setTimeout(() => {
                    this.queryForSchema();
                  }, 10);
                }}
              />
            </>
          )}
          {!selectedMetricSpec && this.renderDimensionSpecControls()}
          {!selectedDimensionSpec && this.renderMetricSpecControls()}
          {this.renderChangeRollupAction()}
          {this.renderChangeDimensionModeAction()}
        </div>
        {this.renderNextBar({
          disabled: !schemaQueryState.data,
        })}
      </>
    );
  }

  private onDimensionOrMetricSelect = (
    selectedDimensionSpec: DimensionSpec | undefined,
    selectedDimensionSpecIndex: number,
    selectedMetricSpec: MetricSpec | undefined,
    selectedMetricSpecIndex: number,
  ) => {
    this.setState({
      selectedDimensionSpec,
      selectedDimensionSpecIndex,
      selectedMetricSpec,
      selectedMetricSpecIndex,
    });
  };

  renderChangeRollupAction() {
    const { newRollup, spec, sampleStrategy, cacheKey } = this.state;
    if (typeof newRollup === 'undefined') return;

    return (
      <AsyncActionDialog
        action={async () => {
          const sampleResponse = await sampleForTransform(spec, sampleStrategy, cacheKey);
          this.updateSpec(
            updateSchemaWithSample(
              spec,
              headerAndRowsFromSampleResponse(sampleResponse),
              getDimensionMode(spec),
              newRollup,
            ),
          );
          setTimeout(() => {
            this.queryForSchema();
          }, 10);
        }}
        confirmButtonText={`Yes - ${newRollup ? 'enable' : 'disable'} rollup`}
        successText={`Rollup was ${newRollup ? 'enabled' : 'disabled'}. Schema has been updated.`}
        failText="Could change rollup"
        intent={Intent.WARNING}
        onClose={() => this.setState({ newRollup: undefined })}
      >
        <p>{`Are you sure you want to ${newRollup ? 'enable' : 'disable'} rollup?`}</p>
        <p>Making this change will reset any work you have done in this section.</p>
      </AsyncActionDialog>
    );
  }

  renderChangeDimensionModeAction() {
    const { newDimensionMode, spec, sampleStrategy, cacheKey } = this.state;
    if (typeof newDimensionMode === 'undefined') return;
    const autoDetect = newDimensionMode === 'auto-detect';

    return (
      <AsyncActionDialog
        action={async () => {
          const sampleResponse = await sampleForTransform(spec, sampleStrategy, cacheKey);
          this.updateSpec(
            updateSchemaWithSample(
              spec,
              headerAndRowsFromSampleResponse(sampleResponse),
              newDimensionMode,
              getRollup(spec),
            ),
          );
          setTimeout(() => {
            this.queryForSchema();
          }, 10);
        }}
        confirmButtonText={`Yes - ${autoDetect ? 'auto detect' : 'explicitly set'} columns`}
        successText={`Dimension mode changes to ${
          autoDetect ? 'auto detect' : 'specific list'
        }. Schema has been updated.`}
        failText="Could change dimension mode"
        intent={Intent.WARNING}
        onClose={() => this.setState({ newDimensionMode: undefined })}
      >
        <p>
          {autoDetect
            ? `Are you sure you don't want to explicitly specify a dimension list?`
            : `Are you sure you want to explicitly specify a dimension list?`}
        </p>
        <p>Making this change will reset any work you have done in this section.</p>
      </AsyncActionDialog>
    );
  }

  renderDimensionSpecControls() {
    const { spec, selectedDimensionSpec, selectedDimensionSpecIndex } = this.state;

    const close = () => {
      this.setState({
        selectedDimensionSpecIndex: -1,
        selectedDimensionSpec: undefined,
      });
    };

    const closeAndQuery = () => {
      close();
      setTimeout(() => {
        this.queryForSchema();
      }, 10);
    };

    if (selectedDimensionSpec) {
      const curDimensions =
        deepGet(spec, `dataSchema.parser.parseSpec.dimensionsSpec.dimensions`) || EMPTY_ARRAY;

      return (
        <div className="edit-controls">
          <AutoForm
            fields={getDimensionSpecFormFields()}
            model={selectedDimensionSpec}
            onChange={selectedDimensionSpec => this.setState({ selectedDimensionSpec })}
          />
          <div className="controls-buttons">
            <Button
              className="add-update"
              text={selectedDimensionSpecIndex === -1 ? 'Add' : 'Update'}
              intent={Intent.PRIMARY}
              onClick={() => {
                this.updateSpec(
                  deepSet(
                    spec,
                    `dataSchema.parser.parseSpec.dimensionsSpec.dimensions.${selectedDimensionSpecIndex}`,
                    selectedDimensionSpec,
                  ),
                );
                closeAndQuery();
              }}
            />
            {selectedDimensionSpecIndex !== -1 && (
              <Button
                icon={IconNames.TRASH}
                intent={Intent.DANGER}
                disabled={curDimensions.length <= 1}
                onClick={() => {
                  if (curDimensions.length <= 1) return; // Guard against removing the last dimension

                  this.updateSpec(
                    deepDelete(
                      spec,
                      `dataSchema.parser.parseSpec.dimensionsSpec.dimensions.${selectedDimensionSpecIndex}`,
                    ),
                  );
                  closeAndQuery();
                }}
              />
            )}
            <Button className="cancel" text="Cancel" onClick={close} />
          </div>
        </div>
      );
    } else {
      return (
        <FormGroup>
          <Button
            text="Add dimension"
            disabled={getDimensionMode(spec) !== 'specific'}
            onClick={() => {
              this.setState({
                selectedDimensionSpecIndex: -1,
                selectedDimensionSpec: {
                  name: 'new_dimension',
                  type: 'string',
                },
              });
            }}
          />
        </FormGroup>
      );
    }
  }

  renderMetricSpecControls() {
    const { spec, selectedMetricSpec, selectedMetricSpecIndex } = this.state;

    const close = () => {
      this.setState({
        selectedMetricSpecIndex: -1,
        selectedMetricSpec: undefined,
      });
    };

    const closeAndQuery = () => {
      close();
      setTimeout(() => {
        this.queryForSchema();
      }, 10);
    };

    if (selectedMetricSpec) {
      return (
        <div className="edit-controls">
          <AutoForm
            fields={getMetricSpecFormFields()}
            model={selectedMetricSpec}
            onChange={selectedMetricSpec => this.setState({ selectedMetricSpec })}
          />
          <div className="controls-buttons">
            <Button
              className="add-update"
              text={selectedMetricSpecIndex === -1 ? 'Add' : 'Update'}
              intent={Intent.PRIMARY}
              onClick={() => {
                this.updateSpec(
                  deepSet(
                    spec,
                    `dataSchema.metricsSpec.${selectedMetricSpecIndex}`,
                    selectedMetricSpec,
                  ),
                );
                closeAndQuery();
              }}
            />
            {selectedMetricSpecIndex !== -1 && (
              <Button
                icon={IconNames.TRASH}
                intent={Intent.DANGER}
                onClick={() => {
                  this.updateSpec(
                    deepDelete(spec, `dataSchema.metricsSpec.${selectedMetricSpecIndex}`),
                  );
                  closeAndQuery();
                }}
              />
            )}
            <Button className="cancel" text="Cancel" onClick={close} />
          </div>
        </div>
      );
    } else {
      return (
        <FormGroup>
          <Button
            text="Add metric"
            onClick={() => {
              this.setState({
                selectedMetricSpecIndex: -1,
                selectedMetricSpec: {
                  name: 'sum_blah',
                  type: 'doubleSum',
                  fieldName: '',
                },
              });
            }}
          />
        </FormGroup>
      );
    }
  }

  // ==================================================================

  renderPartitionStep() {
    const { spec } = this.state;
    const tuningConfig: TuningConfig = deepGet(spec, 'tuningConfig') || EMPTY_OBJECT;
    const granularitySpec: GranularitySpec =
      deepGet(spec, 'dataSchema.granularitySpec') || EMPTY_OBJECT;

    return (
      <>
        <div className="main">
          <H5>Primary partitioning (by time)</H5>
          <AutoForm
            fields={[
              {
                name: 'type',
                type: 'string',
                suggestions: ['uniform', 'arbitrary'],
                info: <>This spec is used to generated segments with uniform intervals.</>,
              },
              {
                name: 'segmentGranularity',
                type: 'string',
                suggestions: ['HOUR', 'DAY', 'WEEK', 'MONTH', 'YEAR'],
                defined: (g: GranularitySpec) => g.type === 'uniform',
                required: true,
                info: (
                  <>
                    The granularity to create time chunks at. Multiple segments can be created per
                    time chunk. For example, with 'DAY' segmentGranularity, the events of the same
                    day fall into the same time chunk which can be optionally further partitioned
                    into multiple segments based on other configurations and input size.
                  </>
                ),
              },
            ]}
            model={granularitySpec}
            onChange={g => this.updateSpec(deepSet(spec, 'dataSchema.granularitySpec', g))}
          />
          <AutoForm
            fields={[
              {
                name: 'dataSchema.granularitySpec.intervals',
                label: 'Time intervals',
                type: 'string-array',
                placeholder: 'ex: 2018-01-01/2018-06-01',
                required: s => Boolean(deepGet(s, 'tuningConfig.forceGuaranteedRollup')),
                info: (
                  <>
                    A comma separated list of intervals for the raw data being ingested. Ignored for
                    real-time ingestion.
                  </>
                ),
              },
            ]}
            model={spec}
            onChange={s => this.updateSpec(s)}
          />
        </div>
        <div className="other">
          <H5>Secondary partitioning</H5>
          <AutoForm
            fields={getPartitionRelatedTuningSpecFormFields(getSpecType(spec) || 'index')}
            model={tuningConfig}
            onChange={t => this.updateSpec(deepSet(spec, 'tuningConfig', t))}
          />
        </div>
        <div className="control">
          <Callout className="intro">
            <p className="optional">Optional</p>
            <p>Configure how Druid will partition data.</p>
          </Callout>
          {this.renderParallelPickerIfNeeded()}
        </div>
        {this.renderNextBar({
          disabled:
            !granularitySpec.segmentGranularity ||
            invalidTuningConfig(tuningConfig, granularitySpec.intervals),
        })}
      </>
    );
  }

  // ==================================================================

  renderTuningStep() {
    const { spec } = this.state;
    const ioConfig: IoConfig = deepGet(spec, 'ioConfig') || EMPTY_OBJECT;
    const tuningConfig: TuningConfig = deepGet(spec, 'tuningConfig') || EMPTY_OBJECT;

    const ingestionComboType = getIngestionComboType(spec);
    const inputTuningFields = ingestionComboType
      ? getIoConfigTuningFormFields(ingestionComboType)
      : null;
    return (
      <>
        <div className="main">
          <H5>Input tuning</H5>
          {inputTuningFields ? (
            inputTuningFields.length ? (
              <AutoForm
                fields={inputTuningFields}
                model={ioConfig}
                onChange={c => this.updateSpec(deepSet(spec, 'ioConfig', c))}
              />
            ) : (
              <div>
                {ioConfig.firehose
                  ? `No specific tuning configs for firehose of type '${deepGet(
                      ioConfig,
                      'firehose.type',
                    )}'.`
                  : `No specific tuning configs.`}
              </div>
            )
          ) : (
            <JsonInput
              value={ioConfig}
              onChange={c => this.updateSpec(deepSet(spec, 'ioConfig', c))}
              height="300px"
            />
          )}
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
            <p className="optional">Optional</p>
            <p>Fine tune how Druid will ingest data.</p>
          </Callout>
          {this.renderParallelPickerIfNeeded()}
        </div>
        {this.renderNextBar({
          disabled: invalidIoConfig(ioConfig),
        })}
      </>
    );
  }

  renderParallelPickerIfNeeded(): JSX.Element | undefined {
    const { spec } = this.state;
    if (!hasParallelAbility(spec)) return;

    return (
      <FormGroup>
        <Switch
          large
          checked={isParallel(spec)}
          onChange={() => this.updateSpec(changeParallel(spec, !isParallel(spec)))}
          labelElement={
            <>
              {'Parallel indexing '}
              <Popover
                content={
                  <div className="label-info-text">
                    Druid currently has two types of native batch indexing tasks,{' '}
                    <Code>index_parallel</Code> which runs tasks in parallel on multiple
                    MiddleManager processes, and <Code>index</Code> which will run a single indexing
                    task locally on a single MiddleManager.
                  </div>
                }
                position="left-bottom"
              >
                <Icon icon={IconNames.INFO_SIGN} iconSize={16} />
              </Popover>
            </>
          }
        />
      </FormGroup>
    );
  }

  // ==================================================================

  renderPublishStep() {
    const { spec } = this.state;
    const parallel = isParallel(spec);

    return (
      <>
        <div className="main">
          <H5>Publish configuration</H5>
          <AutoForm
            fields={[
              {
                name: 'dataSchema.dataSource',
                label: 'Datasource name',
                type: 'string',
                info: <>This is the name of the data source (table) in Druid.</>,
              },
              {
                name: 'ioConfig.appendToExisting',
                label: 'Append to existing',
                type: 'boolean',
                defaultValue: false,
                defined: spec => !deepGet(spec, 'tuningConfig.forceGuaranteedRollup'),
                info: (
                  <>
                    Creates segments as additional shards of the latest version, effectively
                    appending to the segment set instead of replacing it.
                  </>
                ),
              },
            ]}
            model={spec}
            onChange={s => this.updateSpec(s)}
          />
        </div>
        <div className="other">
          <H5>Parse error reporting</H5>
          <AutoForm
            fields={[
              {
                name: 'tuningConfig.logParseExceptions',
                label: 'Log parse exceptions',
                type: 'boolean',
                defaultValue: false,
                disabled: parallel,
                info: (
                  <>
                    If true, log an error message when a parsing exception occurs, containing
                    information about the row where the error occurred.
                  </>
                ),
              },
              {
                name: 'tuningConfig.maxParseExceptions',
                label: 'Max parse exceptions',
                type: 'number',
                disabled: parallel,
                placeholder: '(unlimited)',
                info: (
                  <>
                    The maximum number of parse exceptions that can occur before the task halts
                    ingestion and fails.
                  </>
                ),
              },
              {
                name: 'tuningConfig.maxSavedParseExceptions',
                label: 'Max saved parse exceptions',
                type: 'number',
                disabled: parallel,
                defaultValue: 0,
                info: (
                  <>
                    <p>
                      When a parse exception occurs, Druid can keep track of the most recent parse
                      exceptions.
                    </p>
                    <p>
                      This property limits how many exception instances will be saved. These saved
                      exceptions will be made available after the task finishes in the task view.
                    </p>
                  </>
                ),
              },
            ]}
            model={spec}
            onChange={s => this.updateSpec(s)}
          />
        </div>
        <div className="control">
          <Callout className="intro">
            <p>Configure behavior of indexed data once it reaches Druid.</p>
          </Callout>
          {this.renderParallelPickerIfNeeded()}
        </div>
        {this.renderNextBar({})}
      </>
    );
  }

  // ==================================================================
  private getSupervisorJson = async (): Promise<void> => {
    const { initSupervisorId } = this.props;

    try {
      const resp = await axios.get(`/druid/indexer/v1/supervisor/${initSupervisorId}`);
      this.updateSpec(resp.data);
      this.setState({ continueToSpec: true });
      this.updateStep('spec');
    } catch (e) {
      AppToaster.show({
        message: `Failed to get supervisor spec: ${getDruidErrorMessage(e)}`,
        intent: Intent.DANGER,
      });
    }
  };

  private getTaskJson = async (): Promise<void> => {
    const { initTaskId } = this.props;

    try {
      const resp = await axios.get(`/druid/indexer/v1/task/${initTaskId}`);
      this.updateSpec(resp.data.payload);
      this.setState({ continueToSpec: true });
      this.updateStep('spec');
    } catch (e) {
      AppToaster.show({
        message: `Failed to get task spec: ${getDruidErrorMessage(e)}`,
        intent: Intent.DANGER,
      });
    }
  };

  renderLoading() {
    return <Loader loading />;
  }

  renderSpecStep() {
    const { spec, submitting } = this.state;

    return (
      <>
        <div className="main">
          <JsonInput
            value={spec}
            onChange={s => {
              if (!s) return;
              this.updateSpec(s);
            }}
            height="100%"
          />
        </div>
        <div className="control">
          <Callout className="intro">
            <p className="optional">Optional</p>
            <p>
              Druid begins ingesting data once you submit a JSON ingestion spec. If you modify any
              values in this view, the values entered in previous sections will update accordingly.
              If you modify any values in previous sections, this spec will automatically update.
            </p>
            <p>Submit the spec to begin loading data into Druid.</p>
          </Callout>
        </div>
        <div className="next-bar">
          {!isEmptyIngestionSpec(spec) && (
            <Button
              className="left"
              icon={IconNames.RESET}
              text="Reset spec"
              onClick={this.handleResetConfirm}
            />
          )}
          <Button
            text="Submit"
            rightIcon={IconNames.CLOUD_UPLOAD}
            intent={Intent.PRIMARY}
            disabled={submitting}
            onClick={this.handleSubmit}
          />
        </div>
      </>
    );
  }

  private handleSubmit = async () => {
    const { goToTask } = this.props;
    const { spec, submitting } = this.state;
    if (submitting) return;

    this.setState({ submitting: true });
    if (isTask(spec)) {
      let taskResp: any;
      try {
        taskResp = await axios.post('/druid/indexer/v1/task', {
          type: spec.type,
          spec,

          // A hack to let context be set from the spec can be removed when https://github.com/apache/incubator-druid/issues/8662 is resolved
          context: (spec as any).context,
        });
      } catch (e) {
        AppToaster.show({
          message: `Failed to submit task: ${getDruidErrorMessage(e)}`,
          intent: Intent.DANGER,
        });
        this.setState({ submitting: false });
        return;
      }

      AppToaster.show({
        message: 'Task submitted successfully. Going to task view...',
        intent: Intent.SUCCESS,
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
          intent: Intent.DANGER,
        });
        this.setState({ submitting: false });
        return;
      }

      AppToaster.show({
        message: 'Supervisor submitted successfully. Going to task view...',
        intent: Intent.SUCCESS,
      });

      setTimeout(() => {
        goToTask(undefined); // Can we get the supervisor ID here?
      }, 1000);
    }
  };
}
