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

import type { IconName } from '@blueprintjs/core';
import {
  AnchorButton,
  Button,
  ButtonGroup,
  Callout,
  Card,
  Code,
  FormGroup,
  H5,
  Icon,
  InputGroup,
  Intent,
  Menu,
  MenuItem,
  Radio,
  RadioGroup,
  Switch,
  TextArea,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import classNames from 'classnames';
import * as JSONBig from 'json-bigint-native';
import memoize from 'memoize-one';
import type { JSX } from 'react';
import React from 'react';

import {
  ArrayModeSwitch,
  AutoForm,
  CenterMessage,
  ClearableInput,
  ExternalLink,
  FormGroupWithInfo,
  JsonInput,
  Loader,
  PopoverText,
} from '../../components';
import { AlertDialog, AsyncActionDialog } from '../../dialogs';
import type {
  ArrayMode,
  DimensionSpec,
  DruidFilter,
  FlattenField,
  IngestionComboTypeWithExtra,
  IngestionSpec,
  InputFormat,
  IoConfig,
  MetricSpec,
  SchemaMode,
  TimestampSpec,
  Transform,
  TuningConfig,
} from '../../druid-models';
import {
  addTimestampTransform,
  adjustForceGuaranteedRollup,
  adjustId,
  BATCH_INPUT_FORMAT_FIELDS,
  changeFlattenSpec,
  chooseByBestTimestamp,
  cleanSpec,
  computeFlattenPathsForData,
  CONSTANT_TIMESTAMP_SPEC,
  CONSTANT_TIMESTAMP_SPEC_FIELDS,
  DIMENSION_SPEC_FIELDS,
  fillDataSourceNameIfNeeded,
  fillInputFormatIfNeeded,
  FILTER_FIELDS,
  FILTERS_FIELDS,
  FLATTEN_FIELD_FIELDS,
  getArrayMode,
  getDimensionSpecName,
  getFlattenSpec,
  getIngestionComboType,
  getIngestionImage,
  getIngestionTitle,
  getIoConfigFormFields,
  getIoConfigTuningFormFields,
  getIssueWithSpec,
  getMetricSpecName,
  getPossibleSystemFieldsForSpec,
  getRequiredModule,
  getRollup,
  getSchemaMode,
  getSecondaryPartitionRelatedFormFields,
  getSpecType,
  getTimestampExpressionFields,
  getTimestampSchema,
  getTuningFormFields,
  inputFormatCanProduceNestedData,
  invalidIoConfig,
  invalidPartitionConfig,
  isDruidSource,
  isEmptyIngestionSpec,
  isStreamingSpec,
  issueWithIoConfig,
  issueWithSampleData,
  joinFilter,
  KAFKA_METADATA_INPUT_FORMAT_FIELDS,
  KNOWN_FILTER_TYPES,
  MAX_INLINE_DATA_LENGTH,
  METRIC_SPEC_FIELDS,
  normalizeSpec,
  possibleDruidFormatForValues,
  PRIMARY_PARTITION_RELATED_FORM_FIELDS,
  removeTimestampTransform,
  showArrayModeToggle,
  splitFilter,
  STREAMING_INPUT_FORMAT_FIELDS,
  TIME_COLUMN,
  TIMESTAMP_SPEC_FIELDS,
  TRANSFORM_FIELDS,
  updateIngestionType,
  updateSchemaWithSample,
  upgradeSpec,
} from '../../druid-models';
import { getSpecDatasourceName } from '../../helpers';
import { getLink } from '../../links';
import { Api, AppToaster, UrlBaser } from '../../singletons';
import {
  alphanumericCompare,
  compact,
  deepDelete,
  deepGet,
  deepMove,
  deepSet,
  deepSetMulti,
  EMPTY_ARRAY,
  EMPTY_OBJECT,
  filterMap,
  getDruidErrorMessage,
  localStorageGetJson,
  LocalStorageKeys,
  localStorageSetJson,
  moveElement,
  moveToIndex,
  pluralIfNeeded,
  QueryState,
} from '../../utils';
import type {
  CacheRows,
  SampleEntry,
  SampleResponse,
  SampleResponseWithExtraInfo,
  SampleStrategy,
} from '../../utils/sampler';
import {
  getCacheRowsFromSampleResponse,
  getHeaderNamesFromSampleResponse,
  getProxyOverlordModules,
  guessDimensionsFromSampleResponse,
  sampleForConnect,
  sampleForFilter,
  sampleForParser,
  sampleForSchema,
  sampleForTimestamp,
  sampleForTransform,
} from '../../utils/sampler';

import { ExamplePicker } from './example-picker/example-picker';
import { EXAMPLE_SPECS } from './example-specs';
import { FilterTable, filterTableSelectedColumnName } from './filter-table/filter-table';
import { FormEditor } from './form-editor/form-editor';
import {
  AppendToExistingIssue,
  ConnectMessage,
  FilterMessage,
  ParserMessage,
  PartitionMessage,
  PublishMessage,
  SchemaMessage,
  SpecMessage,
  TimestampMessage,
  TransformMessage,
  TuningMessage,
} from './info-messages';
import { ParseDataTable } from './parse-data-table/parse-data-table';
import {
  ParseTimeTable,
  parseTimeTableSelectedColumnName,
} from './parse-time-table/parse-time-table';
import { ReorderMenu } from './reorder-menu/reorder-menu';
import { SchemaTable } from './schema-table/schema-table';
import {
  TransformTable,
  transformTableSelectedColumnName,
} from './transform-table/transform-table';

import './load-data-view.scss';

function showRawLine(line: SampleEntry): string {
  if (!line.parsed) return 'No parse';
  const raw = line.parsed.raw;
  if (typeof raw !== 'string') return String(raw);
  if (raw.includes('\n')) {
    return `[Multi-line row, length: ${raw.length}]`;
  }
  if (raw.length > 1000) {
    return raw.slice(0, 1000) + '...';
  }
  return raw;
}

function showDruidLine(line: SampleEntry): string {
  if (!line.input) return 'Invalid druid row';
  return `[Druid row: ${JSONBig.stringify(line.input)}]`;
}

function showKafkaLine(line: SampleEntry): string {
  const { input } = line;
  if (!input) return 'Invalid kafka row';
  return compact([
    `[ Kafka timestamp: ${input['kafka.timestamp']}`,
    `  Topic: ${input['kafka.topic']}`,
    ...filterMap(Object.entries(input), ([k, v]) => {
      if (!k.startsWith('kafka.header.')) return;
      return `  Header: ${k.slice(13)}=${v}`;
    }),
    input['kafka.key'] ? `  Key: ${input['kafka.key']}` : undefined,
    `  Payload: ${input.raw}`,
    ']',
  ]).join('\n');
}

function showBlankLine(line: SampleEntry): string {
  return line.parsed ? `[Row: ${JSONBig.stringify(line.parsed)}]` : '[Binary data]';
}

function formatSampleEntries(
  sampleEntries: SampleEntry[],
  druidSource: boolean,
  kafkaSource: boolean,
): string {
  if (!sampleEntries.length) return 'No data returned from sampler';

  if (druidSource) {
    return sampleEntries.map(showDruidLine).join('\n');
  }

  if (kafkaSource) {
    return sampleEntries.map(showKafkaLine).join('\n');
  }

  return (
    sampleEntries.every(l => !l.parsed)
      ? sampleEntries.map(showBlankLine)
      : sampleEntries.map(showRawLine)
  ).join('\n');
}

function getTimestampSpec(sampleResponse: SampleResponse | null): TimestampSpec {
  if (!sampleResponse) return CONSTANT_TIMESTAMP_SPEC;

  const timestampSpecs = filterMap(
    getHeaderNamesFromSampleResponse(sampleResponse),
    sampleHeader => {
      const possibleFormat = possibleDruidFormatForValues(
        filterMap(sampleResponse.data, d => (d.parsed ? d.parsed[sampleHeader] : undefined)),
      );
      if (!possibleFormat) return;
      return {
        column: sampleHeader,
        format: possibleFormat,
      };
    },
  );

  return chooseByBestTimestamp(timestampSpecs) || CONSTANT_TIMESTAMP_SPEC;
}

function initializeSchemaWithSampleIfNeeded(
  spec: Partial<IngestionSpec>,
  sample: SampleResponse,
): Partial<IngestionSpec> {
  if (deepGet(spec, 'spec.dataSchema.dimensionsSpec')) return spec;
  return updateSchemaWithSample(spec, sample, 'fixed', 'multi-values', getRollup(spec, false));
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
  { name: 'Connect and parse raw data', steps: ['welcome', 'connect', 'parser'] },
  {
    name: 'Transform data and configure schema',
    steps: ['timestamp', 'transform', 'filter', 'schema'],
  },
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

export type LoadDataViewMode = 'all' | 'streaming' | 'batch';

export interface LoadDataViewProps {
  mode: LoadDataViewMode;
  initSupervisorId?: string;
  initTaskId?: string;
  goToSupervisor: (supervisorId: string) => void;
  openSupervisorSubmit: () => void;
  goToTasks: (taskGroupId: string) => void;
  openTaskSubmit: () => void;
}

interface SelectedIndex<T> {
  value: Partial<T>;
  index: number;
}

export interface LoadDataViewState {
  step: Step;
  spec: Partial<IngestionSpec>;
  nextSpec?: Partial<IngestionSpec>;
  cacheRows?: CacheRows;
  // dialogs / modals
  continueToSpec: boolean;
  showResetConfirm: boolean;
  newRollup?: boolean;
  newSchemaMode?: SchemaMode;
  newArrayMode?: ArrayMode;

  // welcome
  overlordModules?: string[];
  selectedComboType?: IngestionComboTypeWithExtra;

  // general
  sampleStrategy: SampleStrategy;
  columnFilter: string;
  specialColumnsOnly: boolean;
  unsavedChange: boolean;

  // for ioConfig
  inputQueryState: QueryState<SampleResponseWithExtraInfo>;

  // for parser
  parserQueryState: QueryState<SampleResponse>;

  // for flatten
  selectedFlattenField?: SelectedIndex<FlattenField>;

  // for timestamp
  timestampQueryState: QueryState<{
    sampleResponse: SampleResponse;
    spec: Partial<IngestionSpec>;
  }>;

  // for transform
  transformQueryState: QueryState<SampleResponse>;
  selectedTransform?: SelectedIndex<Transform>;

  // for filter
  filterQueryState: QueryState<SampleResponse>;
  selectedFilter?: SelectedIndex<DruidFilter>;

  // for schema
  schemaQueryState: QueryState<{
    sampleResponse: SampleResponse;
    dimensions: (string | DimensionSpec)[] | undefined;
    metricsSpec: MetricSpec[] | undefined;
    definedDimensions: boolean;
  }>;
  selectedAutoDimension?: string;
  selectedDimensionSpec?: SelectedIndex<DimensionSpec>;
  selectedMetricSpec?: SelectedIndex<MetricSpec>;

  // for final step
  existingDatasources?: string[];
  submitting: boolean;
}

export class LoadDataView extends React.PureComponent<LoadDataViewProps, LoadDataViewState> {
  static MODE_TO_KEY: Record<LoadDataViewMode, LocalStorageKeys> = {
    all: LocalStorageKeys.INGESTION_SPEC,
    streaming: LocalStorageKeys.STREAMING_INGESTION_SPEC,
    batch: LocalStorageKeys.BATCH_INGESTION_SPEC,
  };

  private readonly localStorageKey: LocalStorageKeys;

  constructor(props: LoadDataViewProps) {
    super(props);

    this.localStorageKey = LoadDataView.MODE_TO_KEY[props.mode];
    let spec = localStorageGetJson(this.localStorageKey);
    if (!spec || typeof spec !== 'object') spec = {};
    this.state = {
      step: 'loading',
      spec,

      // dialogs / modals
      showResetConfirm: false,
      continueToSpec: false,

      // general
      sampleStrategy: 'start',
      columnFilter: '',
      specialColumnsOnly: false,
      unsavedChange: false,

      // for inputSource
      inputQueryState: QueryState.INIT,

      // for parser
      parserQueryState: QueryState.INIT,

      // for timestamp
      timestampQueryState: QueryState.INIT,

      // for transform
      transformQueryState: QueryState.INIT,

      // for filter
      filterQueryState: QueryState.INIT,

      // for dimensions
      schemaQueryState: QueryState.INIT,

      // for final step
      submitting: false,
    };
  }

  componentDidMount(): void {
    const { initTaskId, initSupervisorId } = this.props;
    const { spec } = this.state;

    void this.getOverlordModules();
    if (initTaskId) {
      this.updateStep('loading');
      void this.getTaskJson();
    } else if (initSupervisorId) {
      this.updateStep('loading');
      void this.getSupervisorJson();
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
      overlordModules = await getProxyOverlordModules();
    } catch (e) {
      AppToaster.show({
        message: `Failed to get the list of loaded modules from the overlord: ${e.message}`,
        intent: Intent.DANGER,
      });
      this.setState({ overlordModules: undefined });
      return;
    }

    this.setState({ overlordModules });
  }

  isStepEnabled(step: Step): boolean {
    const { spec, cacheRows } = this.state;
    const druidSource = isDruidSource(spec);
    const ioConfig: IoConfig = deepGet(spec, 'spec.ioConfig') || EMPTY_OBJECT;

    switch (step) {
      case 'connect':
        return Boolean(spec.type);

      case 'parser':
        return Boolean(!druidSource && spec.type && !issueWithIoConfig(ioConfig));

      case 'timestamp':
        return Boolean(!druidSource && cacheRows && deepGet(spec, 'spec.dataSchema.timestampSpec'));

      case 'transform':
      case 'filter':
        return Boolean(cacheRows && deepGet(spec, 'spec.dataSchema.timestampSpec'));

      case 'schema':
      case 'partition':
      case 'tuning':
      case 'publish':
        return Boolean(
          cacheRows &&
            deepGet(spec, 'spec.dataSchema.timestampSpec') &&
            deepGet(spec, 'spec.dataSchema.dimensionsSpec'),
        );

      default:
        return true;
    }
  }

  private readonly handleDirty = () => {
    this.setState({ unsavedChange: true });
  };

  private readonly updateStep = (newStep: Step) => {
    const { unsavedChange, nextSpec } = this.state;
    if (unsavedChange || nextSpec) {
      AppToaster.show({
        message: `You have an unsaved change in this step.`,
        intent: Intent.WARNING,
        action: {
          icon: IconNames.TRASH,
          text: 'Discard change',
          onClick: this.resetSelected,
        },
      });
      return;
    }

    this.resetSelected();
    this.setState({ step: newStep });
  };

  private readonly updateSpec = (newSpec: Partial<IngestionSpec>) => {
    newSpec = normalizeSpec(newSpec);
    try {
      newSpec = upgradeSpec(newSpec);
    } catch (e) {
      const streaming = isStreamingSpec(newSpec);
      newSpec = {};
      AppToaster.show({
        icon: IconNames.ERROR,
        intent: Intent.DANGER,
        timeout: 30000,
        message: (
          <>
            <p>
              This spec can not be used in the data loader because it can not be auto-converted to
              the latest spec format:
            </p>
            <p>{e.message}</p>
            <p>You can still submit it directly form the Ingestion view.</p>
          </>
        ),
        action: {
          text: `Go to ${streaming ? 'Supervisors' : 'Tasks'} view`,
          onClick: streaming ? this.props.openSupervisorSubmit : this.props.openTaskSubmit,
        },
      });
    }
    const deltaState: Partial<LoadDataViewState> = { spec: newSpec };
    if (!deepGet(newSpec, 'spec.ioConfig.type')) {
      deltaState.cacheRows = undefined;
    }
    this.setState(deltaState as LoadDataViewState);
    localStorageSetJson(this.localStorageKey, newSpec);
  };

  private readonly updateSpecPreview = (newSpecPreview: Partial<IngestionSpec>) => {
    this.setState({ nextSpec: newSpecPreview });
  };

  private readonly applyPreviewSpec = () => {
    this.setState(({ spec, nextSpec }) => {
      if (nextSpec) {
        localStorageSetJson(this.localStorageKey, nextSpec);
      }
      return { spec: nextSpec ? nextSpec : { ...spec }, nextSpec: undefined }; // If applying again, make a shallow copy to force a refresh
    });
  };

  private getEffectiveSpec() {
    const { spec, nextSpec } = this.state;
    return nextSpec || spec;
  }

  private readonly resetSelected = () => {
    this.setState({
      nextSpec: undefined,
      selectedFlattenField: undefined,
      selectedTransform: undefined,
      selectedFilter: undefined,
      selectedAutoDimension: undefined,
      selectedDimensionSpec: undefined,
      selectedMetricSpec: undefined,
      unsavedChange: false,
    });
  };

  componentDidUpdate(_prevProps: LoadDataViewProps, prevState: LoadDataViewState) {
    const { spec, step } = this.state;
    const { spec: prevSpec, step: prevStep } = prevState;
    if (spec !== prevSpec || step !== prevStep) {
      this.doQueryForStep(step !== prevStep);
    }
  }

  doQueryForStep(initRun: boolean): any {
    const { step } = this.state;

    switch (step) {
      case 'connect':
        return this.queryForConnect(initRun);

      case 'parser':
        return this.queryForParser(initRun);

      case 'timestamp':
        return this.queryForTimestamp(initRun);

      case 'transform':
        return this.queryForTransform(initRun);

      case 'filter':
        return this.queryForFilter(initRun);

      case 'schema':
        return this.queryForSchema(initRun);

      case 'loading':
      case 'welcome':
      case 'partition':
      case 'publish':
      case 'tuning':
        return;

      case 'spec':
        return this.queryForSpec();
    }
  }

  renderActionCard(icon: IconName, title: string, caption: string, onClick: () => void) {
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

  render() {
    const { mode } = this.props;
    const { step, continueToSpec } = this.state;
    const type = mode === 'all' ? '' : `${mode} `;

    if (!continueToSpec) {
      return (
        <div className={classNames('load-data-continue-view load-data-view')}>
          {this.renderActionCard(
            IconNames.ASTERISK,
            `Start a new ${type}spec`,
            `Begin a new ${type}ingestion flow.`,
            this.handleResetSpec,
          )}
          {this.renderActionCard(
            IconNames.REPEAT,
            `Continue from previous ${type}spec`,
            `Go back to the most recent ${type}ingestion flow you were working on.`,
            this.handleContinueSpec,
          )}
        </div>
      );
    }

    return (
      <div className={classNames('load-data-view', 'app-view', step)}>
        {this.renderStepNav()}
        {step === 'loading' && <Loader />}

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

        {this.renderResetConfirm()}
      </div>
    );
  }

  renderApplyButtonBar(queryState: QueryState<unknown>, issue: string | undefined) {
    const { nextSpec } = this.state;
    const queryStateHasError = Boolean(queryState && queryState.error);

    return (
      <FormGroup className="control-buttons">
        {nextSpec && <Button text="Cancel" onClick={this.resetSelected} />}
        <Button
          text="Apply"
          disabled={(!nextSpec && !queryStateHasError) || Boolean(issue)}
          intent={Intent.PRIMARY}
          onClick={this.applyPreviewSpec}
        />
      </FormGroup>
    );
  }

  renderStepNav() {
    const { step } = this.state;

    return (
      <div className="step-nav">
        <div className="step-nav-inner">
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
      </div>
    );
  }

  renderNextBar(options: { nextStep?: Step; disabled?: boolean; onNextStep?: () => boolean }) {
    const { disabled, onNextStep } = options;
    const { step, nextSpec, unsavedChange } = this.state;
    const nextStep = options.nextStep || STEPS[STEPS.indexOf(step) + 1] || STEPS[0];

    return (
      <div className="next-bar">
        <Button
          text={`Next: ${VIEW_TITLE[nextStep]}`}
          rightIcon={IconNames.ARROW_RIGHT}
          intent={Intent.PRIMARY}
          disabled={Boolean(disabled || nextSpec || unsavedChange)}
          onClick={() => {
            if (onNextStep && !onNextStep()) return;

            this.updateStep(nextStep);
          }}
        />
      </div>
    );
  }

  // ==================================================================

  renderIngestionCard(
    comboType: IngestionComboTypeWithExtra,
    disabled?: boolean,
  ): JSX.Element | undefined {
    const { overlordModules, selectedComboType, spec } = this.state;
    const requiredModule = getRequiredModule(comboType);
    const goodToGo =
      !disabled &&
      (!requiredModule || !overlordModules || overlordModules.includes(requiredModule));

    return (
      <Card
        className={classNames('ingestion-card', {
          disabled: !goodToGo,
          active: selectedComboType === comboType,
        })}
        interactive
        elevation={1}
        onClick={e => {
          if (e.altKey && e.shiftKey) {
            this.updateSpec(updateIngestionType(spec, comboType as any));
            this.updateStep('connect');
          } else {
            this.setState({
              selectedComboType: selectedComboType !== comboType ? comboType : undefined,
            });
          }
        }}
      >
        <img
          src={UrlBaser.base(`/assets/${getIngestionImage(comboType)}.png`)}
          alt={`Ingestion tile for ${comboType}`}
        />
        <p>{getIngestionTitle(comboType)}</p>
      </Card>
    );
  }

  renderWelcomeStep() {
    const { mode } = this.props;
    const { spec } = this.state;

    const welcomeMessage = this.renderWelcomeStepMessage();
    return (
      <>
        <div className="main">
          <div className="ingestion-cards">
            {mode !== 'batch' && (
              <>
                {this.renderIngestionCard('kafka')}
                {this.renderIngestionCard('kinesis')}
                {this.renderIngestionCard('azure-event-hubs')}
              </>
            )}
            {mode !== 'streaming' && (
              <>
                {this.renderIngestionCard('index_parallel:s3')}
                {this.renderIngestionCard('index_parallel:azureStorage')}
                {this.renderIngestionCard('index_parallel:google')}
                {this.renderIngestionCard('index_parallel:delta')}
                {this.renderIngestionCard('index_parallel:hdfs')}
                {this.renderIngestionCard('index_parallel:druid')}
                {this.renderIngestionCard('index_parallel:http')}
                {this.renderIngestionCard('index_parallel:local')}
                {this.renderIngestionCard('index_parallel:inline')}
                {this.renderIngestionCard('example')}
              </>
            )}
            {this.renderIngestionCard('other')}
          </div>
        </div>
        <div className="control">
          {welcomeMessage && (
            <FormGroup>
              <Callout>{welcomeMessage}</Callout>
            </FormGroup>
          )}
          {this.renderWelcomeStepControls()}
          {!isEmptyIngestionSpec(spec) && (
            <Button icon={IconNames.RESET} text="Reset spec" onClick={this.handleResetConfirm} />
          )}
        </div>
      </>
    );
  }

  renderWelcomeStepMessage(): JSX.Element | undefined {
    const { selectedComboType } = this.state;

    if (!selectedComboType) {
      return <p>Please specify where your raw data is located.</p>;
    }

    const issue = this.selectedIngestionTypeIssue();
    if (issue) return issue;

    switch (selectedComboType) {
      case 'index_parallel:http':
        return (
          <>
            <p>Load data accessible through HTTP(s).</p>
            <p>
              Data must be in text, orc, or parquet format and the HTTP(s) endpoint must be
              reachable by every Druid process in the cluster.
            </p>
          </>
        );

      case 'index_parallel:local':
        return (
          <>
            <p>
              <em>Recommended only in single server deployments.</em>
            </p>
            <p>Load data directly from a local file.</p>
            <p>
              Files must be in text, orc, or parquet format and must be accessible to all the Druid
              processes in the cluster.
            </p>
          </>
        );

      case 'index_parallel:delta':
        return (
          <>
            <p>Load data from Delta Lake.</p>
            <p>Data must be stored in the Delta Lake format.</p>
          </>
        );

      case 'index_parallel:druid':
        return (
          <>
            <p>Reindex data from existing Druid segments.</p>
            <p>
              Reindexing data allows you to filter rows, add, transform, and delete columns, as well
              as change the partitioning of the data.
            </p>
          </>
        );

      case 'index_parallel:inline':
        return (
          <>
            <p>Ingest a small amount of data directly from the clipboard.</p>
          </>
        );

      case 'index_parallel:s3':
        return <p>Load text based, orc, or parquet data from Amazon S3.</p>;

      case 'index_parallel:azureStorage':
        return <p>Load text based, orc, or parquet data from Azure.</p>;

      case 'index_parallel:google':
        return <p>Load text based, orc, or parquet data from the Google Blobstore.</p>;

      case 'index_parallel:hdfs':
        return <p>Load text based, orc, or parquet data from HDFS.</p>;

      case 'kafka':
        return <p>Load streaming data in real-time from Apache Kafka.</p>;

      case 'kinesis':
        return <p>Load streaming data in real-time from Amazon Kinesis.</p>;

      case 'azure-event-hubs':
        return (
          <>
            <p>Azure Event Hubs provides an Apache Kafka compatible API for consuming data.</p>
            <p>
              Data from an Event Hub can be streamed into Druid by enabling the Kafka API on the
              Namespace.
            </p>
            <p>
              Please see the{' '}
              <ExternalLink href="https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-for-kafka-ecosystem-overview">
                Event Hub documentation
              </ExternalLink>{' '}
              for more information.
            </p>
          </>
        );

      case 'example':
        return; // Yield to example picker controls

      case 'other':
        return (
          <p>
            If you do not see your source of raw data here, you can try to ingest it by submitting a{' '}
            <ExternalLink href={`${getLink('DOCS')}/ingestion/index.html`}>
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
    const { openSupervisorSubmit, openTaskSubmit } = this.props;
    const { spec, selectedComboType } = this.state;

    const issue = this.selectedIngestionTypeIssue();
    if (issue) return;

    switch (selectedComboType) {
      case 'index_parallel:http':
      case 'index_parallel:local':
      case 'index_parallel:druid':
      case 'index_parallel:inline':
      case 'index_parallel:s3':
      case 'index_parallel:azureStorage':
      case 'index_parallel:google':
      case 'index_parallel:delta':
      case 'index_parallel:hdfs':
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
                this.updateStep('connect');
              }}
            />
          </FormGroup>
        );

      case 'azure-event-hubs':
        return (
          <>
            <FormGroup>
              <Callout intent={Intent.WARNING}>
                Please review and fill in the <Code>consumerProperties</Code> on the next step.
              </Callout>
            </FormGroup>
            <FormGroup>
              <Button
                text="Connect via Kafka API"
                rightIcon={IconNames.ARROW_RIGHT}
                intent={Intent.PRIMARY}
                onClick={() => {
                  // Use the kafka ingestion type but preset some consumerProperties required for Event Hubs
                  let newSpec = updateIngestionType(spec, 'kafka');
                  newSpec = deepSet(
                    newSpec,
                    'spec.ioConfig.consumerProperties.{security.protocol}',
                    'SASL_SSL',
                  );
                  newSpec = deepSet(
                    newSpec,
                    'spec.ioConfig.consumerProperties.{sasl.mechanism}',
                    'PLAIN',
                  );
                  newSpec = deepSet(
                    newSpec,
                    'spec.ioConfig.consumerProperties.{sasl.jaas.config}',
                    `org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="Value of 'Connection string-primary key' in the Azure UI";`,
                  );
                  this.updateSpec(newSpec);
                  this.updateStep('connect');
                }}
              />
            </FormGroup>
          </>
        );

      case 'example':
        return (
          <ExamplePicker
            exampleSpecs={EXAMPLE_SPECS}
            onSelectExample={exampleSpec => {
              this.updateSpec(exampleSpec.spec);
              this.updateStep('connect');
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
                onClick={openSupervisorSubmit}
              />
            </FormGroup>
            <FormGroup>
              <Button
                text="Submit task"
                rightIcon={IconNames.ARROW_RIGHT}
                intent={Intent.PRIMARY}
                onClick={openTaskSubmit}
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
          <Code>&quot;{requiredModule}&quot;</Code> extension is included in the{' '}
          <Code>druid.extensions.loadList</Code>.
        </p>
        <p>
          For more information please refer to the{' '}
          <ExternalLink href={`${getLink('DOCS')}/operations/including-extensions`}>
            documentation on loading extensions
          </ExternalLink>
          .
        </p>
      </>
    );
  }

  private readonly handleResetConfirm = () => {
    this.setState({ showResetConfirm: true });
  };

  private readonly handleResetSpec = () => {
    this.setState({ showResetConfirm: false, continueToSpec: true });
    this.updateSpec({});
    this.updateStep('welcome');
  };

  private readonly handleContinueSpec = () => {
    this.setState({ continueToSpec: true });
  };

  renderResetConfirm(): JSX.Element | undefined {
    const { showResetConfirm } = this.state;
    if (!showResetConfirm) return;

    return (
      <AlertDialog
        cancelButtonText="Cancel"
        confirmButtonText="Reset spec"
        icon="trash"
        intent={Intent.DANGER}
        isOpen
        onCancel={() => this.setState({ showResetConfirm: false })}
        onConfirm={this.handleResetSpec}
      >
        <p>This will discard the current progress in the spec.</p>
      </AlertDialog>
    );
  }

  // ==================================================================

  async queryForConnect(initRun = false) {
    const { spec, sampleStrategy } = this.state;
    const ioConfig: IoConfig = deepGet(spec, 'spec.ioConfig') || EMPTY_OBJECT;

    let issue: string | undefined;
    if (issueWithIoConfig(ioConfig, true)) {
      issue = `IoConfig not ready, ${issueWithIoConfig(ioConfig)}`;
    }

    if (issue) {
      this.setState({
        inputQueryState: initRun ? QueryState.INIT : new QueryState({ error: new Error(issue) }),
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
        inputQueryState: new QueryState({ error: e }),
      });
      return;
    }

    const deltaState: Partial<LoadDataViewState> = {
      inputQueryState: new QueryState({ data: sampleResponse }),
    };
    if (isDruidSource(spec)) {
      deltaState.cacheRows = getCacheRowsFromSampleResponse(sampleResponse);
    }
    this.setState(deltaState as LoadDataViewState);
  }

  renderConnectStep() {
    const { inputQueryState, sampleStrategy } = this.state;
    const spec = this.getEffectiveSpec();
    const ioConfig: IoConfig = deepGet(spec, 'spec.ioConfig') || EMPTY_OBJECT;
    const inlineMode = deepGet(spec, 'spec.ioConfig.inputSource.type') === 'inline';
    const druidSource = isDruidSource(spec);
    const kafkaSource = getSpecType(spec) === 'kafka';

    let mainFill: JSX.Element | string;
    if (inlineMode) {
      mainFill = (
        <TextArea
          className="inline-data"
          placeholder="Paste your data here"
          value={deepGet(spec, 'spec.ioConfig.inputSource.data')}
          onChange={(e: any) => {
            const stringValue = e.target.value.slice(0, MAX_INLINE_DATA_LENGTH);
            this.updateSpecPreview(deepSet(spec, 'spec.ioConfig.inputSource.data', stringValue));
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
    } else {
      const data = inputQueryState.getSomeData();
      const inputData = data ? data.data : undefined;

      mainFill = (
        <>
          {inputData && (
            <TextArea
              className="raw-lines"
              readOnly
              value={formatSampleEntries(inputData, druidSource, kafkaSource)}
            />
          )}
          {inputQueryState.isLoading() && <Loader />}
          {inputQueryState.error && (
            <CenterMessage>{inputQueryState.getErrorMessage()}</CenterMessage>
          )}
        </>
      );
    }

    const ingestionComboType = getIngestionComboType(spec);
    const ioConfigFields = ingestionComboType
      ? getIoConfigFormFields(ingestionComboType)
      : undefined;

    return (
      <>
        <div className="main">{mainFill}</div>
        <div className="control">
          <ConnectMessage inlineMode={inlineMode} spec={spec} />
          {ioConfigFields ? (
            <>
              <AutoForm
                fields={ioConfigFields}
                model={ioConfig}
                onChange={c => this.updateSpecPreview(deepSet(spec, 'spec.ioConfig', c))}
              />
              {deepGet(spec, 'spec.ioConfig.inputSource.properties.secretAccessKey.password') && (
                <FormGroup>
                  <Callout intent={Intent.WARNING}>
                    This key will be visible to anyone accessing this console and may appear in
                    server logs. For production scenarios, use of a more secure secret key type is
                    strongly recommended.
                  </Callout>
                </FormGroup>
              )}
            </>
          ) : (
            <FormGroup label="IO Config">
              <JsonInput
                value={ioConfig}
                onChange={c => this.updateSpecPreview(deepSet(spec, 'spec.ioConfig', c))}
                height="300px"
              />
            </FormGroup>
          )}
          {deepGet(spec, 'spec.ioConfig.inputSource.type') === 'local' && (
            <FormGroup>
              <Callout intent={Intent.WARNING}>
                This path must be available on the local filesystem of all Druid services.
              </Callout>
            </FormGroup>
          )}
          {isStreamingSpec(spec) && (
            <FormGroup label="Where should the data be sampled from?">
              <RadioGroup
                selectedValue={sampleStrategy}
                onChange={e => this.setState({ sampleStrategy: e.currentTarget.value as any })}
              >
                <Radio value="start">Start of stream</Radio>
                <Radio value="end">End of stream</Radio>
              </RadioGroup>
            </FormGroup>
          )}
          {this.renderApplyButtonBar(
            inputQueryState,
            ioConfigFields ? AutoForm.issueWithModel(ioConfig, ioConfigFields) : undefined,
          )}
        </div>
        {this.renderNextBar({
          disabled: !inputQueryState.data,
          nextStep: druidSource ? 'transform' : 'parser',
          onNextStep: () => {
            if (!inputQueryState.data) return false;
            const inputData = inputQueryState.data;

            if (druidSource) {
              let newSpec = deepSet(spec, 'spec.dataSchema.timestampSpec', {
                column: TIME_COLUMN,
                format: 'millis',
              });

              if (typeof inputData.rollup === 'boolean') {
                newSpec = deepSet(
                  newSpec,
                  'spec.dataSchema.granularitySpec.rollup',
                  inputData.rollup,
                );
              }

              newSpec = deepSet(
                newSpec,
                'spec.dataSchema.granularitySpec.queryGranularity',
                'none',
              );

              if (inputData.columns) {
                const aggregators = inputData.aggregators || {};
                newSpec = deepSet(
                  newSpec,
                  'spec.dataSchema.dimensionsSpec.dimensions',
                  filterMap(inputData.columns, column => {
                    if (column === TIME_COLUMN || aggregators[column]) return;
                    return {
                      name: column,
                      type: String(inputData.columnInfo?.[column]?.type || 'string').toLowerCase(),
                    };
                  }),
                );

                if (inputData.aggregators) {
                  newSpec = deepSet(
                    newSpec,
                    'spec.dataSchema.metricsSpec',
                    filterMap(inputData.columns, column => aggregators[column]),
                  );
                }
              }

              this.updateSpec(fillDataSourceNameIfNeeded(newSpec));
            } else {
              const issue = issueWithSampleData(
                filterMap(inputData.data, l => l.input?.raw),
                isStreamingSpec(spec),
              );
              if (issue) {
                AppToaster.show({
                  icon: IconNames.WARNING_SIGN,
                  intent: Intent.WARNING,
                  message: issue,
                  timeout: 30000,
                });
                return false;
              }

              this.updateSpec(fillDataSourceNameIfNeeded(fillInputFormatIfNeeded(spec, inputData)));
            }
            return true;
          },
        })}
      </>
    );
  }

  // ==================================================================

  async queryForParser(initRun = false) {
    const { spec, sampleStrategy } = this.state;
    const ioConfig: IoConfig = deepGet(spec, 'spec.ioConfig') || EMPTY_OBJECT;

    let issue: string | undefined;
    if (issueWithIoConfig(ioConfig)) {
      issue = `IoConfig not ready, ${issueWithIoConfig(ioConfig)}`;
    }

    if (issue) {
      this.setState(({ parserQueryState }) => ({
        parserQueryState: initRun
          ? QueryState.INIT
          : new QueryState({ error: new Error(issue), lastData: parserQueryState.getSomeData() }),
      }));
      return;
    }

    this.setState(({ parserQueryState }) => ({
      parserQueryState: new QueryState({ loading: true, lastData: parserQueryState.getSomeData() }),
    }));

    let sampleResponse: SampleResponse;
    try {
      sampleResponse = await sampleForParser(spec, sampleStrategy);
    } catch (e) {
      this.setState(({ parserQueryState }) => ({
        parserQueryState: new QueryState({ error: e, lastData: parserQueryState.getSomeData() }),
      }));
      return;
    }

    this.setState(({ parserQueryState }) => ({
      cacheRows: getCacheRowsFromSampleResponse(sampleResponse),
      parserQueryState: new QueryState({
        data: sampleResponse,
        lastData: parserQueryState.getSomeData(),
      }),
    }));
  }

  renderParserStep() {
    const { columnFilter, specialColumnsOnly, parserQueryState, selectedFlattenField } = this.state;
    const spec = this.getEffectiveSpec();
    const inputFormat: InputFormat = deepGet(spec, 'spec.ioConfig.inputFormat') || EMPTY_OBJECT;
    const flattenFields: FlattenField[] = getFlattenSpec(spec)?.fields || EMPTY_ARRAY;

    const canHaveNestedData = inputFormatCanProduceNestedData(inputFormat);

    let mainFill: JSX.Element | string;
    if (parserQueryState.isInit()) {
      mainFill = (
        <CenterMessage>
          Please enter the parser details on the right <Icon icon={IconNames.ARROW_RIGHT} />
        </CenterMessage>
      );
    } else {
      const data = parserQueryState.getSomeData();
      mainFill = (
        <div className="table-with-control">
          <div className="table-control">
            <ClearableInput
              value={columnFilter}
              onChange={columnFilter => this.setState({ columnFilter })}
              placeholder="Search columns"
            />
            {canHaveNestedData && (
              <Switch
                checked={specialColumnsOnly}
                label="Flattened columns only"
                onChange={() => this.setState({ specialColumnsOnly: !specialColumnsOnly })}
                disabled={!flattenFields.length}
              />
            )}
          </div>
          {data && (
            <ParseDataTable
              sampleResponse={data}
              columnFilter={columnFilter}
              canFlatten={canHaveNestedData}
              flattenedColumnsOnly={specialColumnsOnly}
              flattenFields={flattenFields}
              onFlattenFieldSelect={this.onFlattenFieldSelect}
            />
          )}
          {parserQueryState.isLoading() && <Loader />}
          {parserQueryState.error && (
            <CenterMessage>{parserQueryState.getErrorMessage()}</CenterMessage>
          )}
        </div>
      );
    }

    let suggestedFlattenFields: FlattenField[] | undefined;
    if (canHaveNestedData && !flattenFields.length && parserQueryState.data) {
      suggestedFlattenFields = computeFlattenPathsForData(
        filterMap(parserQueryState.data.data, r => r.input),
        'ignore-arrays',
      );
    }

    const specType = getSpecType(spec);
    const inputFormatFields = isStreamingSpec(spec)
      ? STREAMING_INPUT_FORMAT_FIELDS
      : BATCH_INPUT_FORMAT_FIELDS;

    const possibleSystemFields = getPossibleSystemFieldsForSpec(spec);

    const normalInputAutoForm = (
      <AutoForm
        fields={inputFormatFields}
        model={inputFormat}
        onChange={p => this.updateSpecPreview(deepSet(spec, 'spec.ioConfig.inputFormat', p))}
      />
    );

    return (
      <>
        <div className="main">{mainFill}</div>
        <div className="control">
          <ParserMessage />
          {!selectedFlattenField && (
            <>
              {specType !== 'kafka' ? (
                normalInputAutoForm
              ) : (
                <>
                  {inputFormat?.type !== 'kafka' ? (
                    normalInputAutoForm
                  ) : (
                    <AutoForm
                      fields={inputFormatFields}
                      model={inputFormat?.valueFormat}
                      onChange={p =>
                        this.updateSpecPreview(
                          deepSet(spec, 'spec.ioConfig.inputFormat.valueFormat', p),
                        )
                      }
                    />
                  )}
                  <FormGroup className="parse-metadata">
                    <Switch
                      label="Parse Kafka metadata (ts, headers, key)"
                      checked={inputFormat?.type === 'kafka'}
                      onChange={() => {
                        this.updateSpecPreview(
                          inputFormat?.type === 'kafka'
                            ? deepMove(
                                spec,
                                'spec.ioConfig.inputFormat.valueFormat',
                                'spec.ioConfig.inputFormat',
                              )
                            : deepSet(spec, 'spec.ioConfig.inputFormat', {
                                type: 'kafka',
                                valueFormat: inputFormat,
                              }),
                        );
                      }}
                    />
                  </FormGroup>
                  {inputFormat?.type === 'kafka' && (
                    <AutoForm
                      fields={KAFKA_METADATA_INPUT_FORMAT_FIELDS}
                      model={inputFormat}
                      onChange={p =>
                        this.updateSpecPreview(deepSet(spec, 'spec.ioConfig.inputFormat', p))
                      }
                    />
                  )}
                </>
              )}
              {possibleSystemFields.length > 0 && (
                <AutoForm
                  fields={[
                    {
                      name: 'spec.ioConfig.inputSource.systemFields',
                      label: 'System fields',
                      type: 'string-array',
                      suggestions: possibleSystemFields,
                      info: 'JSON array of system fields to return as part of input rows.',
                    },
                  ]}
                  model={spec}
                  onChange={this.updateSpecPreview}
                />
              )}
              {this.renderApplyButtonBar(
                parserQueryState,
                AutoForm.issueWithModel(inputFormat, inputFormatFields) ||
                  (inputFormat?.type === 'kafka'
                    ? AutoForm.issueWithModel(inputFormat, KAFKA_METADATA_INPUT_FORMAT_FIELDS)
                    : undefined),
              )}
            </>
          )}
          {canHaveNestedData && this.renderFlattenControls()}
          {suggestedFlattenFields && suggestedFlattenFields.length ? (
            <FormGroup>
              <Button
                icon={IconNames.LIGHTBULB}
                text={`Auto add ${pluralIfNeeded(suggestedFlattenFields.length, 'flatten spec')}`}
                onClick={() => {
                  this.updateSpec(changeFlattenSpec(spec, { fields: suggestedFlattenFields }));
                }}
              />
            </FormGroup>
          ) : undefined}
        </div>
        {this.renderNextBar({
          disabled: !parserQueryState.data,
          onNextStep: () => {
            if (!parserQueryState.data) return false;
            let possibleTimestampSpec: TimestampSpec;
            if (isDruidSource(spec)) {
              possibleTimestampSpec = {
                column: TIME_COLUMN,
                format: 'auto',
              };
            } else {
              possibleTimestampSpec = getTimestampSpec(parserQueryState.data);
            }

            if (possibleTimestampSpec) {
              const newSpec = deepSet(spec, 'spec.dataSchema.timestampSpec', possibleTimestampSpec);
              this.updateSpec(newSpec);
            }

            return true;
          },
        })}
      </>
    );
  }

  private readonly onFlattenFieldSelect = (field: FlattenField, index: number) => {
    const { spec, unsavedChange } = this.state;
    const inputFormat: InputFormat = deepGet(spec, 'spec.ioConfig.inputFormat') || EMPTY_OBJECT;
    if (unsavedChange || !inputFormatCanProduceNestedData(inputFormat)) return;

    this.setState({
      selectedFlattenField: { value: field, index },
    });
  };

  renderFlattenControls(): JSX.Element | undefined {
    const { spec, nextSpec, selectedFlattenField } = this.state;

    if (selectedFlattenField) {
      return (
        <FormEditor
          key={selectedFlattenField.index}
          fields={FLATTEN_FIELD_FIELDS}
          initValue={selectedFlattenField.value}
          onClose={this.resetSelected}
          onDirty={this.handleDirty}
          onApply={flattenField => {
            const flattenSpec = getFlattenSpec(spec) || {};
            this.updateSpec(
              changeFlattenSpec(
                spec,
                deepSet(flattenSpec, `fields.${selectedFlattenField.index}`, flattenField),
              ),
            );
          }}
          showDelete={selectedFlattenField.index !== -1}
          onDelete={() => {
            const flattenSpec = getFlattenSpec(spec) || {};
            this.updateSpec(
              changeFlattenSpec(
                spec,
                deepDelete(flattenSpec, `fields.${selectedFlattenField.index}`),
              ),
            );
          }}
        />
      );
    } else {
      return (
        <FormGroup>
          <Button
            text="Add column flattening"
            disabled={Boolean(nextSpec)}
            onClick={() => {
              this.setState({
                selectedFlattenField: { value: { type: 'path' }, index: -1 },
              });
            }}
          />
          <AnchorButton
            icon={IconNames.INFO_SIGN}
            href={`${getLink('DOCS')}/ingestion/data-formats.html#flattenspec`}
            target="_blank"
            minimal
          />
        </FormGroup>
      );
    }
  }

  // ==================================================================

  async queryForTimestamp(initRun = false) {
    const { spec, cacheRows } = this.state;

    if (!cacheRows) {
      this.setState(({ timestampQueryState }) => ({
        timestampQueryState: initRun
          ? QueryState.INIT
          : new QueryState({
              error: new Error('must complete parse step'),
              lastData: timestampQueryState.getSomeData(),
            }),
      }));
      return;
    }

    this.setState(({ timestampQueryState }) => ({
      timestampQueryState: new QueryState({
        loading: true,
        lastData: timestampQueryState.getSomeData(),
      }),
    }));

    let sampleResponse: SampleResponse;
    try {
      sampleResponse = await sampleForTimestamp(spec, cacheRows);
    } catch (e) {
      this.setState(({ timestampQueryState }) => ({
        timestampQueryState: new QueryState({
          error: e,
          lastData: timestampQueryState.getSomeData(),
        }),
      }));
      return;
    }

    this.setState(({ timestampQueryState }) => ({
      timestampQueryState: new QueryState({
        data: {
          sampleResponse,
          spec,
        },
        lastData: timestampQueryState.getSomeData(),
      }),
    }));
  }

  renderTimestampStep() {
    const { columnFilter, specialColumnsOnly, timestampQueryState } = this.state;
    const spec = this.getEffectiveSpec();
    const timestampSchema = getTimestampSchema(spec);
    const timestampSpec: TimestampSpec =
      deepGet(spec, 'spec.dataSchema.timestampSpec') || EMPTY_OBJECT;
    const transforms: Transform[] =
      deepGet(spec, 'spec.dataSchema.transformSpec.transforms') || EMPTY_ARRAY;

    let mainFill: JSX.Element | string;
    if (timestampQueryState.isInit()) {
      mainFill = (
        <CenterMessage>
          Please enter the timestamp column details on the right{' '}
          <Icon icon={IconNames.ARROW_RIGHT} />
        </CenterMessage>
      );
    } else {
      const data = timestampQueryState.getSomeData();
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
          {data && (
            <ParseTimeTable
              sampleBundle={data}
              columnFilter={columnFilter}
              possibleTimestampColumnsOnly={specialColumnsOnly}
              selectedColumnName={parseTimeTableSelectedColumnName(
                data.sampleResponse,
                timestampSpec,
              )}
              onTimestampColumnSelect={this.onTimestampColumnSelect}
            />
          )}
          {timestampQueryState.isLoading() && <Loader />}
          {timestampQueryState.error && (
            <CenterMessage>{timestampQueryState.getErrorMessage()}</CenterMessage>
          )}
        </div>
      );
    }

    return (
      <>
        <div className="main">{mainFill}</div>
        <div className="control">
          <TimestampMessage />
          <FormGroup label="Parse timestamp from">
            <ButtonGroup>
              <Button
                text="None"
                active={timestampSchema === 'none'}
                onClick={() => {
                  this.updateSpecPreview(
                    deepSetMulti(spec, {
                      'spec.dataSchema.timestampSpec': CONSTANT_TIMESTAMP_SPEC,
                      'spec.dataSchema.transformSpec.transforms':
                        removeTimestampTransform(transforms),
                    }),
                  );
                }}
              />
              <Button
                text="Column"
                active={timestampSchema === 'column'}
                onClick={() => {
                  const timestampSpec = {
                    column: 'timestamp',
                    format: 'auto',
                  };
                  this.updateSpecPreview(
                    deepSetMulti(spec, {
                      'spec.dataSchema.timestampSpec': timestampSpec,
                      'spec.dataSchema.transformSpec.transforms':
                        removeTimestampTransform(transforms),
                    }),
                  );
                }}
              />
              <Button
                text="Expression"
                active={timestampSchema === 'expression'}
                onClick={() => {
                  this.updateSpecPreview(
                    deepSetMulti(spec, {
                      'spec.dataSchema.timestampSpec': CONSTANT_TIMESTAMP_SPEC,
                      'spec.dataSchema.transformSpec.transforms': addTimestampTransform(transforms),
                    }),
                  );
                }}
              />
            </ButtonGroup>
          </FormGroup>
          {timestampSchema === 'expression' ? (
            <AutoForm
              fields={getTimestampExpressionFields(transforms)}
              model={transforms}
              onChange={transforms => {
                this.updateSpecPreview(
                  deepSet(spec, 'spec.dataSchema.transformSpec.transforms', transforms),
                );
              }}
            />
          ) : (
            <AutoForm
              fields={
                timestampSchema === 'none' ? CONSTANT_TIMESTAMP_SPEC_FIELDS : TIMESTAMP_SPEC_FIELDS
              }
              model={timestampSpec}
              onChange={timestampSpec => {
                this.updateSpecPreview(
                  deepSet(spec, 'spec.dataSchema.timestampSpec', timestampSpec),
                );
              }}
            />
          )}
          {this.renderApplyButtonBar(timestampQueryState, undefined)}
        </div>
        {this.renderNextBar({
          disabled: !timestampQueryState.data,
        })}
      </>
    );
  }

  private readonly onTimestampColumnSelect = (newTimestampSpec: TimestampSpec) => {
    const spec = this.getEffectiveSpec();
    this.updateSpecPreview(deepSet(spec, 'spec.dataSchema.timestampSpec', newTimestampSpec));
  };

  // ==================================================================

  async queryForTransform(initRun = false) {
    const { spec, cacheRows } = this.state;

    if (!cacheRows) {
      this.setState(({ transformQueryState }) => ({
        transformQueryState: initRun
          ? QueryState.INIT
          : new QueryState({
              error: new Error('must complete parse step'),
              lastData: transformQueryState.getSomeData(),
            }),
      }));
      return;
    }

    this.setState(({ transformQueryState }) => ({
      transformQueryState: new QueryState({
        loading: true,
        lastData: transformQueryState.getSomeData(),
      }),
    }));

    let sampleResponse: SampleResponse;
    try {
      sampleResponse = await sampleForTransform(spec, cacheRows);
    } catch (e) {
      this.setState(({ transformQueryState }) => ({
        transformQueryState: new QueryState({
          error: e,
          lastData: transformQueryState.getSomeData(),
        }),
      }));
      return;
    }

    this.setState(({ transformQueryState }) => ({
      transformQueryState: new QueryState({
        data: sampleResponse,
        lastData: transformQueryState.getSomeData(),
      }),
    }));
  }

  renderTransformStep() {
    const { spec, columnFilter, specialColumnsOnly, transformQueryState, selectedTransform } =
      this.state;
    const transforms: Transform[] =
      deepGet(spec, 'spec.dataSchema.transformSpec.transforms') || EMPTY_ARRAY;

    let mainFill: JSX.Element | string;
    if (transformQueryState.isInit()) {
      mainFill = <CenterMessage>Please fill in the previous steps</CenterMessage>;
    } else {
      const sampleResponse = transformQueryState.getSomeData();

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
          {sampleResponse && (
            <TransformTable
              sampleResponse={sampleResponse}
              columnFilter={columnFilter}
              transformedColumnsOnly={specialColumnsOnly}
              transforms={transforms}
              selectedColumnName={transformTableSelectedColumnName(
                sampleResponse,
                selectedTransform?.value,
              )}
              onTransformSelect={this.onTransformSelect}
            />
          )}
          {transformQueryState.isLoading() && <Loader />}
          {transformQueryState.error && (
            <CenterMessage>{transformQueryState.getErrorMessage()}</CenterMessage>
          )}
        </div>
      );
    }

    return (
      <>
        <div className="main">{mainFill}</div>
        <div className="control">
          <TransformMessage />
          {Boolean(transformQueryState.error && transforms.length) && (
            <FormGroup>
              <Button
                icon={IconNames.EDIT}
                text="Edit last added transform"
                intent={Intent.PRIMARY}
                onClick={() => {
                  this.setState({
                    selectedTransform: {
                      value: transforms[transforms.length - 1],
                      index: transforms.length - 1,
                    },
                  });
                }}
              />
            </FormGroup>
          )}
          {this.renderTransformControls()}
        </div>
        {this.renderNextBar({
          disabled: !transformQueryState.data,
          onNextStep: () => {
            if (!transformQueryState.data) return false;
            this.updateSpec(initializeSchemaWithSampleIfNeeded(spec, transformQueryState.data));
            return true;
          },
        })}
      </>
    );
  }

  private readonly onTransformSelect = (transform: Partial<Transform>, index: number) => {
    const { unsavedChange } = this.state;
    if (unsavedChange) return;

    this.setState({
      selectedTransform: { value: transform, index },
    });
  };

  renderTransformControls() {
    const { spec, selectedTransform } = this.state;

    if (selectedTransform) {
      return (
        <FormEditor
          key={selectedTransform.index}
          fields={TRANSFORM_FIELDS}
          initValue={selectedTransform.value}
          onClose={this.resetSelected}
          onDirty={this.handleDirty}
          onApply={transform =>
            this.updateSpec(
              deepSet(
                spec,
                `spec.dataSchema.transformSpec.transforms.${selectedTransform.index}`,
                transform,
              ),
            )
          }
          showDelete={selectedTransform.index !== -1}
          onDelete={() =>
            this.updateSpec(
              deepDelete(
                spec,
                `spec.dataSchema.transformSpec.transforms.${selectedTransform.index}`,
              ),
            )
          }
        />
      );
    } else {
      return (
        <FormGroup>
          <Button
            text="Add column transform"
            onClick={() => {
              this.onTransformSelect({ type: 'expression' }, -1);
            }}
          />
        </FormGroup>
      );
    }
  }

  // ==================================================================

  async queryForFilter(initRun = false) {
    const { spec, cacheRows } = this.state;

    if (!cacheRows) {
      this.setState(({ filterQueryState }) => ({
        filterQueryState: initRun
          ? QueryState.INIT
          : new QueryState({
              error: new Error('must complete parse step'),
              lastData: filterQueryState.getSomeData(),
            }),
      }));
      return;
    }

    this.setState(({ filterQueryState }) => ({
      filterQueryState: new QueryState({ loading: true, lastData: filterQueryState.getSomeData() }),
    }));

    let sampleResponse: SampleResponse;
    try {
      sampleResponse = await sampleForFilter(spec, cacheRows);
    } catch (e) {
      this.setState(({ filterQueryState }) => ({
        filterQueryState: new QueryState({ error: e, lastData: filterQueryState.getSomeData() }),
      }));
      return;
    }

    if (sampleResponse.data.length) {
      this.setState(({ filterQueryState }) => ({
        filterQueryState: new QueryState({
          data: sampleResponse,
          lastData: filterQueryState.getSomeData(),
        }),
      }));
      return;
    }

    // The filters matched no data
    let sampleResponseNoFilter: SampleResponse;
    try {
      const specNoFilter = deepSet(spec, 'spec.dataSchema.transformSpec.filter', null);
      sampleResponseNoFilter = await sampleForFilter(specNoFilter, cacheRows);
    } catch (e) {
      this.setState(({ filterQueryState }) => ({
        filterQueryState: new QueryState({ error: e, lastData: filterQueryState.getSomeData() }),
      }));
      return;
    }

    this.setState(({ filterQueryState }) => ({
      // cacheRows: sampleResponseNoFilter.cacheKey,
      filterQueryState: new QueryState({
        data: sampleResponseNoFilter,
        lastData: filterQueryState.getSomeData(),
      }),
    }));
  }

  private readonly getMemoizedDimensionFiltersFromSpec = memoize(spec => {
    const { dimensionFilters } = splitFilter(deepGet(spec, 'spec.dataSchema.transformSpec.filter'));
    return dimensionFilters;
  });

  renderFilterStep() {
    const { columnFilter, filterQueryState, selectedFilter } = this.state;
    const spec = this.getEffectiveSpec();
    const dimensionFilters = this.getMemoizedDimensionFiltersFromSpec(spec);

    let mainFill: JSX.Element | string;
    if (filterQueryState.isInit()) {
      mainFill = <CenterMessage>Please enter more details for the previous steps</CenterMessage>;
    } else {
      const filterQuery = filterQueryState.getSomeData();

      mainFill = (
        <div className="table-with-control">
          <div className="table-control">
            <ClearableInput
              value={columnFilter}
              onChange={columnFilter => this.setState({ columnFilter })}
              placeholder="Search columns"
            />
          </div>
          {filterQuery && (
            <FilterTable
              sampleResponse={filterQuery}
              columnFilter={columnFilter}
              dimensionFilters={dimensionFilters}
              selectedFilterName={filterTableSelectedColumnName(filterQuery, selectedFilter?.value)}
              onFilterSelect={this.onFilterSelect}
            />
          )}
          {filterQueryState.isLoading() && <Loader />}
          {filterQueryState.error && (
            <CenterMessage>{filterQueryState.getErrorMessage()}</CenterMessage>
          )}
        </div>
      );
    }

    return (
      <>
        <div className="main">{mainFill}</div>
        <div className="control">
          <FilterMessage />
          {!selectedFilter && (
            <>
              <AutoForm fields={FILTERS_FIELDS} model={spec} onChange={this.updateSpecPreview} />
              {this.renderApplyButtonBar(filterQueryState, undefined)}
              <FormGroup>
                <Button
                  text="Add column filter"
                  onClick={() => {
                    this.setState({
                      selectedFilter: { value: { type: 'selector' }, index: -1 },
                    });
                  }}
                />
              </FormGroup>
            </>
          )}
          {this.renderColumnFilterControls()}
        </div>
        {this.renderNextBar({
          onNextStep: () => {
            if (!filterQueryState.data) return false;
            this.updateSpec(initializeSchemaWithSampleIfNeeded(spec, filterQueryState.data));
            return true;
          },
        })}
      </>
    );
  }

  private readonly onFilterSelect = (filter: DruidFilter, index: number) => {
    this.setState({
      selectedFilter: { value: filter, index },
    });
  };

  renderColumnFilterControls() {
    const { spec, selectedFilter } = this.state;
    if (!selectedFilter) return;

    return (
      <FormEditor
        key={selectedFilter.index}
        fields={FILTER_FIELDS}
        initValue={selectedFilter.value}
        showCustom={f => !KNOWN_FILTER_TYPES.includes(f.type || '')}
        onClose={this.resetSelected}
        onDirty={this.handleDirty}
        onApply={filter => {
          const curFilter = splitFilter(deepGet(spec, 'spec.dataSchema.transformSpec.filter'));
          const newFilter = joinFilter(
            deepSet(curFilter, `dimensionFilters.${selectedFilter.index}`, filter),
          );
          this.updateSpec(deepSet(spec, 'spec.dataSchema.transformSpec.filter', newFilter));
        }}
        showDelete={selectedFilter.index !== -1}
        onDelete={() => {
          const curFilter = splitFilter(deepGet(spec, 'spec.dataSchema.transformSpec.filter'));
          const newFilter = joinFilter(
            deepDelete(curFilter, `dimensionFilters.${selectedFilter.index}`),
          );
          this.updateSpec(deepSet(spec, 'spec.dataSchema.transformSpec.filter', newFilter));
        }}
      />
    );
  }

  // ==================================================================

  async queryForSchema(initRun = false) {
    const { spec, cacheRows } = this.state;
    const metricsSpec: MetricSpec[] | undefined = deepGet(spec, 'spec.dataSchema.metricsSpec');
    const dimensions: (string | DimensionSpec)[] = deepGet(
      spec,
      'spec.dataSchema.dimensionsSpec.dimensions',
    );

    if (!cacheRows) {
      this.setState(({ schemaQueryState }) => ({
        schemaQueryState: initRun
          ? QueryState.INIT
          : new QueryState({
              error: new Error('must complete parse step'),
              lastData: schemaQueryState.getSomeData(),
            }),
      }));
      return;
    }

    this.setState(({ schemaQueryState }) => ({
      schemaQueryState: new QueryState({ loading: true, lastData: schemaQueryState.getSomeData() }),
    }));

    let sampleResponse: SampleResponse;
    try {
      sampleResponse = await sampleForSchema(spec, cacheRows);
    } catch (e) {
      this.setState(({ schemaQueryState }) => ({
        schemaQueryState: new QueryState({ error: e, lastData: schemaQueryState.getSomeData() }),
      }));
      return;
    }

    this.setState(({ schemaQueryState }) => ({
      schemaQueryState: new QueryState({
        data: {
          sampleResponse,
          dimensions: dimensions || guessDimensionsFromSampleResponse(sampleResponse),
          metricsSpec,
          definedDimensions: Boolean(dimensions),
        },
        lastData: schemaQueryState.getSomeData(),
      }),
    }));
  }

  renderSchemaStep() {
    const {
      columnFilter,
      schemaQueryState,
      selectedAutoDimension,
      selectedDimensionSpec,
      selectedMetricSpec,
    } = this.state;
    const spec = this.getEffectiveSpec();
    const rollup = Boolean(deepGet(spec, 'spec.dataSchema.granularitySpec.rollup'));
    const somethingSelected = Boolean(
      selectedAutoDimension || selectedDimensionSpec || selectedMetricSpec,
    );
    const schemaMode = getSchemaMode(spec);
    const arrayMode = getArrayMode(spec);

    let mainFill: JSX.Element | string;
    if (schemaQueryState.isInit()) {
      mainFill = <CenterMessage>Please enter more details for the previous steps</CenterMessage>;
    } else {
      const data = schemaQueryState.getSomeData();
      mainFill = (
        <div className="table-with-control">
          <div className="table-control">
            <ClearableInput
              value={columnFilter}
              onChange={columnFilter => this.setState({ columnFilter })}
              placeholder="Search columns"
            />
          </div>
          {data && (
            <SchemaTable
              sampleBundle={data}
              columnFilter={columnFilter}
              selectedAutoDimension={selectedAutoDimension}
              selectedDimensionSpecIndex={selectedDimensionSpec ? selectedDimensionSpec.index : -1}
              selectedMetricSpecIndex={selectedMetricSpec ? selectedMetricSpec.index : -1}
              onAutoDimensionSelect={this.onAutoDimensionSelect}
              onDimensionSelect={this.onDimensionSelect}
              onMetricSelect={this.onMetricSelect}
            />
          )}
          {schemaQueryState.isLoading() && <Loader />}
          {schemaQueryState.error && (
            <CenterMessage>{schemaQueryState.getErrorMessage()}</CenterMessage>
          )}
        </div>
      );
    }

    const schemaToolsMenu = this.renderSchemaToolsMenu();
    return (
      <>
        <div className="main">{mainFill}</div>
        <div className="control">
          <SchemaMessage schemaMode={schemaMode} />
          {!somethingSelected && (
            <>
              <FormGroupWithInfo
                inlineInfo
                info={
                  <PopoverText>
                    <p>
                      Select whether or not you want to set an explicit list of{' '}
                      <ExternalLink
                        href={`${getLink('DOCS')}/ingestion/ingestion-spec.html#dimensionsspec`}
                      >
                        dimensions
                      </ExternalLink>{' '}
                      and{' '}
                      <ExternalLink href={`${getLink('DOCS')}/querying/aggregations.html`}>
                        metrics
                      </ExternalLink>
                      . Explicitly setting dimensions and metrics can lead to better compression and
                      performance. If you disable this option, Druid will try to auto-detect fields
                      in your data and treat them as individual columns.
                    </p>
                  </PopoverText>
                }
              >
                <Switch
                  checked={schemaMode === 'fixed'}
                  onChange={() =>
                    this.setState({
                      newSchemaMode: schemaMode === 'fixed' ? 'type-aware-discovery' : 'fixed',
                    })
                  }
                  label="Explicitly specify schema"
                />
              </FormGroupWithInfo>
              {showArrayModeToggle(spec) && (
                <ArrayModeSwitch
                  arrayMode={arrayMode}
                  changeArrayMode={newArrayMode => {
                    this.setState({
                      newArrayMode,
                    });
                  }}
                />
              )}
              {schemaMode !== 'fixed' && (
                <AutoForm
                  fields={[
                    {
                      name: 'spec.dataSchema.dimensionsSpec.dimensionExclusions',
                      label: 'Dimension exclusions',
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
                  onChange={this.updateSpec}
                />
              )}
              <FormGroupWithInfo
                inlineInfo
                info={
                  <PopoverText>
                    <p>
                      If you enable{' '}
                      <ExternalLink href={`${getLink('DOCS')}/tutorials/tutorial-rollup.html`}>
                        roll-up
                      </ExternalLink>
                      , Druid will try to pre-aggregate data before indexing it to conserve storage.
                      The primary timestamp will be truncated to the specified query granularity,
                      and rows containing the same string field values will be aggregated together.
                    </p>
                    <p>
                      If you enable rollup, you must specify which columns are{' '}
                      <a href={`${getLink('DOCS')}/ingestion/ingestion-spec.html#dimensionsspec`}>
                        dimensions
                      </a>{' '}
                      (fields you want to group and filter on), and which are{' '}
                      <a href={`${getLink('DOCS')}/querying/aggregations.html`}>metrics</a> (fields
                      you want to aggregate on).
                    </p>
                  </PopoverText>
                }
              >
                <Switch
                  checked={rollup}
                  onChange={() => this.setState({ newRollup: !rollup })}
                  label="Rollup"
                />
              </FormGroupWithInfo>
              <AutoForm
                fields={[
                  {
                    name: 'spec.dataSchema.granularitySpec.queryGranularity',
                    label: 'Query granularity',
                    type: 'string',
                    suggestions: [
                      'none',
                      'second',
                      'minute',
                      'fifteen_minute',
                      'thirty_minute',
                      'hour',
                      'day',
                      'week',
                      'month',
                      'quarter',
                      'year',
                      'all',
                    ],
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
                onChange={this.updateSpecPreview}
                onFinalize={this.applyPreviewSpec}
              />
              <FormGroup>
                <Button
                  text="Add dimension"
                  disabled={schemaMode !== 'fixed'}
                  onClick={() => {
                    this.setState({
                      selectedDimensionSpec: {
                        value: {
                          type: 'string',
                        },
                        index: -1,
                      },
                    });
                  }}
                />
              </FormGroup>
              <FormGroup>
                <Button
                  text="Add metric"
                  onClick={() => {
                    this.setState({
                      selectedMetricSpec: {
                        value: {
                          type: 'doubleSum',
                        },
                        index: -1,
                      },
                    });
                  }}
                />
              </FormGroup>
              {schemaToolsMenu && (
                <FormGroup>
                  <Popover2 content={schemaToolsMenu}>
                    <Button icon={IconNames.BUILD} />
                  </Popover2>
                </FormGroup>
              )}
            </>
          )}
          {this.renderAutoDimensionControls()}
          {this.renderDimensionSpecControls()}
          {this.renderMetricSpecControls()}
          {this.renderChangeRollupAction()}
          {this.renderChangeSchemaModeAction()}
          {this.renderChangeArrayModeAction()}
        </div>
        {this.renderNextBar({
          disabled: !schemaQueryState.data,
        })}
      </>
    );
  }

  private readonly renderSchemaToolsMenu = () => {
    const { spec } = this.state;
    const dimensions: DimensionSpec[] | undefined = deepGet(
      spec,
      `spec.dataSchema.dimensionsSpec.dimensions`,
    );
    const metrics: MetricSpec[] | undefined = deepGet(spec, `spec.dataSchema.metricsSpec`);

    if (!dimensions && !metrics) return;
    return (
      <Menu>
        {dimensions && (
          <MenuItem
            icon={IconNames.ARROWS_HORIZONTAL}
            text="Order dimensions alphabetically"
            onClick={() => {
              if (!dimensions) return;
              const newSpec = deepSet(
                spec,
                `spec.dataSchema.dimensionsSpec.dimensions`,
                dimensions
                  .slice()
                  .sort((d1, d2) =>
                    alphanumericCompare(getDimensionSpecName(d1), getDimensionSpecName(d2)),
                  ),
              );
              this.updateSpec(newSpec);
            }}
          />
        )}
        {metrics && (
          <MenuItem
            icon={IconNames.ARROWS_HORIZONTAL}
            text="Order metrics alphabetically"
            onClick={() => {
              if (!metrics) return;
              const newSpec = deepSet(
                spec,
                `spec.dataSchema.metricsSpec`,
                metrics
                  .slice()
                  .sort((m1, m2) =>
                    alphanumericCompare(getMetricSpecName(m1), getMetricSpecName(m2)),
                  ),
              );
              this.updateSpec(newSpec);
            }}
          />
        )}
      </Menu>
    );
  };

  private readonly onAutoDimensionSelect = (selectedAutoDimension: string) => {
    this.setState({
      selectedAutoDimension,
      selectedDimensionSpec: undefined,
      selectedMetricSpec: undefined,
    });
  };

  private readonly onDimensionSelect = (dimensionSpec: DimensionSpec, index: number) => {
    this.setState({
      selectedAutoDimension: undefined,
      selectedDimensionSpec: { value: dimensionSpec, index },
      selectedMetricSpec: undefined,
    });
  };

  private readonly onMetricSelect = (metricSpec: MetricSpec, index: number) => {
    this.setState({
      selectedAutoDimension: undefined,
      selectedDimensionSpec: undefined,
      selectedMetricSpec: { value: metricSpec, index },
    });
  };

  renderChangeRollupAction() {
    const { newRollup, spec, cacheRows } = this.state;
    if (typeof newRollup === 'undefined' || !cacheRows) return;

    return (
      <AsyncActionDialog
        action={async () => {
          const sampleResponse = await sampleForTransform(spec, cacheRows);
          this.updateSpec(
            updateSchemaWithSample(
              spec,
              sampleResponse,
              getSchemaMode(spec),
              getArrayMode(spec),
              newRollup,
              true,
            ),
          );
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

  renderChangeSchemaModeAction() {
    const { newSchemaMode, spec, cacheRows } = this.state;
    if (!newSchemaMode || !cacheRows) return;
    const autoDetect = newSchemaMode !== 'fixed';

    return (
      <AsyncActionDialog
        action={async () => {
          const sampleResponse = await sampleForTransform(spec, cacheRows);
          this.updateSpec(
            updateSchemaWithSample(
              spec,
              sampleResponse,
              newSchemaMode,
              getArrayMode(spec),
              getRollup(spec),
            ),
          );
        }}
        confirmButtonText={`Yes - ${autoDetect ? 'auto detect' : 'explicitly define'} schema`}
        successText={`Schema mode changed to ${autoDetect ? 'auto detect' : 'explicitly defined'}.`}
        failText="Could not change schema mode"
        intent={Intent.WARNING}
        onClose={() => this.setState({ newSchemaMode: undefined })}
      >
        <p>
          {autoDetect
            ? `Are you sure you want Druid to auto detect the data schema?`
            : `Are you sure you want to explicitly specify a schema?`}
        </p>
        <p>Making this change will reset all schema configuration done so far.</p>
        {autoDetect && (
          <RadioGroup
            label="Schemaless mode"
            selectedValue={newSchemaMode}
            onChange={() => {
              this.setState({
                newSchemaMode:
                  newSchemaMode === 'string-only-discovery'
                    ? 'type-aware-discovery'
                    : 'string-only-discovery',
              });
            }}
          >
            <Radio value="type-aware-discovery">
              Use the new type-aware schema discovery capability to discover columns according to
              data type. Columns with multiple values will be ingested as ARRAY types. For more
              information see the{' '}
              <ExternalLink
                href={`${getLink(
                  'DOCS',
                )}/ingestion/schema-design.html#schema-auto-discovery-for-dimensions`}
              >
                documentation
              </ExternalLink>
              .
            </Radio>
            <Radio value="string-only-discovery">
              Use classic string-only schema discovery to discover all new columns as strings.
              Columns with multiple values will be ingested as multi-value-strings.
            </Radio>
          </RadioGroup>
        )}
      </AsyncActionDialog>
    );
  }

  renderChangeArrayModeAction() {
    const { newArrayMode, spec, cacheRows } = this.state;
    if (!newArrayMode || !cacheRows) return;
    const multiValues = newArrayMode === 'multi-values';

    return (
      <AsyncActionDialog
        action={async () => {
          const sampleResponse = await sampleForTransform(spec, cacheRows);
          this.updateSpec(
            updateSchemaWithSample(
              spec,
              sampleResponse,
              getSchemaMode(spec),
              newArrayMode,
              getRollup(spec),
            ),
          );
        }}
        confirmButtonText={`Yes - ${multiValues ? 'use MVDs' : 'ARRAYs'}`}
        successText={`Array mode changed to ${multiValues ? 'multi-values' : 'arrays'}.`}
        failText="Could not change array mode"
        intent={Intent.WARNING}
        onClose={() => this.setState({ newArrayMode: undefined })}
      >
        <p>
          {multiValues
            ? `Are you sure you want to use multi-value dimensions?`
            : `Are you sure you want to use ARRAYs?`}
        </p>
        <p>Making this change will reset all schema configuration done so far.</p>
      </AsyncActionDialog>
    );
  }

  renderAutoDimensionControls() {
    const { spec, selectedAutoDimension } = this.state;
    if (!selectedAutoDimension) return;

    return (
      <div className="edit-controls">
        <FormGroup label="Name">
          <InputGroup value={selectedAutoDimension} onChange={() => {}} readOnly />
        </FormGroup>
        <FormGroup>
          <Button
            icon={IconNames.CROSS}
            text="Exclude"
            intent={Intent.DANGER}
            onClick={() => {
              this.updateSpec(
                deepSet(
                  spec,
                  `spec.dataSchema.dimensionsSpec.dimensionExclusions.[append]`,
                  selectedAutoDimension,
                ),
              );
              this.resetSelected();
            }}
          />
        </FormGroup>
        <FormGroup>
          <Button text="Close" onClick={this.resetSelected} />
        </FormGroup>
      </div>
    );
  }

  renderDimensionSpecControls() {
    const { spec, selectedDimensionSpec } = this.state;
    if (!selectedDimensionSpec) return;
    const schemaMode = getSchemaMode(spec);

    const dimensions = deepGet(spec, `spec.dataSchema.dimensionsSpec.dimensions`) || EMPTY_ARRAY;

    const moveTo = (newIndex: number) => {
      const newDimension = moveElement(dimensions, selectedDimensionSpec.index, newIndex);
      const newSpec = deepSet(spec, `spec.dataSchema.dimensionsSpec.dimensions`, newDimension);
      this.updateSpec(newSpec);
      this.resetSelected();
    };

    const reorderDimensionMenu = (
      <ReorderMenu
        things={dimensions}
        selectedIndex={selectedDimensionSpec.index}
        moveTo={moveTo}
      />
    );

    const convertToMetric = (type: string, prefix: string) => {
      const specWithoutDimension =
        schemaMode === 'fixed'
          ? deepDelete(
              spec,
              `spec.dataSchema.dimensionsSpec.dimensions.${selectedDimensionSpec.index}`,
            )
          : spec;

      const specWithMetric = deepSet(specWithoutDimension, `spec.dataSchema.metricsSpec.[append]`, {
        name: `${prefix}_${selectedDimensionSpec.value.name}`,
        type,
        fieldName: selectedDimensionSpec.value.name,
      });

      this.updateSpec(specWithMetric);
      this.resetSelected();
    };

    const convertToMetricMenu = (
      <Menu>
        <MenuItem
          text="Convert to longSum metric"
          onClick={() => convertToMetric('longSum', 'sum')}
        />
        <MenuItem
          text="Convert to doubleSum metric"
          onClick={() => convertToMetric('doubleSum', 'sum')}
        />
        <MenuItem
          text="Convert to thetaSketch metric"
          onClick={() => convertToMetric('thetaSketch', 'theta')}
        />
        <MenuItem
          text="Convert to HLLSketchBuild metric"
          onClick={() => convertToMetric('HLLSketchBuild', 'hll')}
        />
        <MenuItem
          text="Convert to quantilesDoublesSketch metric"
          onClick={() => convertToMetric('quantilesDoublesSketch', 'quantiles_doubles')}
        />
        <MenuItem
          text="Convert to hyperUnique metric"
          onClick={() => convertToMetric('hyperUnique', 'unique')}
        />
      </Menu>
    );

    return (
      <FormEditor
        key={selectedDimensionSpec.index}
        fields={DIMENSION_SPEC_FIELDS}
        initValue={selectedDimensionSpec.value}
        onClose={this.resetSelected}
        onDirty={this.handleDirty}
        onApply={dimensionSpec =>
          this.updateSpec(
            deepSet(
              spec,
              `spec.dataSchema.dimensionsSpec.dimensions.${selectedDimensionSpec.index}`,
              dimensionSpec,
            ),
          )
        }
        showDelete={selectedDimensionSpec.index !== -1}
        disableDelete={dimensions.length <= 1}
        onDelete={() =>
          this.updateSpec(
            deepDelete(
              spec,
              `spec.dataSchema.dimensionsSpec.dimensions.${selectedDimensionSpec.index}`,
            ),
          )
        }
      >
        {selectedDimensionSpec.index !== -1 && (
          <FormGroup>
            <Popover2 content={reorderDimensionMenu}>
              <Button
                icon={IconNames.ARROWS_HORIZONTAL}
                text="Reorder dimension"
                rightIcon={IconNames.CARET_DOWN}
              />
            </Popover2>
          </FormGroup>
        )}
        {selectedDimensionSpec.index !== -1 && deepGet(spec, 'spec.dataSchema.metricsSpec') && (
          <FormGroup>
            <Popover2 content={convertToMetricMenu}>
              <Button
                icon={IconNames.EXCHANGE}
                text="Convert to metric"
                rightIcon={IconNames.CARET_DOWN}
                disabled={dimensions.length <= 1}
              />
            </Popover2>
          </FormGroup>
        )}
      </FormEditor>
    );
  }

  renderMetricSpecControls() {
    const { spec, selectedMetricSpec } = this.state;
    if (!selectedMetricSpec) return;
    const schemaMode = getSchemaMode(spec);
    const selectedMetricSpecFieldName = selectedMetricSpec.value.fieldName;

    const convertToDimension = (type: string) => {
      if (!selectedMetricSpecFieldName) return;
      const specWithoutMetric = deepDelete(
        spec,
        `spec.dataSchema.metricsSpec.${selectedMetricSpec.index}`,
      );

      const specWithDimension = deepSet(
        specWithoutMetric,
        `spec.dataSchema.dimensionsSpec.dimensions.[append]`,
        {
          type,
          name: selectedMetricSpecFieldName,
        },
      );

      this.updateSpec(specWithDimension);
      this.resetSelected();
    };

    const convertToDimensionMenu = (
      <Menu>
        <MenuItem text="Convert to string dimension" onClick={() => convertToDimension('string')} />
        <MenuItem text="Convert to long dimension" onClick={() => convertToDimension('long')} />
        <MenuItem text="Convert to float dimension" onClick={() => convertToDimension('float')} />
        <MenuItem text="Convert to double dimension" onClick={() => convertToDimension('double')} />
      </Menu>
    );

    return (
      <FormEditor
        key={selectedMetricSpec.index}
        fields={METRIC_SPEC_FIELDS}
        initValue={selectedMetricSpec.value}
        onClose={this.resetSelected}
        onDirty={this.handleDirty}
        onApply={metricSpec =>
          this.updateSpec(
            deepSet(spec, `spec.dataSchema.metricsSpec.${selectedMetricSpec.index}`, metricSpec),
          )
        }
        showDelete={selectedMetricSpec.index !== -1}
        onDelete={() =>
          this.updateSpec(
            deepDelete(spec, `spec.dataSchema.metricsSpec.${selectedMetricSpec.index}`),
          )
        }
      >
        {selectedMetricSpec.index !== -1 &&
          schemaMode === 'fixed' &&
          selectedMetricSpecFieldName && (
            <FormGroup>
              <Popover2 content={convertToDimensionMenu}>
                <Button
                  icon={IconNames.EXCHANGE}
                  text="Convert to dimension"
                  rightIcon={IconNames.CARET_DOWN}
                />
              </Popover2>
            </FormGroup>
          )}
      </FormEditor>
    );
  }

  // ==================================================================

  renderPartitionStep() {
    const { spec } = this.state;
    const tuningConfig: TuningConfig = deepGet(spec, 'spec.tuningConfig') || EMPTY_OBJECT;
    const dimensions: (string | DimensionSpec)[] | undefined = deepGet(
      spec,
      'spec.dataSchema.dimensionsSpec.dimensions',
    );
    const dimensionNames = dimensions?.map(getDimensionSpecName);

    let nonsensicalSingleDimPartitioningMessage: JSX.Element | undefined;
    if (dimensions && Array.isArray(dimensionNames) && dimensionNames.length) {
      const partitionDimensions = deepGet(tuningConfig, 'partitionsSpec.partitionDimensions');
      if (
        deepGet(tuningConfig, 'partitionsSpec.type') === 'range' &&
        Array.isArray(partitionDimensions) &&
        partitionDimensions.join(',') !==
          dimensionNames.slice(0, partitionDimensions.length).join(',')
      ) {
        const dimensionNamesPrefix = dimensionNames.slice(0, partitionDimensions.length);
        nonsensicalSingleDimPartitioningMessage = (
          <FormGroup>
            <Callout intent={Intent.WARNING}>
              <p>Your partitioning and sorting configuration is uncommon.</p>
              <p>
                For best performance the leading dimensions in your schema (
                <Code>{dimensionNamesPrefix.join(', ')}</Code>), which is what the data will be
                primarily sorted on, commonly matches the partitioning dimensions (
                <Code>{partitionDimensions.join(', ')}</Code>).
              </p>
              <p>
                <Button
                  intent={Intent.WARNING}
                  onClick={() =>
                    this.updateSpec(
                      deepSet(
                        spec,
                        'spec.dataSchema.dimensionsSpec.dimensions',
                        moveToIndex(dimensions, d =>
                          partitionDimensions.indexOf(getDimensionSpecName(d)),
                        ),
                      ),
                    )
                  }
                >
                  {`Put `}
                  <strong>{partitionDimensions.join(', ')}</strong>
                  {` first in the dimensions list`}
                </Button>
              </p>
              <p>
                <Button
                  intent={Intent.WARNING}
                  onClick={() =>
                    this.updateSpec(
                      deepSet(
                        spec,
                        'spec.tuningConfig.partitionsSpec.partitionDimensions',
                        dimensionNamesPrefix,
                      ),
                    )
                  }
                >
                  {`Partition on `}
                  <strong>{dimensionNames.slice(0, partitionDimensions.length).join(', ')}</strong>
                  {` instead`}
                </Button>
              </p>
            </Callout>
          </FormGroup>
        );
      }

      const partitionDimension = deepGet(tuningConfig, 'partitionsSpec.partitionDimension');
      if (
        deepGet(tuningConfig, 'partitionsSpec.type') === 'single_dim' &&
        typeof partitionDimension === 'string' &&
        partitionDimension !== dimensionNames[0]
      ) {
        const firstDimensionName = dimensionNames[0];
        nonsensicalSingleDimPartitioningMessage = (
          <FormGroup>
            <Callout intent={Intent.WARNING}>
              <p>Your partitioning and sorting configuration is uncommon.</p>
              <p>
                For best performance the first dimension in your schema (
                <Code>{firstDimensionName}</Code>), which is what the data will be primarily sorted
                on, commonly matches the partitioning dimension (<Code>{partitionDimension}</Code>).
              </p>
              <p>
                <Button
                  intent={Intent.WARNING}
                  onClick={() =>
                    this.updateSpec(
                      deepSet(
                        spec,
                        'spec.dataSchema.dimensionsSpec.dimensions',
                        moveToIndex(dimensions, d =>
                          getDimensionSpecName(d) === partitionDimension ? 0 : -1,
                        ),
                      ),
                    )
                  }
                >
                  {`Put `}
                  <strong>{partitionDimension}</strong>
                  {` first in the dimensions list`}
                </Button>
              </p>
              <p>
                <Button
                  intent={Intent.WARNING}
                  onClick={() =>
                    this.updateSpec(
                      deepSet(
                        spec,
                        'spec.tuningConfig.partitionsSpec.partitionDimension',
                        firstDimensionName,
                      ),
                    )
                  }
                >
                  {`Partition on `}
                  <strong>{firstDimensionName}</strong>
                  {` instead`}
                </Button>
              </p>
            </Callout>
          </FormGroup>
        );
      }
    }

    return (
      <>
        <div className="main">
          <H5>Primary partitioning (by time)</H5>
          <AutoForm
            fields={PRIMARY_PARTITION_RELATED_FORM_FIELDS}
            model={spec}
            onChange={this.updateSpec}
          />
        </div>
        <div className="other">
          <H5>Secondary partitioning</H5>
          <AutoForm
            fields={getSecondaryPartitionRelatedFormFields(spec, dimensionNames)}
            model={spec}
            globalAdjustment={adjustForceGuaranteedRollup}
            onChange={this.updateSpec}
          />
        </div>
        <div className="control">
          <PartitionMessage />
          {nonsensicalSingleDimPartitioningMessage}
          <AppendToExistingIssue spec={spec} onChangeSpec={this.updateSpec} />
        </div>
        {this.renderNextBar({
          disabled: invalidPartitionConfig(spec),
        })}
      </>
    );
  }

  // ==================================================================

  renderTuningStep() {
    const { spec } = this.state;
    const ioConfig: IoConfig = deepGet(spec, 'spec.ioConfig') || EMPTY_OBJECT;

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
                onChange={c => this.updateSpec(deepSet(spec, 'spec.ioConfig', c))}
              />
            ) : (
              <div>
                {ioConfig.inputSource
                  ? `No specific tuning configs for inputSource of type '${deepGet(
                      ioConfig,
                      'inputSource.type',
                    )}'.`
                  : `No specific tuning configs.`}
              </div>
            )
          ) : (
            <JsonInput
              value={ioConfig}
              onChange={c => this.updateSpec(deepSet(spec, 'spec.ioConfig', c))}
              height="300px"
            />
          )}
        </div>
        <div className="other">
          <H5>General tuning</H5>
          <AutoForm fields={getTuningFormFields()} model={spec} onChange={this.updateSpec} />
        </div>
        <div className="control">
          <TuningMessage />
        </div>
        {this.renderNextBar({
          disabled: invalidIoConfig(ioConfig),
        })}
      </>
    );
  }

  // ==================================================================

  renderPublishStep() {
    const { spec } = this.state;
    const parallel = deepGet(spec, 'spec.tuningConfig.maxNumConcurrentSubTasks') > 1;

    return (
      <>
        <div className="main">
          <H5>Publish configuration</H5>
          <AutoForm
            fields={[
              {
                name: 'spec.dataSchema.dataSource',
                label: 'Datasource name',
                type: 'string',
                valueAdjustment: d => (typeof d === 'string' ? adjustId(d) : d),
                info: (
                  <>
                    <p>This is the name of the datasource (table) in Druid.</p>
                    <p>
                      The datasource name can not start with a dot <Code>.</Code>, include slashes{' '}
                      <Code>/</Code>, or have whitespace other than space.
                    </p>
                  </>
                ),
              },
              {
                name: 'spec.ioConfig.appendToExisting',
                label: 'Append to existing',
                type: 'boolean',
                defined: s => !isStreamingSpec(s),
                defaultValue: false,
                // appendToExisting can only be set on 'dynamic' portioning.
                // We chose to show it always and instead have a specific message, separate from this form, to notify the user of the issue.
                info: (
                  <>
                    Creates segments as additional shards of the latest version, effectively
                    appending to the segment set instead of replacing it.
                  </>
                ),
              },
            ]}
            model={spec}
            onChange={this.updateSpec}
          />
          <FormGroupWithInfo
            inlineInfo
            info={
              <PopoverText>
                <p>
                  If you want to append data to a datasource while compaction is running, you need
                  to enable concurrent append and replace for the datasource by updating the
                  compaction settings.
                </p>
                <p>
                  For more information refer to the{' '}
                  <ExternalLink
                    href={`${getLink('DOCS')}/ingestion/concurrent-append-replace.html`}
                  >
                    documentation
                  </ExternalLink>
                  .
                </p>
              </PopoverText>
            }
          >
            <Switch
              label="Use concurrent locks (experimental)"
              checked={Boolean(deepGet(spec, 'context.useConcurrentLocks'))}
              onChange={() => {
                this.updateSpec(
                  deepGet(spec, 'context.useConcurrentLocks')
                    ? deepDelete(spec, 'context.useConcurrentLocks')
                    : deepSet(spec, 'context.useConcurrentLocks', true),
                );
              }}
            />
          </FormGroupWithInfo>
        </div>
        <div className="other">
          <H5>Parse error reporting</H5>
          <AutoForm
            fields={[
              {
                name: 'spec.tuningConfig.logParseExceptions',
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
                name: 'spec.tuningConfig.maxParseExceptions',
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
                name: 'spec.tuningConfig.maxSavedParseExceptions',
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
            onChange={this.updateSpec}
          />
        </div>
        <div className="control">
          <PublishMessage />
          <AppendToExistingIssue spec={spec} onChangeSpec={this.updateSpec} />
        </div>
        {this.renderNextBar({})}
      </>
    );
  }

  // ==================================================================
  private readonly getSupervisorJson = async (): Promise<void> => {
    const { initSupervisorId } = this.props;
    if (!initSupervisorId) return;

    try {
      const resp = await Api.instance.get(
        `/druid/indexer/v1/supervisor/${Api.encodePath(initSupervisorId)}`,
      );
      this.updateSpec(cleanSpec(resp.data));
      this.setState({ continueToSpec: true });
      this.updateStep('spec');
    } catch (e) {
      AppToaster.show({
        message: `Failed to get supervisor spec: ${getDruidErrorMessage(e)}`,
        intent: Intent.DANGER,
      });
    }
  };

  private readonly getTaskJson = async (): Promise<void> => {
    const { initTaskId } = this.props;
    if (!initTaskId) return;

    try {
      const resp = await Api.instance.get(`/druid/indexer/v1/task/${Api.encodePath(initTaskId)}`);
      this.updateSpec(cleanSpec(resp.data.payload));
      this.setState({ continueToSpec: true });
      this.updateStep('spec');
    } catch (e) {
      AppToaster.show({
        message: `Failed to get task spec: ${getDruidErrorMessage(e)}`,
        intent: Intent.DANGER,
      });
    }
  };

  async queryForSpec() {
    let existingDatasources: string[];
    try {
      existingDatasources = (await Api.instance.get<string[]>('/druid/coordinator/v1/datasources'))
        .data;
    } catch {
      return;
    }

    this.setState({
      existingDatasources,
    });
  }

  renderSpecStep() {
    const { spec, existingDatasources, submitting } = this.state;
    const issueWithSpec = getIssueWithSpec(spec);
    const datasource = deepGet(spec, 'spec.dataSchema.dataSource');

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
          <SpecMessage />
          {issueWithSpec && (
            <FormGroup>
              <Callout
                intent={Intent.WARNING}
              >{`There is an issue with the spec: ${issueWithSpec}`}</Callout>
            </FormGroup>
          )}
          {getSchemaMode(spec) === 'type-aware-discovery' &&
            existingDatasources?.includes(datasource) && (
              <FormGroup>
                <Callout intent={Intent.WARNING}>
                  <p>
                    You have enabled type-aware schema discovery (
                    <Code>useSchemaDiscovery: true</Code>) to ingest data into the existing
                    datasource <Code>{datasource}</Code>.
                  </p>
                  <p>
                    If you used string-based schema discovery when first ingesting data to{' '}
                    <Code>{datasource}</Code>, using type-aware schema discovery now can cause
                    problems with the values multi-value string dimensions.
                  </p>
                  <p>
                    For more information see the{' '}
                    <ExternalLink
                      href={`${getLink(
                        'DOCS',
                      )}/ingestion/schema-design.html#schema-auto-discovery-for-dimensions`}
                    >
                      documentation
                    </ExternalLink>
                    .
                  </p>
                </Callout>
              </FormGroup>
            )}
          <AppendToExistingIssue spec={spec} onChangeSpec={this.updateSpec} />
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
            text={submitting ? 'Submitting...' : 'Submit'}
            rightIcon={IconNames.CLOUD_UPLOAD}
            intent={Intent.PRIMARY}
            disabled={submitting || Boolean(issueWithSpec)}
            onClick={() => void this.handleSubmit()}
          />
        </div>
      </>
    );
  }

  private readonly handleSubmit = async () => {
    const { goToSupervisor, goToTasks } = this.props;
    const { spec, submitting } = this.state;
    if (submitting) return;

    this.setState({ submitting: true });
    if (isStreamingSpec(spec)) {
      try {
        await Api.instance.post('/druid/indexer/v1/supervisor', spec);
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
        goToSupervisor(getSpecDatasourceName(spec));
      }, 1000);
    } else {
      let taskResp: any;
      try {
        taskResp = await Api.instance.post('/druid/indexer/v1/task', spec);
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
        goToTasks(taskResp.data.task);
      }, 1000);
    }
  };
}
