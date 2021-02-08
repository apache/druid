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
  InputGroup,
  Intent,
  Menu,
  MenuItem,
  Popover,
  Switch,
  TextArea,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import classNames from 'classnames';
import * as JSONBig from 'json-bigint-native';
import memoize from 'memoize-one';
import React from 'react';

import {
  AutoForm,
  CenterMessage,
  ClearableInput,
  ExternalLink,
  JsonInput,
  Loader,
  PopoverText,
} from '../../components';
import { FormGroupWithInfo } from '../../components/form-group-with-info/form-group-with-info';
import { AsyncActionDialog } from '../../dialogs';
import {
  addTimestampTransform,
  adjustId,
  CONSTANT_TIMESTAMP_SPEC,
  CONSTANT_TIMESTAMP_SPEC_FIELDS,
  DIMENSION_SPEC_FIELDS,
  FILTER_FIELDS,
  FILTERS_FIELDS,
  FLATTEN_FIELD_FIELDS,
  getDimensionSpecName,
  getIssueWithSpec,
  getMetricSpecName,
  getTimestampExpressionFields,
  getTimestampSchema,
  INPUT_FORMAT_FIELDS,
  issueWithSampleData,
  KNOWN_FILTER_TYPES,
  METRIC_SPEC_FIELDS,
  PRIMARY_PARTITION_RELATED_FORM_FIELDS,
  removeTimestampTransform,
  TIMESTAMP_SPEC_FIELDS,
  TimestampSpec,
  Transform,
  TRANSFORM_FIELDS,
  updateSchemaWithSample,
} from '../../druid-models';
import {
  adjustForceGuaranteedRollup,
  cleanSpec,
  computeFlattenPathsForData,
  DimensionMode,
  DimensionSpec,
  DruidFilter,
  fillDataSourceNameIfNeeded,
  fillInputFormatIfNeeded,
  FlattenField,
  getDimensionMode,
  getIngestionComboType,
  getIngestionImage,
  getIngestionTitle,
  getIoConfigFormFields,
  getIoConfigTuningFormFields,
  getRequiredModule,
  getRollup,
  getSecondaryPartitionRelatedFormFields,
  getSpecType,
  getTuningFormFields,
  IngestionComboTypeWithExtra,
  IngestionSpec,
  InputFormat,
  inputFormatCanFlatten,
  invalidIoConfig,
  invalidPartitionConfig,
  IoConfig,
  isDruidSource,
  isEmptyIngestionSpec,
  issueWithIoConfig,
  isTask,
  joinFilter,
  MAX_INLINE_DATA_LENGTH,
  MetricSpec,
  normalizeSpec,
  NUMERIC_TIME_FORMATS,
  possibleDruidFormatForValues,
  splitFilter,
  TuningConfig,
  updateIngestionType,
  upgradeSpec,
} from '../../druid-models';
import { getLink } from '../../links';
import { Api, AppToaster, UrlBaser } from '../../singletons';
import {
  deepDelete,
  deepGet,
  deepSet,
  deepSetMulti,
  EMPTY_ARRAY,
  EMPTY_OBJECT,
  filterMap,
  getDruidErrorMessage,
  localStorageGet,
  LocalStorageKeys,
  localStorageSet,
  moveElement,
  oneOf,
  parseJson,
  pluralIfNeeded,
  QueryState,
} from '../../utils';
import {
  CacheRows,
  ExampleManifest,
  getCacheRowsFromSampleResponse,
  getProxyOverlordModules,
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

import { ExamplePicker } from './example-picker/example-picker';
import { FilterTable, filterTableSelectedColumnName } from './filter-table/filter-table';
import {
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

const DEFAULT_ROLLUP_SETTING = false;

function showRawLine(line: SampleEntry): string {
  if (!line.parsed) return 'No parse';
  const raw = line.parsed.raw;
  if (typeof raw !== 'string') return String(raw);
  if (raw.includes('\n')) {
    return `[Multi-line row, length: ${raw.length}]`;
  }
  if (raw.length > 1000) {
    return raw.substr(0, 1000) + '...';
  }
  return raw;
}

function showDruidLine(line: SampleEntry): string {
  if (!line.parsed) return 'No parse';
  return `Druid row: ${JSONBig.stringify(line.parsed)}`;
}

function showBlankLine(line: SampleEntry): string {
  return line.parsed ? `[Row: ${JSONBig.stringify(line.parsed)}]` : '[Binary data]';
}

function getTimestampSpec(headerAndRows: HeaderAndRows | null): TimestampSpec {
  if (!headerAndRows) return CONSTANT_TIMESTAMP_SPEC;

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
    CONSTANT_TIMESTAMP_SPEC // Ok, empty it is...
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

export interface LoadDataViewProps {
  initSupervisorId?: string;
  initTaskId?: string;
  exampleManifestsUrl?: string;
  goToIngestion: (taskGroupId: string | undefined, supervisor?: string) => void;
}

export interface LoadDataViewState {
  step: Step;
  spec: IngestionSpec;
  specPreview: IngestionSpec;
  cacheRows?: CacheRows;
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
    spec: IngestionSpec;
  }>;

  // for transform
  transformQueryState: QueryState<HeaderAndRows>;
  selectedTransformIndex: number;
  selectedTransform?: Transform;

  // for filter
  filterQueryState: QueryState<HeaderAndRows>;
  selectedFilterIndex: number;
  selectedFilter?: DruidFilter;
  newFilterValue?: Record<string, any>;

  // for schema
  schemaQueryState: QueryState<{
    headerAndRows: HeaderAndRows;
    dimensions: (string | DimensionSpec)[] | undefined;
    metricsSpec: MetricSpec[] | undefined;
  }>;
  selectedAutoDimension?: string;
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
      step: 'loading',
      spec,
      specPreview: spec,

      // dialogs / modals
      showResetConfirm: false,
      continueToSpec: false,

      // general
      sampleStrategy: 'start',
      columnFilter: '',
      specialColumnsOnly: false,

      // for inputSource
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
      overlordModules = await getProxyOverlordModules();
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

  private updateStep = (newStep: Step) => {
    this.setState(state => ({ step: newStep, specPreview: state.spec }));
  };

  private updateSpec = (newSpec: IngestionSpec) => {
    newSpec = normalizeSpec(newSpec);
    newSpec = upgradeSpec(newSpec);
    const deltaState: Partial<LoadDataViewState> = { spec: newSpec, specPreview: newSpec };
    if (!deepGet(newSpec, 'spec.ioConfig.type')) {
      deltaState.cacheRows = undefined;
    }
    this.setState(deltaState as LoadDataViewState);
    localStorageSet(LocalStorageKeys.INGESTION_SPEC, JSONBig.stringify(newSpec));
  };

  private updateSpecPreview = (newSpecPreview: IngestionSpec) => {
    this.setState({ specPreview: newSpecPreview });
  };

  private applyPreviewSpec = () => {
    this.setState(({ spec, specPreview }) => {
      localStorageSet(LocalStorageKeys.INGESTION_SPEC, JSONBig.stringify(specPreview));
      return { spec: spec === specPreview ? Object.assign({}, specPreview) : specPreview }; // If applying again, make a shallow copy to force a refresh
    });
  };

  private revertPreviewSpec = () => {
    this.setState(({ spec }) => ({ specPreview: spec }));
  };

  isPreviewSpecSame() {
    const { spec, specPreview } = this.state;
    return spec === specPreview;
  }

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
      case 'welcome':
        return this.queryForWelcome();

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
    }
  }

  renderActionCard(icon: IconName, title: string, caption: string, onClick: () => void) {
    return (
      <Card className={'spec-card'} interactive onClick={onClick} elevation={1}>
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
    const previewSpecSame = this.isPreviewSpecSame();
    const queryStateHasError = Boolean(queryState && queryState.error);

    return (
      <FormGroup className="control-buttons">
        <Button
          text="Apply"
          disabled={(previewSpecSame && !queryStateHasError) || Boolean(issue)}
          intent={Intent.PRIMARY}
          onClick={this.applyPreviewSpec}
        />
        {!previewSpecSame && <Button text="Cancel" onClick={this.revertPreviewSpec} />}
      </FormGroup>
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

  renderNextBar(options: { nextStep?: Step; disabled?: boolean; onNextStep?: () => boolean }) {
    const { disabled, onNextStep } = options;
    const { step } = this.state;
    const nextStep = options.nextStep || STEPS[STEPS.indexOf(step) + 1] || STEPS[0];

    return (
      <div className="next-bar">
        <Button
          text={`Next: ${VIEW_TITLE[nextStep]}`}
          rightIcon={IconNames.ARROW_RIGHT}
          intent={Intent.PRIMARY}
          disabled={disabled || !this.isPreviewSpecSame()}
          onClick={() => {
            if (disabled) return;
            if (onNextStep && !onNextStep()) return;

            this.updateStep(nextStep);
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
        elevation={1}
        onClick={() => {
          this.setState({
            selectedComboType: selectedComboType !== comboType ? comboType : undefined,
          });
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
    const { exampleManifestsUrl } = this.props;
    const { spec, exampleManifests } = this.state;
    const noExamples = Boolean(!exampleManifests || !exampleManifests.length);

    const welcomeMessage = this.renderWelcomeStepMessage();
    return (
      <>
        <div className="main">
          <div className="ingestion-cards">
            {this.renderIngestionCard('kafka')}
            {this.renderIngestionCard('kinesis')}
            {this.renderIngestionCard('azure-event-hubs')}
            {this.renderIngestionCard('index_parallel:s3')}
            {this.renderIngestionCard('index_parallel:azure')}
            {this.renderIngestionCard('index_parallel:google')}
            {this.renderIngestionCard('index_parallel:hdfs')}
            {this.renderIngestionCard('index_parallel:druid')}
            {this.renderIngestionCard('index_parallel:http')}
            {this.renderIngestionCard('index_parallel:local')}
            {this.renderIngestionCard('index_parallel:inline')}
            {exampleManifestsUrl && this.renderIngestionCard('example', noExamples)}
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
    const { selectedComboType, exampleManifests } = this.state;

    if (!selectedComboType) {
      return <p>Please specify where your raw data is located</p>;
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

      case 'index_parallel:azure':
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
        if (exampleManifests && exampleManifests.length) {
          return; // Yield to example picker controls
        } else {
          return <p>Could not load examples.</p>;
        }

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
    const { goToIngestion } = this.props;
    const { spec, selectedComboType, exampleManifests } = this.state;

    const issue = this.selectedIngestionTypeIssue();
    if (issue) return;

    switch (selectedComboType) {
      case 'index_parallel:http':
      case 'index_parallel:local':
      case 'index_parallel:druid':
      case 'index_parallel:inline':
      case 'index_parallel:s3':
      case 'index_parallel:azure':
      case 'index_parallel:google':
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
        if (!exampleManifests) return;
        return (
          <ExamplePicker
            exampleManifests={exampleManifests}
            onSelectExample={exampleManifest => {
              this.updateSpec(exampleManifest.spec);
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
                onClick={() => goToIngestion(undefined, 'supervisor')}
              />
            </FormGroup>
            <FormGroup>
              <Button
                text="Submit task"
                rightIcon={IconNames.ARROW_RIGHT}
                intent={Intent.PRIMARY}
                onClick={() => goToIngestion(undefined, 'task')}
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
          <ExternalLink href={`${getLink('DOCS')}/operations/including-extensions`}>
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
      deltaState.cacheRows = getCacheRowsFromSampleResponse(sampleResponse, true);
    }
    this.setState(deltaState as LoadDataViewState);
  }

  renderConnectStep() {
    const { specPreview: spec, inputQueryState, sampleStrategy } = this.state;
    const specType = getSpecType(spec);
    const ioConfig: IoConfig = deepGet(spec, 'spec.ioConfig') || EMPTY_OBJECT;
    const inlineMode = deepGet(spec, 'spec.ioConfig.inputSource.type') === 'inline';
    const druidSource = isDruidSource(spec);

    let mainFill: JSX.Element | string = '';
    if (inlineMode) {
      mainFill = (
        <TextArea
          className="inline-data"
          placeholder="Paste your data here"
          value={deepGet(spec, 'spec.ioConfig.inputSource.data')}
          onChange={(e: any) => {
            const stringValue = e.target.value.substr(0, MAX_INLINE_DATA_LENGTH);
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
              value={
                inputData.length
                  ? (inputData.every(l => !l.parsed)
                      ? inputData.map(showBlankLine)
                      : druidSource
                      ? inputData.map(showDruidLine)
                      : inputData.map(showRawLine)
                    ).join('\n')
                  : 'No data returned from sampler'
              }
            />
          )}
          {inputQueryState.isLoading() && <Loader />}
          {inputQueryState.error && (
            <CenterMessage>{`Error: ${inputQueryState.getErrorMessage()}`}</CenterMessage>
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
          {oneOf(specType, 'kafka', 'kinesis') && (
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
                column: '__time',
                format: 'iso',
              });

              if (typeof inputData.rollup === 'boolean') {
                newSpec = deepSet(
                  newSpec,
                  'spec.dataSchema.granularitySpec.rollup',
                  inputData.rollup,
                );
              }

              if (inputData.queryGranularity) {
                newSpec = deepSet(
                  newSpec,
                  'spec.dataSchema.granularitySpec.queryGranularity',
                  inputData.queryGranularity,
                );
              }

              if (inputData.columns) {
                const aggregators = inputData.aggregators || {};
                newSpec = deepSet(
                  newSpec,
                  'spec.dataSchema.dimensionsSpec.dimensions',
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
                  'spec.dataSchema.metricsSpec',
                  Object.values(inputData.aggregators),
                );
              }

              this.updateSpec(fillDataSourceNameIfNeeded(newSpec));
            } else {
              const sampleLines = filterMap(inputQueryState.data.data, l =>
                l.input ? l.input.raw : undefined,
              );

              const issue = issueWithSampleData(sampleLines);
              if (issue) {
                AppToaster.show({
                  icon: IconNames.WARNING_SIGN,
                  intent: Intent.WARNING,
                  message: issue,
                  timeout: 10000,
                });
                return false;
              }

              this.updateSpec(
                fillDataSourceNameIfNeeded(fillInputFormatIfNeeded(spec, sampleLines)),
              );
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
    const inputFormatColumns: string[] =
      deepGet(spec, 'spec.ioConfig.inputFormat.columns') || EMPTY_ARRAY;

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
        data: headerAndRowsFromSampleResponse({
          sampleResponse,
          ignoreTimeColumn: true,
          columnOrder: inputFormatColumns,
        }),
        lastData: parserQueryState.getSomeData(),
      }),
    }));
  }

  renderParserStep() {
    const {
      specPreview: spec,
      columnFilter,
      specialColumnsOnly,
      parserQueryState,
      selectedFlattenField,
    } = this.state;
    const inputFormat: InputFormat = deepGet(spec, 'spec.ioConfig.inputFormat') || EMPTY_OBJECT;
    const flattenFields: FlattenField[] =
      deepGet(spec, 'spec.ioConfig.inputFormat.flattenSpec.fields') || EMPTY_ARRAY;

    const canFlatten = inputFormatCanFlatten(inputFormat);

    let mainFill: JSX.Element | string = '';
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
            {canFlatten && (
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
              sampleData={data}
              columnFilter={columnFilter}
              canFlatten={canFlatten}
              flattenedColumnsOnly={specialColumnsOnly}
              flattenFields={flattenFields}
              onFlattenFieldSelect={this.onFlattenFieldSelect}
            />
          )}
          {parserQueryState.isLoading() && <Loader />}
          {parserQueryState.error && (
            <CenterMessage>{`Error: ${parserQueryState.getErrorMessage()}`}</CenterMessage>
          )}
        </div>
      );
    }

    let suggestedFlattenFields: FlattenField[] | undefined;
    if (canFlatten && !flattenFields.length && parserQueryState.data) {
      suggestedFlattenFields = computeFlattenPathsForData(
        filterMap(parserQueryState.data.rows, r => r.input),
        'path',
        'ignore-arrays',
      );
    }

    return (
      <>
        <div className="main">{mainFill}</div>
        <div className="control">
          <ParserMessage canFlatten={canFlatten} />
          {!selectedFlattenField && (
            <>
              <AutoForm
                fields={INPUT_FORMAT_FIELDS}
                model={inputFormat}
                onChange={p =>
                  this.updateSpecPreview(deepSet(spec, 'spec.ioConfig.inputFormat', p))
                }
              />
              {this.renderApplyButtonBar(
                parserQueryState,
                AutoForm.issueWithModel(inputFormat, INPUT_FORMAT_FIELDS),
              )}
            </>
          )}
          {this.renderFlattenControls()}
          {suggestedFlattenFields && suggestedFlattenFields.length ? (
            <FormGroup>
              <Button
                icon={IconNames.LIGHTBULB}
                text={`Auto add ${pluralIfNeeded(suggestedFlattenFields.length, 'flatten spec')}`}
                onClick={() => {
                  this.updateSpec(
                    deepSet(
                      spec,
                      'spec.ioConfig.inputFormat.flattenSpec.fields',
                      suggestedFlattenFields,
                    ),
                  );
                }}
              />
            </FormGroup>
          ) : (
            undefined
          )}
        </div>
        {this.renderNextBar({
          disabled: !parserQueryState.data,
          onNextStep: () => {
            if (!parserQueryState.data) return false;
            let possibleTimestampSpec: TimestampSpec;
            if (isDruidSource(spec)) {
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
                'spec.dataSchema.timestampSpec',
                possibleTimestampSpec,
              );
              this.updateSpec(newSpec);
            }

            return true;
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
    const inputFormat: InputFormat = deepGet(spec, 'spec.ioConfig.inputFormat') || EMPTY_OBJECT;
    if (!inputFormatCanFlatten(inputFormat)) return;

    const close = () => {
      this.setState({
        selectedFlattenFieldIndex: -1,
        selectedFlattenField: undefined,
      });
    };

    if (selectedFlattenField) {
      return (
        <div className="edit-controls">
          <AutoForm
            fields={FLATTEN_FIELD_FIELDS}
            model={selectedFlattenField}
            onChange={f => this.setState({ selectedFlattenField: f })}
          />
          <div className="control-buttons">
            <Button
              text="Apply"
              intent={Intent.PRIMARY}
              onClick={() => {
                this.updateSpec(
                  deepSet(
                    spec,
                    `spec.ioConfig.inputFormat.flattenSpec.fields.${selectedFlattenFieldIndex}`,
                    selectedFlattenField,
                  ),
                );
                close();
              }}
            />
            <Button text="Cancel" onClick={close} />
            {selectedFlattenFieldIndex !== -1 && (
              <Button
                className="right"
                icon={IconNames.TRASH}
                intent={Intent.DANGER}
                onClick={() => {
                  this.updateSpec(
                    deepDelete(
                      spec,
                      `spec.ioConfig.inputFormat.flattenSpec.fields.${selectedFlattenFieldIndex}`,
                    ),
                  );
                  close();
                }}
              />
            )}
          </div>
        </div>
      );
    } else {
      return (
        <FormGroup>
          <Button
            text="Add column flattening"
            disabled={!this.isPreviewSpecSame()}
            onClick={() => {
              this.setState({
                selectedFlattenField: { type: 'path', name: '', expr: '' },
                selectedFlattenFieldIndex: -1,
              });
            }}
          />
          <AnchorButton
            icon={IconNames.INFO_SIGN}
            href={`${getLink('DOCS')}/ingestion/flatten-json.html`}
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
    const inputFormatColumns: string[] =
      deepGet(spec, 'spec.ioConfig.inputFormat.columns') || EMPTY_ARRAY;

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
          headerAndRows: headerAndRowsFromSampleResponse({
            sampleResponse,
            columnOrder: ['__time'].concat(inputFormatColumns),
          }),
          spec,
        },
        lastData: timestampQueryState.getSomeData(),
      }),
    }));
  }

  renderTimestampStep() {
    const { specPreview: spec, columnFilter, specialColumnsOnly, timestampQueryState } = this.state;
    const timestampSchema = getTimestampSchema(spec);
    const timestampSpec: TimestampSpec =
      deepGet(spec, 'spec.dataSchema.timestampSpec') || EMPTY_OBJECT;
    const transforms: Transform[] =
      deepGet(spec, 'spec.dataSchema.transformSpec.transforms') || EMPTY_ARRAY;

    let mainFill: JSX.Element | string = '';
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
                data.headerAndRows,
                timestampSpec,
              )}
              onTimestampColumnSelect={this.onTimestampColumnSelect}
            />
          )}
          {timestampQueryState.isLoading() && <Loader />}
          {timestampQueryState.error && (
            <CenterMessage>{`Error: ${timestampQueryState.getErrorMessage()}`}</CenterMessage>
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
                      'spec.dataSchema.transformSpec.transforms': removeTimestampTransform(
                        transforms,
                      ),
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
                      'spec.dataSchema.transformSpec.transforms': removeTimestampTransform(
                        transforms,
                      ),
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

  private onTimestampColumnSelect = (newTimestampSpec: TimestampSpec) => {
    const { specPreview } = this.state;
    this.updateSpecPreview(deepSet(specPreview, 'spec.dataSchema.timestampSpec', newTimestampSpec));
  };

  // ==================================================================

  async queryForTransform(initRun = false) {
    const { spec, cacheRows } = this.state;
    const inputFormatColumns: string[] =
      deepGet(spec, 'spec.ioConfig.inputFormat.columns') || EMPTY_ARRAY;

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
        data: headerAndRowsFromSampleResponse({
          sampleResponse,
          columnOrder: ['__time'].concat(inputFormatColumns),
        }),
        lastData: transformQueryState.getSomeData(),
      }),
    }));
  }

  renderTransformStep() {
    const {
      spec,
      columnFilter,
      specialColumnsOnly,
      transformQueryState,
      selectedTransform,
      // selectedTransformIndex,
    } = this.state;
    const transforms: Transform[] =
      deepGet(spec, 'spec.dataSchema.transformSpec.transforms') || EMPTY_ARRAY;

    let mainFill: JSX.Element | string = '';
    if (transformQueryState.isInit()) {
      mainFill = <CenterMessage>{`Please fill in the previous steps`}</CenterMessage>;
    } else {
      const data = transformQueryState.getSomeData();
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
          {data && (
            <TransformTable
              sampleData={data}
              columnFilter={columnFilter}
              transformedColumnsOnly={specialColumnsOnly}
              transforms={transforms}
              selectedColumnName={transformTableSelectedColumnName(data, selectedTransform)}
              onTransformSelect={this.onTransformSelect}
            />
          )}
          {transformQueryState.isLoading() && <Loader />}
          {transformQueryState.error && (
            <CenterMessage>{`Error: ${transformQueryState.getErrorMessage()}`}</CenterMessage>
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
                    selectedTransformIndex: transforms.length - 1,
                    selectedTransform: transforms[transforms.length - 1],
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

            let newSpec = spec;
            if (!deepGet(newSpec, 'spec.dataSchema.dimensionsSpec')) {
              const currentRollup = deepGet(newSpec, 'spec.dataSchema.granularitySpec.rollup');
              newSpec = updateSchemaWithSample(
                newSpec,
                transformQueryState.data,
                'specific',
                typeof currentRollup === 'boolean' ? currentRollup : DEFAULT_ROLLUP_SETTING,
              );
            }

            this.updateSpec(newSpec);
            return true;
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

    if (selectedTransform) {
      return (
        <div className="edit-controls">
          <AutoForm
            fields={TRANSFORM_FIELDS}
            model={selectedTransform}
            onChange={selectedTransform => this.setState({ selectedTransform })}
          />
          <div className="control-buttons">
            <Button
              text="Apply"
              intent={Intent.PRIMARY}
              onClick={() => {
                this.updateSpec(
                  deepSet(
                    spec,
                    `spec.dataSchema.transformSpec.transforms.${selectedTransformIndex}`,
                    selectedTransform,
                  ),
                );
                close();
              }}
            />
            <Button text="Cancel" onClick={close} />
            {selectedTransformIndex !== -1 && (
              <Button
                className="right"
                icon={IconNames.TRASH}
                intent={Intent.DANGER}
                onClick={() => {
                  this.updateSpec(
                    deepDelete(
                      spec,
                      `spec.dataSchema.transformSpec.transforms.${selectedTransformIndex}`,
                    ),
                  );
                  close();
                }}
              />
            )}
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
    const { spec, cacheRows } = this.state;
    const inputFormatColumns: string[] =
      deepGet(spec, 'spec.ioConfig.inputFormat.columns') || EMPTY_ARRAY;

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
          data: headerAndRowsFromSampleResponse({
            sampleResponse,
            columnOrder: ['__time'].concat(inputFormatColumns),
            parsedOnly: true,
          }),
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

    const headerAndRowsNoFilter = headerAndRowsFromSampleResponse({
      sampleResponse: sampleResponseNoFilter,
      columnOrder: ['__time'].concat(inputFormatColumns),
      parsedOnly: true,
    });

    this.setState(({ filterQueryState }) => ({
      // cacheRows: sampleResponseNoFilter.cacheKey,
      filterQueryState: new QueryState({
        data: deepSet(headerAndRowsNoFilter, 'rows', []),
        lastData: filterQueryState.getSomeData(),
      }),
    }));
  }

  private getMemoizedDimensionFiltersFromSpec = memoize(spec => {
    const { dimensionFilters } = splitFilter(deepGet(spec, 'spec.dataSchema.transformSpec.filter'));
    return dimensionFilters;
  });

  renderFilterStep() {
    const { spec, specPreview, columnFilter, filterQueryState, selectedFilter } = this.state;
    const dimensionFilters = this.getMemoizedDimensionFiltersFromSpec(spec);

    let mainFill: JSX.Element | string = '';
    if (filterQueryState.isInit()) {
      mainFill = <CenterMessage>Please enter more details for the previous steps</CenterMessage>;
    } else {
      const data = filterQueryState.getSomeData();
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
            <FilterTable
              sampleData={data}
              columnFilter={columnFilter}
              dimensionFilters={dimensionFilters}
              selectedFilterName={filterTableSelectedColumnName(data, selectedFilter)}
              onFilterSelect={this.onFilterSelect}
            />
          )}
          {filterQueryState.isLoading() && <Loader />}
          {filterQueryState.error && (
            <CenterMessage>{`Error: ${filterQueryState.getErrorMessage()}`}</CenterMessage>
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
              <AutoForm
                fields={FILTERS_FIELDS}
                model={specPreview}
                onChange={this.updateSpecPreview}
              />
              {this.renderApplyButtonBar(filterQueryState, undefined)}
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
            </>
          )}
          {this.renderColumnFilterControls()}
        </div>
        {this.renderNextBar({})}
      </>
    );
  }

  private onFilterSelect = (filter: DruidFilter, index: number) => {
    this.setState({
      selectedFilterIndex: index,
      selectedFilter: filter,
    });
  };

  renderColumnFilterControls() {
    const { spec, selectedFilter, selectedFilterIndex } = this.state;
    if (!selectedFilter) return;

    const close = () => {
      this.setState({
        selectedFilterIndex: -1,
        selectedFilter: undefined,
      });
    };

    return (
      <div className="edit-controls">
        <AutoForm
          fields={FILTER_FIELDS}
          model={selectedFilter}
          onChange={f => this.setState({ selectedFilter: f })}
          showCustom={f => !KNOWN_FILTER_TYPES.includes(f.type)}
        />
        <div className="control-buttons">
          <Button
            text="Apply"
            intent={Intent.PRIMARY}
            onClick={() => {
              const curFilter = splitFilter(deepGet(spec, 'spec.dataSchema.transformSpec.filter'));
              const newFilter = joinFilter(
                deepSet(curFilter, `dimensionFilters.${selectedFilterIndex}`, selectedFilter),
              );
              this.updateSpec(deepSet(spec, 'spec.dataSchema.transformSpec.filter', newFilter));
              close();
            }}
          />
          <Button text="Cancel" onClick={close} />
          {selectedFilterIndex !== -1 && (
            <Button
              className="right"
              icon={IconNames.TRASH}
              intent={Intent.DANGER}
              onClick={() => {
                const curFilter = splitFilter(
                  deepGet(spec, 'spec.dataSchema.transformSpec.filter'),
                );
                const newFilter = joinFilter(
                  deepDelete(curFilter, `dimensionFilters.${selectedFilterIndex}`),
                );
                this.updateSpec(deepSet(spec, 'spec.dataSchema.transformSpec.filter', newFilter));
                close();
              }}
            />
          )}
        </div>
      </div>
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
          headerAndRows: headerAndRowsFromSampleResponse({
            sampleResponse,
            columnOrder: ['__time'].concat(dimensions ? dimensions.map(getDimensionSpecName) : []),
            suffixColumnOrder: metricsSpec ? metricsSpec.map(getMetricSpecName) : undefined,
          }),
          dimensions,
          metricsSpec,
        },
        lastData: schemaQueryState.getSomeData(),
      }),
    }));
  }

  renderSchemaStep() {
    const {
      specPreview: spec,
      columnFilter,
      schemaQueryState,
      selectedAutoDimension,
      selectedDimensionSpec,
      selectedDimensionSpecIndex,
      selectedMetricSpec,
      selectedMetricSpecIndex,
    } = this.state;
    const rollup: boolean = Boolean(deepGet(spec, 'spec.dataSchema.granularitySpec.rollup'));
    const somethingSelected = Boolean(
      selectedAutoDimension || selectedDimensionSpec || selectedMetricSpec,
    );
    const dimensionMode = getDimensionMode(spec);

    let mainFill: JSX.Element | string = '';
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
              selectedDimensionSpecIndex={selectedDimensionSpecIndex}
              selectedMetricSpecIndex={selectedMetricSpecIndex}
              onAutoDimensionSelect={this.onAutoDimensionSelect}
              onDimensionSelect={this.onDimensionSelect}
              onMetricSelect={this.onMetricSelect}
            />
          )}
          {schemaQueryState.isLoading() && <Loader />}
          {schemaQueryState.error && (
            <CenterMessage>{`Error: ${schemaQueryState.getErrorMessage()}`}</CenterMessage>
          )}
        </div>
      );
    }

    return (
      <>
        <div className="main">{mainFill}</div>
        <div className="control">
          <SchemaMessage dimensionMode={dimensionMode} />
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
                  checked={dimensionMode === 'specific'}
                  onChange={() =>
                    this.setState({
                      newDimensionMode: dimensionMode === 'specific' ? 'auto-detect' : 'specific',
                    })
                  }
                  label="Explicitly specify dimension list"
                />
              </FormGroupWithInfo>
              {dimensionMode === 'auto-detect' && (
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
                  disabled={dimensionMode !== 'specific'}
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
            </>
          )}
          {this.renderAutoDimensionControls()}
          {this.renderDimensionSpecControls()}
          {this.renderMetricSpecControls()}
          {this.renderChangeRollupAction()}
          {this.renderChangeDimensionModeAction()}
        </div>
        {this.renderNextBar({
          disabled: !schemaQueryState.data,
        })}
      </>
    );
  }

  private onAutoDimensionSelect = (selectedAutoDimension: string) => {
    this.setState({
      selectedAutoDimension,
      selectedDimensionSpec: undefined,
      selectedDimensionSpecIndex: -1,
      selectedMetricSpec: undefined,
      selectedMetricSpecIndex: -1,
    });
  };

  private onDimensionSelect = (
    selectedDimensionSpec: DimensionSpec | undefined,
    selectedDimensionSpecIndex: number,
  ) => {
    this.setState({
      selectedAutoDimension: undefined,
      selectedDimensionSpec,
      selectedDimensionSpecIndex,
      selectedMetricSpec: undefined,
      selectedMetricSpecIndex: -1,
    });
  };

  private onMetricSelect = (
    selectedMetricSpec: MetricSpec | undefined,
    selectedMetricSpecIndex: number,
  ) => {
    this.setState({
      selectedAutoDimension: undefined,
      selectedDimensionSpec: undefined,
      selectedDimensionSpecIndex: -1,
      selectedMetricSpec,
      selectedMetricSpecIndex,
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
              headerAndRowsFromSampleResponse({ sampleResponse }),
              getDimensionMode(spec),
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

  renderChangeDimensionModeAction() {
    const { newDimensionMode, spec, cacheRows } = this.state;
    if (typeof newDimensionMode === 'undefined' || !cacheRows) return;
    const autoDetect = newDimensionMode === 'auto-detect';

    return (
      <AsyncActionDialog
        action={async () => {
          const sampleResponse = await sampleForTransform(spec, cacheRows);
          this.updateSpec(
            updateSchemaWithSample(
              spec,
              headerAndRowsFromSampleResponse({ sampleResponse }),
              newDimensionMode,
              getRollup(spec),
            ),
          );
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

  renderAutoDimensionControls() {
    const { spec, selectedAutoDimension } = this.state;
    if (!selectedAutoDimension) return;

    const close = () => {
      this.setState({
        selectedAutoDimension: undefined,
      });
    };

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
              close();
            }}
          />
        </FormGroup>
        <FormGroup>
          <Button text="Close" onClick={close} />
        </FormGroup>
      </div>
    );
  }

  renderDimensionSpecControls() {
    const { spec, selectedDimensionSpec, selectedDimensionSpecIndex } = this.state;
    if (!selectedDimensionSpec) return;
    const dimensionMode = getDimensionMode(spec);

    const close = () => {
      this.setState({
        selectedDimensionSpecIndex: -1,
        selectedDimensionSpec: undefined,
      });
    };

    const dimensions = deepGet(spec, `spec.dataSchema.dimensionsSpec.dimensions`) || EMPTY_ARRAY;

    const moveTo = (newIndex: number) => {
      const newDimension = moveElement(dimensions, selectedDimensionSpecIndex, newIndex);
      const newSpec = deepSet(spec, `spec.dataSchema.dimensionsSpec.dimensions`, newDimension);
      this.updateSpec(newSpec);
      close();
    };

    const reorderDimensionMenu = (
      <ReorderMenu things={dimensions} selectedIndex={selectedDimensionSpecIndex} moveTo={moveTo} />
    );

    const convertToMetric = (type: string, prefix: string) => {
      const specWithoutDimension =
        dimensionMode === 'specific'
          ? deepDelete(
              spec,
              `spec.dataSchema.dimensionsSpec.dimensions.${selectedDimensionSpecIndex}`,
            )
          : spec;

      const specWithMetric = deepSet(specWithoutDimension, `spec.dataSchema.metricsSpec.[append]`, {
        name: `${prefix}_${selectedDimensionSpec.name}`,
        type,
        fieldName: selectedDimensionSpec.name,
      });

      this.updateSpec(specWithMetric);
      close();
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
      <div className="edit-controls">
        <AutoForm
          fields={DIMENSION_SPEC_FIELDS}
          model={selectedDimensionSpec}
          onChange={selectedDimensionSpec => this.setState({ selectedDimensionSpec })}
        />
        {reorderDimensionMenu && (
          <FormGroup>
            <Popover content={reorderDimensionMenu}>
              <Button
                icon={IconNames.ARROWS_HORIZONTAL}
                text="Reorder dimension"
                rightIcon={IconNames.CARET_DOWN}
              />
            </Popover>
          </FormGroup>
        )}
        {selectedDimensionSpecIndex !== -1 && deepGet(spec, 'spec.dataSchema.metricsSpec') && (
          <FormGroup>
            <Popover content={convertToMetricMenu}>
              <Button
                icon={IconNames.EXCHANGE}
                text="Convert to metric"
                rightIcon={IconNames.CARET_DOWN}
                disabled={dimensions.length <= 1}
              />
            </Popover>
          </FormGroup>
        )}
        <div className="control-buttons">
          <Button
            text="Apply"
            intent={Intent.PRIMARY}
            onClick={() => {
              this.updateSpec(
                deepSet(
                  spec,
                  `spec.dataSchema.dimensionsSpec.dimensions.${selectedDimensionSpecIndex}`,
                  selectedDimensionSpec,
                ),
              );
              close();
            }}
          />
          <Button text="Cancel" onClick={close} />
          {selectedDimensionSpecIndex !== -1 && (
            <Button
              className="right"
              icon={IconNames.TRASH}
              intent={Intent.DANGER}
              disabled={dimensions.length <= 1}
              onClick={() => {
                if (dimensions.length <= 1) return; // Guard against removing the last dimension

                this.updateSpec(
                  deepDelete(
                    spec,
                    `spec.dataSchema.dimensionsSpec.dimensions.${selectedDimensionSpecIndex}`,
                  ),
                );
                close();
              }}
            />
          )}
        </div>
      </div>
    );
  }

  renderMetricSpecControls() {
    const { spec, selectedMetricSpec, selectedMetricSpecIndex } = this.state;
    if (!selectedMetricSpec) return;
    const dimensionMode = getDimensionMode(spec);

    const close = () => {
      this.setState({
        selectedMetricSpecIndex: -1,
        selectedMetricSpec: undefined,
      });
    };

    const convertToDimension = (type: string) => {
      const specWithoutMetric = deepDelete(
        spec,
        `spec.dataSchema.metricsSpec.${selectedMetricSpecIndex}`,
      );

      const specWithDimension = deepSet(
        specWithoutMetric,
        `spec.dataSchema.dimensionsSpec.dimensions.[append]`,
        {
          type,
          name: selectedMetricSpec.fieldName,
        },
      );

      this.updateSpec(specWithDimension);
      close();
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
      <div className="edit-controls">
        <AutoForm
          fields={METRIC_SPEC_FIELDS}
          model={selectedMetricSpec}
          onChange={selectedMetricSpec => this.setState({ selectedMetricSpec })}
        />
        {selectedMetricSpecIndex !== -1 && dimensionMode === 'specific' && (
          <FormGroup>
            <Popover content={convertToDimensionMenu}>
              <Button
                icon={IconNames.EXCHANGE}
                text="Convert to dimension"
                rightIcon={IconNames.CARET_DOWN}
              />
            </Popover>
          </FormGroup>
        )}
        <div className="control-buttons">
          <Button
            text="Apply"
            intent={Intent.PRIMARY}
            onClick={() => {
              this.updateSpec(
                deepSet(
                  spec,
                  `spec.dataSchema.metricsSpec.${selectedMetricSpecIndex}`,
                  selectedMetricSpec,
                ),
              );
              close();
            }}
          />
          <Button text="Cancel" onClick={close} />
          {selectedMetricSpecIndex !== -1 && (
            <Button
              className="right"
              icon={IconNames.TRASH}
              intent={Intent.DANGER}
              onClick={() => {
                this.updateSpec(
                  deepDelete(spec, `spec.dataSchema.metricsSpec.${selectedMetricSpecIndex}`),
                );
                close();
              }}
            />
          )}
        </div>
      </div>
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
    const dimensionNames = dimensions ? dimensions.map(getDimensionSpecName) : undefined;

    let nonsensicalSingleDimPartitioningMessage: JSX.Element | undefined;
    const partitionDimension = deepGet(tuningConfig, 'partitionsSpec.partitionDimension');
    if (
      dimensions &&
      Array.isArray(dimensionNames) &&
      dimensionNames.length &&
      deepGet(tuningConfig, 'partitionsSpec.type') === 'single_dim' &&
      typeof partitionDimension === 'string' &&
      partitionDimension !== dimensionNames[0]
    ) {
      const firstDimensionName = dimensionNames[0];
      nonsensicalSingleDimPartitioningMessage = (
        <FormGroup>
          <Callout intent={Intent.WARNING}>
            <p>Your partitioning and sorting configuration does not make sense.</p>
            <p>
              For best performance the first dimension in your schema (
              <Code>{firstDimensionName}</Code>), which is what the data will be primarily sorted
              on, should match the partitioning dimension (<Code>{partitionDimension}</Code>).
            </p>
            <p>
              <Button
                intent={Intent.WARNING}
                text={`Put '${partitionDimension}' first in the dimensions list`}
                onClick={() => {
                  this.updateSpec(
                    deepSet(
                      spec,
                      'spec.dataSchema.dimensionsSpec.dimensions',
                      moveElement(
                        dimensions,
                        dimensions.findIndex(d => getDimensionSpecName(d) === partitionDimension),
                        0,
                      ),
                    ),
                  );
                }}
              />
            </p>
            <p>
              <Button
                intent={Intent.WARNING}
                text={`Partition on '${firstDimensionName}' instead`}
                onClick={() => {
                  this.updateSpec(
                    deepSet(
                      spec,
                      'spec.tuningConfig.partitionsSpec.partitionDimension',
                      firstDimensionName,
                    ),
                  );
                }}
              />
            </p>
          </Callout>
        </FormGroup>
      );
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
                defaultValue: false,
                defined: spec =>
                  deepGet(spec, 'spec.tuningConfig.partitionsSpec.type') === 'dynamic',
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
        </div>
        {this.renderNextBar({})}
      </>
    );
  }

  // ==================================================================
  private getSupervisorJson = async (): Promise<void> => {
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

  private getTaskJson = async (): Promise<void> => {
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

  renderSpecStep() {
    const { spec, submitting } = this.state;
    const issueWithSpec = getIssueWithSpec(spec);

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
            disabled={submitting || Boolean(issueWithSpec)}
            onClick={this.handleSubmit}
          />
        </div>
      </>
    );
  }

  private handleSubmit = async () => {
    const { goToIngestion } = this.props;
    const { spec, submitting } = this.state;
    if (submitting) return;

    this.setState({ submitting: true });
    if (isTask(spec)) {
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
        goToIngestion(taskResp.data.task);
      }, 1000);
    } else {
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
        goToIngestion(undefined); // Can we get the supervisor ID here?
      }, 1000);
    }
  };
}
