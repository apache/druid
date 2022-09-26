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

import { Button, Callout, Code, FormGroup, Intent } from '@blueprintjs/core';
import React from 'react';

import { ExternalLink, LearnMore } from '../../components';
import { DimensionMode, getIngestionDocLink, IngestionSpec } from '../../druid-models';
import { getLink } from '../../links';
import { deepGet, deepSet } from '../../utils';

export interface ConnectMessageProps {
  inlineMode: boolean;
  spec: Partial<IngestionSpec>;
}

export const ConnectMessage = React.memo(function ConnectMessage(props: ConnectMessageProps) {
  const { inlineMode, spec } = props;

  return (
    <FormGroup>
      <Callout>
        <p>
          Druid ingests raw data and converts it into a custom,{' '}
          <ExternalLink href={`${getLink('DOCS')}/design/segments.html`}>
            indexed format
          </ExternalLink>{' '}
          that is optimized for analytic queries.
        </p>
        {inlineMode ? (
          <>
            <p>To get started, please paste some data in the box to the left.</p>
            <p>Click &quot;Apply&quot; to verify your data with Druid.</p>
          </>
        ) : (
          <p>To get started, please specify what data you want to ingest.</p>
        )}
        <LearnMore href={getIngestionDocLink(spec)} />
      </Callout>
    </FormGroup>
  );
});

export interface ParserMessageProps {
  canFlatten: boolean;
}

export const ParserMessage = React.memo(function ParserMessage(props: ParserMessageProps) {
  const { canFlatten } = props;

  return (
    <FormGroup>
      <Callout>
        <p>
          Druid requires flat data (non-nested, non-hierarchical). Each row should represent a
          discrete event.
        </p>
        {canFlatten && (
          <p>
            If you have nested data, you can{' '}
            <ExternalLink href={`${getLink('DOCS')}/ingestion/index.html#flattenspec`}>
              flatten
            </ExternalLink>{' '}
            it here. If the provided flattening capabilities are not sufficient, please pre-process
            your data before ingesting it into Druid.
          </p>
        )}
        <p>Ensure that your data appears correctly in a row/column orientation.</p>
        <LearnMore href={`${getLink('DOCS')}/ingestion/data-formats.html`} />
      </Callout>
    </FormGroup>
  );
});

export const TimestampMessage = React.memo(function TimestampMessage() {
  return (
    <FormGroup>
      <Callout>
        <p>
          Druid partitions data based on the primary time column of your data. This column is stored
          internally in Druid as <Code>__time</Code>.
        </p>
        <p>Configure how to define the time column for this data.</p>
        <p>
          If your data does not have a time column, you can select <Code>None</Code> to use a
          placeholder value. If the time information is spread across multiple columns you can
          combine them into one by selecting <Code>Expression</Code> and defining a transform
          expression.
        </p>
        <LearnMore href={`${getLink('DOCS')}/ingestion/index.html#timestampspec`} />
      </Callout>
    </FormGroup>
  );
});

export const TransformMessage = React.memo(function TransformMessage() {
  return (
    <FormGroup>
      <Callout>
        <p>
          Druid can perform per-row{' '}
          <ExternalLink href={`${getLink('DOCS')}/ingestion/transform-spec.html#transforms`}>
            transforms
          </ExternalLink>{' '}
          of column values allowing you to create new derived columns or alter existing column.
        </p>
        <LearnMore href={`${getLink('DOCS')}/ingestion/index.html#transforms`} />
      </Callout>
    </FormGroup>
  );
});

export const FilterMessage = React.memo(function FilterMessage() {
  return (
    <FormGroup>
      <Callout>
        <p>
          Druid can filter out unwanted data by applying per-row{' '}
          <ExternalLink href={`${getLink('DOCS')}/querying/filters.html`}>filters</ExternalLink>.
        </p>
        <LearnMore href={`${getLink('DOCS')}/ingestion/index.html#filter`} />
      </Callout>
    </FormGroup>
  );
});

export interface SchemaMessageProps {
  dimensionMode: DimensionMode;
}

export const SchemaMessage = React.memo(function SchemaMessage(props: SchemaMessageProps) {
  const { dimensionMode } = props;

  return (
    <FormGroup>
      <Callout>
        <p>
          Each column in Druid must have an assigned type (string, long, float, double, complex,
          etc).
        </p>
        {dimensionMode === 'specific' && (
          <p>
            Default primitive types have been automatically assigned to your columns. If you want to
            change the type, click on the column header.
          </p>
        )}
        <LearnMore href={`${getLink('DOCS')}/ingestion/schema-design.html`} />
      </Callout>
    </FormGroup>
  );
});

export const PartitionMessage = React.memo(function PartitionMessage() {
  return (
    <FormGroup>
      <Callout>
        <p>Configure how Druid will partition data.</p>
        <p>
          Druid datasources are always partitioned by time into time chunks (
          <Code>Primary partitioning</Code>), and each time chunk contains one or more segments (
          <Code>Secondary partitioning</Code>).
        </p>
        <LearnMore href={`${getLink('DOCS')}/ingestion/index.html#partitioning`} />
      </Callout>
    </FormGroup>
  );
});

export const TuningMessage = React.memo(function TuningMessage() {
  return (
    <FormGroup>
      <Callout>
        <p>Fine tune how Druid will ingest data.</p>
        <LearnMore href={`${getLink('DOCS')}/ingestion/index.html#tuningconfig`} />
      </Callout>
    </FormGroup>
  );
});

export const PublishMessage = React.memo(function PublishMessage() {
  return (
    <FormGroup>
      <Callout>
        <p>Configure behavior of indexed data once it reaches Druid.</p>
      </Callout>
    </FormGroup>
  );
});

export const SpecMessage = React.memo(function SpecMessage() {
  return (
    <FormGroup>
      <Callout>
        <p>
          Druid begins ingesting data once you submit a JSON ingestion spec. If you modify any
          values in this view, the values entered in previous sections will update accordingly. If
          you modify any values in previous sections, this spec will automatically update.
        </p>
        <p>Submit the spec to begin loading data into Druid.</p>
        <LearnMore href={`${getLink('DOCS')}/ingestion/index.html#ingestion-specs`} />
      </Callout>
    </FormGroup>
  );
});

export interface AppendToExistingIssueProps {
  spec: Partial<IngestionSpec>;
  onChangeSpec(newSpec: Partial<IngestionSpec>): void;
}

export const AppendToExistingIssue = React.memo(function AppendToExistingIssue(
  props: AppendToExistingIssueProps,
) {
  const { spec, onChangeSpec } = props;

  const partitionsSpecType = deepGet(spec, 'spec.tuningConfig.partitionsSpec.type');
  if (
    partitionsSpecType === 'dynamic' ||
    deepGet(spec, 'spec.ioConfig.appendToExisting') !== true
  ) {
    return null;
  }

  const dynamicPartitionSpec = {
    type: 'dynamic',
    maxRowsPerSegment:
      deepGet(spec, 'spec.tuningConfig.partitionsSpec.maxRowsPerSegment') ||
      deepGet(spec, 'spec.tuningConfig.partitionsSpec.targetRowsPerSegment'),
  };

  return (
    <FormGroup>
      <Callout intent={Intent.DANGER}>
        <p>
          Only <Code>dynamic</Code> partitioning supports <Code>appendToExisting: true</Code>. You
          have currently selected <Code>{partitionsSpecType}</Code> partitioning.
        </p>
        <Button
          intent={Intent.SUCCESS}
          onClick={() =>
            onChangeSpec(deepSet(spec, 'spec.tuningConfig.partitionsSpec', dynamicPartitionSpec))
          }
        >
          Change to <Code>dynamic</Code> partitioning
        </Button>
      </Callout>
    </FormGroup>
  );
});
