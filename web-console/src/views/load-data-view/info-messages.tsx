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

import { Callout, Code } from '@blueprintjs/core';
import React from 'react';

import { ExternalLink } from '../../components';
import { DimensionMode, getIngestionDocLink, IngestionSpec } from '../../druid-models';
import { getLink } from '../../links';

import { LearnMore } from './learn-more/learn-more';

export interface ConnectMessageProps {
  inlineMode: boolean;
  spec: IngestionSpec;
}

export const ConnectMessage = React.memo(function ConnectMessage(props: ConnectMessageProps) {
  const { inlineMode, spec } = props;

  return (
    <Callout className="intro">
      <p>
        Druid ingests raw data and converts it into a custom,{' '}
        <ExternalLink href={`${getLink('DOCS')}/design/segments.html`}>indexed format</ExternalLink>{' '}
        that is optimized for analytic queries.
      </p>
      {inlineMode ? (
        <>
          <p>To get started, please paste some data in the box to the left.</p>
          <p>Click "Apply" to verify your data with Druid.</p>
        </>
      ) : (
        <p>To get started, please specify what data you want to ingest.</p>
      )}
      <LearnMore href={getIngestionDocLink(spec)} />
    </Callout>
  );
});

export interface ParserMessageProps {
  canFlatten: boolean;
}

export const ParserMessage = React.memo(function ParserMessage(props: ParserMessageProps) {
  const { canFlatten } = props;

  return (
    <Callout className="intro">
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
  );
});

export const TimestampMessage = React.memo(function TimestampMessage() {
  return (
    <Callout className="intro">
      <p>
        Druid partitions data based on the primary time column of your data. This column is stored
        internally in Druid as <Code>__time</Code>.
      </p>
      <p>Configure how to define the time column for this data.</p>
      <p>
        If your data does not have a time column, you can select <Code>None</Code> to use a
        placeholder value. If the time information is spread across multiple columns you can combine
        them into one by selecting <Code>Expression</Code> and defining a transform expression.
      </p>
      <LearnMore href={`${getLink('DOCS')}/ingestion/index.html#timestampspec`} />
    </Callout>
  );
});

export const TransformMessage = React.memo(function TransformMessage() {
  return (
    <Callout className="intro">
      <p>
        Druid can perform per-row{' '}
        <ExternalLink href={`${getLink('DOCS')}/ingestion/transform-spec.html#transforms`}>
          transforms
        </ExternalLink>{' '}
        of column values allowing you to create new derived columns or alter existing column.
      </p>
      <LearnMore href={`${getLink('DOCS')}/ingestion/index.html#transforms`} />
    </Callout>
  );
});

export const FilterMessage = React.memo(function FilterMessage() {
  return (
    <Callout className="intro">
      <p>
        Druid can filter out unwanted data by applying per-row{' '}
        <ExternalLink href={`${getLink('DOCS')}/querying/filters.html`}>filters</ExternalLink>.
      </p>
      <LearnMore href={`${getLink('DOCS')}/ingestion/index.html#filter`} />
    </Callout>
  );
});

export interface SchemaMessageProps {
  dimensionMode: DimensionMode;
}

export const SchemaMessage = React.memo(function SchemaMessage(props: SchemaMessageProps) {
  const { dimensionMode } = props;

  return (
    <Callout className="intro">
      <p>
        Each column in Druid must have an assigned type (string, long, float, double, complex, etc).
      </p>
      {dimensionMode === 'specific' && (
        <p>
          Default primitive types have been automatically assigned to your columns. If you want to
          change the type, click on the column header.
        </p>
      )}
      <LearnMore href={`${getLink('DOCS')}/ingestion/schema-design.html`} />
    </Callout>
  );
});

export const PartitionMessage = React.memo(function PartitionMessage() {
  return (
    <Callout className="intro">
      <p>Configure how Druid will partition data.</p>
      <LearnMore href={`${getLink('DOCS')}/ingestion/index.html#partitioning`} />
    </Callout>
  );
});

export const TuningMessage = React.memo(function TuningMessage() {
  return (
    <Callout className="intro">
      <p>Fine tune how Druid will ingest data.</p>
      <LearnMore href={`${getLink('DOCS')}/ingestion/index.html#tuningconfig`} />
    </Callout>
  );
});

export const PublishMessage = React.memo(function PublishMessage() {
  return (
    <Callout className="intro">
      <p>Configure behavior of indexed data once it reaches Druid.</p>
    </Callout>
  );
});

export const SpecMessage = React.memo(function SpecMessage() {
  return (
    <Callout className="intro">
      <p>
        Druid begins ingesting data once you submit a JSON ingestion spec. If you modify any values
        in this view, the values entered in previous sections will update accordingly. If you modify
        any values in previous sections, this spec will automatically update.
      </p>
      <p>Submit the spec to begin loading data into Druid.</p>
      <LearnMore href={`${getLink('DOCS')}/ingestion/index.html#ingestion-specs`} />
    </Callout>
  );
});
