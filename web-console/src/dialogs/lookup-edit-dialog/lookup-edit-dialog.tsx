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
  Classes,
  Dialog,
  FormGroup,
  HTMLSelect,
  InputGroup,
  Intent,
} from '@blueprintjs/core';
import React from 'react';

import { AutoForm } from '../../components';
// import { validJson } from '../../utils';

import './lookup-edit-dialog.scss';
// import AceEditor from 'react-ace';

// import {SnitchDialog} from "..";

export interface ExtractionNamespaceSpec {
  type?: string;
  uri?: string;
  uriPrefix?: string;
  namespaceParseSpec: NamespaceParseSpec;
  namespace?: string;
  connectorConfig?: string;
  table?: string;
  keyColumn?: string;
  valueColumn?: string;
  filter?: any;
  tsColumn?: string;
  pollPeriod?: string;
}

export interface NamespaceParseSpec {
  format: string;
  columns?: string[];
  keyColumn?: string;
  valueColumn?: string;
  hasHeaderRow?: boolean;
  skipHeaderRows?: number;
  keyFieldName?: string;
  valueFieldName?: string;
  delimiter?: string;
  listDelimiter?: string;
}

export interface LookupSpec {
  type: string;
  map?: {};
  extractionNamespace?: ExtractionNamespaceSpec;
  firstCacheTimeout?: number;
  injective?: boolean;
}
export interface LookupEditDialogProps {
  onClose: () => void;
  onSubmit: (updateLookupVersion: boolean) => void;
  onChange: (field: string, value: string | LookupSpec) => void;
  lookupName: string;
  lookupTier: string;
  lookupVersion: string;
  lookupSpec: LookupSpec;
  isEdit: boolean;
  allLookupTiers: string[];
}

export const LookupEditDialog = React.memo(function LookupEditDialog(props: LookupEditDialogProps) {
  const {
    onClose,
    onSubmit,
    lookupSpec,
    lookupTier,
    lookupName,
    lookupVersion,
    onChange,
    isEdit,
    allLookupTiers,
  } = props;

  let updateVersionOnSubmit = true;

  function addISOVersion() {
    const currentDate = new Date();
    const ISOString = currentDate.toISOString();
    onChange('lookupEditVersion', ISOString);
  }

  function renderTierInput() {
    if (isEdit) {
      return (
        <FormGroup className="lookup-label" label="Tier">
          <InputGroup
            value={lookupTier}
            onChange={(e: any) => {
              updateVersionOnSubmit = false;
              onChange('lookupEditTier', e.target.value);
            }}
            disabled
          />
        </FormGroup>
      );
    } else {
      return (
        <FormGroup className="lookup-label" label="Tier">
          <HTMLSelect
            disabled={isEdit}
            value={lookupTier}
            onChange={(e: any) => onChange('lookupEditTier', e.target.value)}
          >
            {allLookupTiers.map(tier => (
              <option key={tier} value={tier}>
                {tier}
              </option>
            ))}
          </HTMLSelect>
        </FormGroup>
      );
    }
  }

  let disableSubmit =
    lookupName === '' ||
    lookupVersion === '' ||
    lookupTier === '' ||
    lookupSpec.type === '' ||
    lookupSpec.type === undefined ||
    (lookupSpec.type === 'map' && lookupSpec.map === undefined) ||
    (lookupSpec.type === 'cachedNamespace' && lookupSpec.extractionNamespace === undefined);

  if (!disableSubmit && lookupSpec.type === 'cachedNamespace' && lookupSpec.extractionNamespace) {
    const namespaceParseSpec = lookupSpec.extractionNamespace.namespaceParseSpec;

    switch (lookupSpec.extractionNamespace.type) {
      case 'uri':
        disableSubmit = !lookupSpec.extractionNamespace.namespaceParseSpec;
        if (!disableSubmit) break;
        switch (namespaceParseSpec.format) {
          case 'csv':
            disableSubmit = !namespaceParseSpec.columns && !namespaceParseSpec.skipHeaderRows;
            break;
          case 'tsv':
            disableSubmit = !namespaceParseSpec.columns;
            break;
          case 'customJson':
            disableSubmit = !namespaceParseSpec.keyFieldName && !namespaceParseSpec.valueFieldName;
            break;
        }
        break;
      case 'jdbc':
        const extractionNamespace = lookupSpec.extractionNamespace;
        disableSubmit =
          !extractionNamespace.namespace ||
          !extractionNamespace.connectorConfig ||
          !extractionNamespace.table ||
          !extractionNamespace.keyColumn ||
          !extractionNamespace.valueColumn;
        break;
    }
  }

  return (
    <Dialog
      className="lookup-edit-dialog"
      isOpen
      onClose={onClose}
      title={isEdit ? 'Edit lookup' : 'Add lookup'}
    >
      <FormGroup className="lookup-label" label="Name">
        <InputGroup
          value={lookupName}
          onChange={(e: any) => onChange('lookupEditName', e.target.value)}
          disabled={isEdit}
          placeholder="Enter the lookup name"
        />
      </FormGroup>
      {renderTierInput()}
      <FormGroup className="lookup-label" label="Version">
        <InputGroup
          value={lookupVersion}
          onChange={(e: any) => onChange('lookupEditVersion', e.target.value)}
          placeholder="Enter the lookup version"
          rightElement={
            <Button minimal text="Use ISO as version" onClick={() => addISOVersion()} />
          }
        />
      </FormGroup>
      <AutoForm
        fields={[
          {
            name: 'type',
            type: 'string',
            suggestions: ['map', 'cachedNamespace'],
            adjustment: model => {
              if (
                model.type === 'map' &&
                model.extractionNamespace &&
                model.extractionNamespace.type
              ) {
                return model;
              }
              model.extractionNamespace = { type: 'uri', namespaceParseSpec: { format: 'csv' } };
              return model;
            },
          },
          {
            name: 'map',
            type: 'json',
            defined: model => {
              return model.type === 'map';
            },
          },
          {
            name: 'extractionNamespace.type',
            type: 'string',
            label: 'Globally cached lookup type',
            placeholder: 'uri',
            suggestions: ['uri', 'jdbc'],
            defined: model => model.type === 'cachedNamespace',
          },
          {
            name: 'extractionNamespace.uriPrefix',
            type: 'string',
            label: 'URI prefix',
            info:
              'A URI which specifies a directory (or other searchable resource) in which to search for files',
            placeholder: 's3://bucket/some/key/prefix/',
            defined: model =>
              model.type === 'cachedNamespace' &&
              !!model.extractionNamespace &&
              model.extractionNamespace.type === 'uri',
          },
          {
            name: 'extractionNamespace.fileRegex',
            type: 'string',
            label: 'File regex',
            placeholder: 'renames-[0-9]*\\.gz',
            info:
              'Optional regex for matching the file name under uriPrefix. Only used if uriPrefix is used',
            defined: model =>
              model.type === 'cachedNamespace' &&
              !!model.extractionNamespace &&
              model.extractionNamespace.type === 'uri',
          },
          {
            name: 'extractionNamespace.namespaceParseSpec.format',
            type: 'string',
            label: 'Format',
            defaultValue: 'csv',
            suggestions: ['csv', 'tsv', 'customJson', 'simpleJson'],
            // todo needs info
            defined: model =>
              model.type === 'cachedNamespace' &&
              !!model.extractionNamespace &&
              model.extractionNamespace.type === 'uri',
          },
          {
            name: 'extractionNamespace.namespaceParseSpec.columns',
            type: 'string-array',
            label: 'Columns',
            placeholder: `["key", "value"]`,
            info: 'The list of columns in the csv file',
            defined: model =>
              model.type === 'cachedNamespace' &&
              !!model.extractionNamespace &&
              model.extractionNamespace.type === 'uri' &&
              model.extractionNamespace.namespaceParseSpec &&
              (model.extractionNamespace.namespaceParseSpec.format === 'csv' ||
                model.extractionNamespace.namespaceParseSpec.format === 'tsv'),
          },
          {
            name: 'extractionNamespace.namespaceParseSpec.keyColumn',
            type: 'string',
            label: 'Key column',
            placeholder: 'Key',
            info: 'The name of the column containing the key',
            defined: model =>
              model.type === 'cachedNamespace' &&
              !!model.extractionNamespace &&
              model.extractionNamespace.type === 'uri' &&
              model.extractionNamespace.namespaceParseSpec &&
              (model.extractionNamespace.namespaceParseSpec.format === 'csv' ||
                model.extractionNamespace.namespaceParseSpec.format === 'tsv'),
          },
          {
            name: 'extractionNamespace.namespaceParseSpec.valueColumn',
            type: 'string',
            label: 'Value column',
            placeholder: 'Value',
            info: 'The name of the column containing the value',
            defined: model =>
              model.type === 'cachedNamespace' &&
              !!model.extractionNamespace &&
              model.extractionNamespace.type === 'uri' &&
              model.extractionNamespace.namespaceParseSpec &&
              (model.extractionNamespace.namespaceParseSpec.format === 'csv' ||
                model.extractionNamespace.namespaceParseSpec.format === 'tsv'),
          },
          {
            name: 'extractionNamespace.namespaceParseSpec.hasHeaderRow',
            type: 'boolean',
            label: 'Has header row',
            defaultValue: false,
            info: `A flag to indicate that column information can be extracted from the input files' header row`,
            defined: model =>
              model.type === 'cachedNamespace' &&
              !!model.extractionNamespace &&
              model.extractionNamespace.type === 'uri' &&
              model.extractionNamespace.namespaceParseSpec &&
              (model.extractionNamespace.namespaceParseSpec.format === 'csv' ||
                model.extractionNamespace.namespaceParseSpec.format === 'tsv'),
          },
          {
            name: 'extractionNamespace.namespaceParseSpec.skipHeaderRows',
            type: 'number',
            label: 'Skip header rows',
            placeholder: '0',
            info: `Number of header rows to be skipped`,
            defined: model =>
              model.type === 'cachedNamespace' &&
              !!model.extractionNamespace &&
              model.extractionNamespace.type === 'uri' &&
              model.extractionNamespace.namespaceParseSpec &&
              (model.extractionNamespace.namespaceParseSpec.format === 'csv' ||
                model.extractionNamespace.namespaceParseSpec.format === 'tsv'),
          },
          {
            name: 'extractionNamespace.namespaceParseSpec.delimiter',
            type: 'string',
            label: 'Delimiter',
            placeholder: `\t`,
            info: `The delimiter in the file`,
            defined: model =>
              model.type === 'cachedNamespace' &&
              !!model.extractionNamespace &&
              model.extractionNamespace.type === 'uri' &&
              model.extractionNamespace.namespaceParseSpec &&
              model.extractionNamespace.namespaceParseSpec.format === 'tsv',
          },
          {
            name: 'extractionNamespace.namespaceParseSpec.listDelimiter',
            type: 'string',
            label: 'List delimiter',
            placeholder: `\u0001`,
            info: `The list delimiter in the file\t`,
            defined: model =>
              model.type === 'cachedNamespace' &&
              !!model.extractionNamespace &&
              model.extractionNamespace.type === 'uri' &&
              model.extractionNamespace.namespaceParseSpec &&
              model.extractionNamespace.namespaceParseSpec.format === 'tsv',
          },
          {
            name: 'extractionNamespace.namespaceParseSpec.keyFieldName',
            type: 'string',
            label: 'Key field name',
            placeholder: `key`,
            info: `The field name of the key`,
            defined: model =>
              model.type === 'cachedNamespace' &&
              !!model.extractionNamespace &&
              model.extractionNamespace.type === 'uri' &&
              model.extractionNamespace.namespaceParseSpec &&
              model.extractionNamespace.namespaceParseSpec.format === 'customJson',
          },
          {
            name: 'extractionNamespace.namespaceParseSpec.valueFieldName',
            type: 'string',
            label: 'Value field name',
            placeholder: `value`,
            info: `The field name of the value`,
            defined: model =>
              model.type === 'cachedNamespace' &&
              !!model.extractionNamespace &&
              model.extractionNamespace.type === 'uri' &&
              model.extractionNamespace.namespaceParseSpec &&
              model.extractionNamespace.namespaceParseSpec.format === 'customJson',
          },
          {
            name: 'extractionNamespace.namespace',
            type: 'string',
            label: 'Namespace',
            placeholder: 'some_lookup',
            info: `The namespace to define`,
            defined: model =>
              model.type === 'cachedNamespace' &&
              !!model.extractionNamespace &&
              model.extractionNamespace.type === 'jdbc',
          },
          {
            name: 'extractionNamespace.table',
            type: 'string',
            label: 'Table',
            placeholder: 'some_lookup_table',
            info: `The table which contains the key value pairs`,
            defined: model =>
              model.type === 'cachedNamespace' &&
              !!model.extractionNamespace &&
              model.extractionNamespace.type === 'jdbc',
          },
          {
            name: 'extractionNamespace.keyColumn',
            type: 'string',
            label: 'Key column',
            placeholder: 'the_old_dim_value',
            info: `The column in table which contains the keys`,
            defined: model =>
              model.type === 'cachedNamespace' &&
              !!model.extractionNamespace &&
              model.extractionNamespace.type === 'jdbc',
          },
          {
            name: 'extractionNamespace.valueColumn',
            type: 'string',
            label: 'Value column',
            placeholder: 'the_new_dim_value',
            info: `The column in table which contains the values`,
            defined: model =>
              model.type === 'cachedNamespace' &&
              !!model.extractionNamespace &&
              model.extractionNamespace.type === 'jdbc',
          },
          {
            name: 'extractionNamespace.filter',
            type: 'json',
            label: 'Filter',
            info: `The filter to use when selecting lookups, this is used to create a where clause on lookup population`,
            defined: model =>
              model.type === 'cachedNamespace' &&
              !!model.extractionNamespace &&
              model.extractionNamespace.type === 'jdbc',
          },
          {
            name: 'extractionNamespace.tsColumn',
            type: 'string',
            label: 'TsColumn',
            info: `The column in table which contains when the key was updated`,
            defined: model =>
              model.type === 'cachedNamespace' &&
              !!model.extractionNamespace &&
              model.extractionNamespace.type === 'jdbc',
          },
          {
            name: 'extractionNamespace.pollPeriod',
            type: 'string',
            label: 'Poll Period',
            placeholder: 'PT5M',
            info: `Period between polling for updates`,
            defined: model =>
              model.type === 'cachedNamespace' &&
              !!model.extractionNamespace &&
              model.extractionNamespace.type === 'uri',
          },
          {
            name: 'firstCacheTimeout',
            type: 'number',
            label: 'First cache timeout',
            placeholder: '0',
            info: `How long to wait (in ms) for the first run of the cache to populate. 0 indicates to not wait`,
            defined: model => model.type === 'cachedNamespace',
          },
          {
            name: 'injective',
            type: 'boolean',
            defaultValue: false,
            info: `If the underlying map is injective (keys and values are unique) then optimizations can occur internally by setting this to true`,
            defined: model => model.type === 'cachedNamespace',
          },
        ]}
        model={lookupSpec}
        onChange={m => {
          onChange('lookupEditSpec', m);
        }}
      />
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" onClick={onClose} />
          <Button
            text="Submit"
            intent={Intent.PRIMARY}
            onClick={() => {
              onSubmit(updateVersionOnSubmit && isEdit);
            }}
            disabled={disableSubmit}
          />
        </div>
      </div>
    </Dialog>
  );
});
