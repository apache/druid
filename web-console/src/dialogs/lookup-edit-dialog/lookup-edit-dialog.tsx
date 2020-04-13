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

import { AutoForm, Field } from '../../components';

import './lookup-edit-dialog.scss';

export interface ExtractionNamespaceSpec {
  type?: string;
  uri?: string;
  uriPrefix?: string;
  fileRegex?: string;
  namespaceParseSpec?: NamespaceParseSpec;
  namespace?: string;
  connectorConfig?: {
    createTables: boolean;
    connectURI: string;
    user: string;
    password: string;
  };
  table?: string;
  keyColumn?: string;
  valueColumn?: string;
  filter?: any;
  tsColumn?: string;
  pollPeriod?: number | string;
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
  type?: string;
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

export function isDisabled(
  lookupName?: string,
  lookupVersion?: string,
  lookupTier?: string,
  lookupSpec?: LookupSpec,
) {
  let disableSubmit =
    !lookupName ||
    !lookupVersion ||
    !lookupTier ||
    !lookupSpec ||
    !lookupName ||
    lookupName === '' ||
    lookupVersion === '' ||
    lookupTier === '' ||
    lookupSpec.type === '' ||
    lookupSpec.type === undefined ||
    (lookupSpec.type === 'map' && lookupSpec.map === undefined) ||
    (lookupSpec.type === 'cachedNamespace' && lookupSpec.extractionNamespace === undefined);

  if (
    !disableSubmit &&
    lookupSpec &&
    lookupSpec.type === 'cachedNamespace' &&
    lookupSpec.extractionNamespace
  ) {
    switch (lookupSpec.extractionNamespace.type) {
      case 'uri':
        const namespaceParseSpec = lookupSpec.extractionNamespace.namespaceParseSpec;
        disableSubmit = !namespaceParseSpec;
        if (!namespaceParseSpec) break;
        switch (namespaceParseSpec.format) {
          case 'csv':
            disableSubmit = !namespaceParseSpec.columns && !namespaceParseSpec.skipHeaderRows;
            break;
          case 'tsv':
            disableSubmit = !namespaceParseSpec.columns;
            break;
          case 'customJson':
            disableSubmit = !namespaceParseSpec.keyFieldName || !namespaceParseSpec.valueFieldName;
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
  return disableSubmit;
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

  const fields = [
    {
      name: 'type',
      type: 'string',
      suggestions: ['map', 'cachedNamespace'],
      adjustment: (model: LookupSpec) => {
        if (model.type === 'map' && model.extractionNamespace && model.extractionNamespace.type) {
          return model;
        }
        model.extractionNamespace = { type: 'uri', namespaceParseSpec: { format: 'csv' } };
        return model;
      },
    },
    {
      name: 'map',
      type: 'json',
      defined: (model: LookupSpec) => {
        return model.type === 'map';
      },
    },
    {
      name: 'extractionNamespace.type',
      type: 'string',
      label: 'Globally cached lookup type',
      placeholder: 'uri',
      suggestions: ['uri', 'jdbc'],
      defined: (model: LookupSpec) => model.type === 'cachedNamespace',
    },
    {
      name: 'extractionNamespace.uriPrefix',
      type: 'string',
      label: 'URI prefix',
      info:
        'A URI which specifies a directory (or other searchable resource) in which to search for files',
      placeholder: 's3://bucket/some/key/prefix/',
      defined: (model: LookupSpec) =>
        model.type === 'cachedNamespace' &&
        !!model.extractionNamespace &&
        model.extractionNamespace.type === 'uri',
    },
    {
      name: 'extractionNamespace.fileRegex',
      type: 'string',
      label: 'File regex',
      placeholder: '(optional)',
      info:
        'Optional regex for matching the file name under uriPrefix. Only used if uriPrefix is used',
      defined: (model: LookupSpec) =>
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
      defined: (model: LookupSpec) =>
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
      defined: (model: LookupSpec) =>
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
      defined: (model: LookupSpec) =>
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
      defined: (model: LookupSpec) =>
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
      defined: (model: LookupSpec) =>
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
      placeholder: '(optional)',
      info: `Number of header rows to be skipped. The default number of header rows to be skipped is 0.`,
      defined: (model: LookupSpec) =>
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
      placeholder: `(optional)`,
      defined: (model: LookupSpec) =>
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
      placeholder: `(optional)`,
      defined: (model: LookupSpec) =>
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
      defined: (model: LookupSpec) =>
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
      defined: (model: LookupSpec) =>
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
      info: (
        <>
          <p>The namespace value in the SQL query:</p>
          <p>
            SELECT keyColumn, valueColumn, tsColumn? FROM <strong>namespace</strong>.table WHERE
            filter
          </p>
        </>
      ),
      defined: (model: LookupSpec) =>
        model.type === 'cachedNamespace' &&
        !!model.extractionNamespace &&
        model.extractionNamespace.type === 'jdbc',
    },
    {
      name: 'extractionNamespace.connectorConfig.createTables',
      type: 'boolean',
      label: 'CreateTables',
      info: 'Defines the connectURI value on the The connector config to used',
      defined: (model: LookupSpec) =>
        model.type === 'cachedNamespace' &&
        !!model.extractionNamespace &&
        model.extractionNamespace.type === 'jdbc',
    },
    {
      name: 'extractionNamespace.connectorConfig.connectURI',
      type: 'string',
      label: 'Connect URI',
      info: 'Defines the connectURI value on the The connector config to used',
      defined: (model: LookupSpec) =>
        model.type === 'cachedNamespace' &&
        !!model.extractionNamespace &&
        model.extractionNamespace.type === 'jdbc',
    },
    {
      name: 'extractionNamespace.connectorConfig.user',
      type: 'string',
      label: 'User',
      info: 'Defines the user to be used by the connector config',
      defined: (model: LookupSpec) =>
        model.type === 'cachedNamespace' &&
        !!model.extractionNamespace &&
        model.extractionNamespace.type === 'jdbc',
    },
    {
      name: 'extractionNamespace.connectorConfig.password',
      type: 'string',
      label: 'Password',
      info: 'Defines the password to be used by the connector config',
      defined: (model: LookupSpec) =>
        model.type === 'cachedNamespace' &&
        !!model.extractionNamespace &&
        model.extractionNamespace.type === 'jdbc',
    },
    {
      name: 'extractionNamespace.table',
      type: 'string',
      label: 'Table',
      placeholder: 'some_lookup_table',
      info: (
        <>
          <p>
            The table which contains the key value pairs. This will become the table value in the
            SQL query:
          </p>
          <p>
            SELECT keyColumn, valueColumn, tsColumn? FROM namespace.<strong>table</strong> WHERE
            filter
          </p>
        </>
      ),
      defined: (model: LookupSpec) =>
        model.type === 'cachedNamespace' &&
        !!model.extractionNamespace &&
        model.extractionNamespace.type === 'jdbc',
    },
    {
      name: 'extractionNamespace.keyColumn',
      type: 'string',
      label: 'Key column',
      placeholder: 'my_key_value',
      info: (
        <>
          <p>
            The column in the table which contains the keys. This will become the keyColumn value in
            the SQL query:
          </p>
          <p>
            SELECT <strong>keyColumn</strong>, valueColumn, tsColumn? FROM namespace.table WHERE
            filter
          </p>
        </>
      ),
      defined: (model: LookupSpec) =>
        model.type === 'cachedNamespace' &&
        !!model.extractionNamespace &&
        model.extractionNamespace.type === 'jdbc',
    },
    {
      name: 'extractionNamespace.valueColumn',
      type: 'string',
      label: 'Value column',
      placeholder: 'my_column_value',
      info: (
        <>
          <p>
            The column in table which contains the values. This will become the valueColumn value in
            the SQL query:
          </p>
          <p>
            SELECT keyColumn, <strong>valueColumn</strong>, tsColumn? FROM namespace.table WHERE
            filter
          </p>
        </>
      ),
      defined: (model: LookupSpec) =>
        model.type === 'cachedNamespace' &&
        !!model.extractionNamespace &&
        model.extractionNamespace.type === 'jdbc',
    },
    {
      name: 'extractionNamespace.filter',
      type: 'string',
      label: 'Filter',
      placeholder: '(optional)',
      info: (
        <>
          <p>
            The filter to be used when selecting lookups, this is used to create a where clause on
            lookup population. This will become the expression filter in the SQL query:
          </p>
          <p>
            SELECT keyColumn, valueColumn, tsColumn? FROM namespace.table WHERE{' '}
            <strong>filter</strong>
          </p>
        </>
      ),
      defined: (model: LookupSpec) =>
        model.type === 'cachedNamespace' &&
        !!model.extractionNamespace &&
        model.extractionNamespace.type === 'jdbc',
    },
    {
      name: 'extractionNamespace.tsColumn',
      type: 'string',
      label: 'TsColumn',
      placeholder: '(optional)',
      info: (
        <>
          <p>
            The column in table which contains when the key was updated. This will become the Value
            in the SQL query:
          </p>
          <p>
            SELECT keyColumn, valueColumn, <strong>tsColumn</strong>? FROM namespace.table WHERE
            filter
          </p>
        </>
      ),
      defined: (model: LookupSpec) =>
        model.type === 'cachedNamespace' &&
        !!model.extractionNamespace &&
        model.extractionNamespace.type === 'jdbc',
    },
    {
      name: 'extractionNamespace.pollPeriod',
      type: 'string',
      label: 'Poll period',
      placeholder: '(optional)',
      info: `Period between polling for updates`,
      defined: (model: LookupSpec) =>
        model.type === 'cachedNamespace' &&
        !!model.extractionNamespace &&
        model.extractionNamespace.type === 'uri',
    },
    {
      name: 'firstCacheTimeout',
      type: 'number',
      label: 'First cache timeout',
      placeholder: '(optional)',
      info: `How long to wait (in ms) for the first run of the cache to populate. 0 indicates to not wait`,
      defined: (model: LookupSpec) => model.type === 'cachedNamespace',
    },
    {
      name: 'injective',
      type: 'boolean',
      defaultValue: false,
      info: `If the underlying map is injective (keys and values are unique) then optimizations can occur internally by setting this to true`,
      defined: (model: LookupSpec) => model.type === 'cachedNamespace',
    },
  ];

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
        fields={fields as Field<LookupSpec>[]}
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
            disabled={isDisabled(lookupName, lookupVersion, lookupTier, lookupSpec)}
          />
        </div>
      </div>
    </Dialog>
  );
});
