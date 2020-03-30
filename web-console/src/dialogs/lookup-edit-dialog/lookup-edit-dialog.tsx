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

export interface LookupSpec {
  type: string;
  map?: {};
}
export interface LookupEditDialogProps {
  onClose: () => void;
  onSubmit: () => void;
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

  function isValidMap(map: object) {
    try {
      if (typeof map !== 'object') return false;
      const entries = Object.entries(map);
      for (let i = 0; i < entries.length; i++) {
        if (typeof entries[i][1] !== 'string') return false;
      }
      return true;
    } catch {
      return false;
    }
    return true;
  }

  function addISOVersion() {
    const currentDate = new Date();
    const ISOString = currentDate.toISOString();
    onChange('lookupEditVersion', ISOString);
  }

  function renderTierInput() {
    if (isEdit) {
      return (
        <FormGroup className="lookup-label" label="Tier: ">
          <InputGroup
            value={lookupTier}
            onChange={(e: any) => onChange('lookupEditTier', e.target.value)}
            disabled
          />
        </FormGroup>
      );
    } else {
      return (
        <FormGroup className="lookup-label" label="Tier:">
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

  const disableSubmit =
    lookupName === '' ||
    lookupVersion === '' ||
    lookupTier === '' ||
    lookupSpec.type === '' ||
    lookupSpec.type === undefined ||
    lookupSpec.map === undefined ||
    !isValidMap(lookupSpec.map);

  return (
    <Dialog
      className="lookup-edit-dialog"
      isOpen
      onClose={onClose}
      title={isEdit ? 'Edit lookup' : 'Add lookup'}
    >
      <FormGroup className="lookup-label" label="Name: ">
        <InputGroup
          value={lookupName}
          onChange={(e: any) => onChange('lookupEditName', e.target.value)}
          disabled={isEdit}
          placeholder="Enter the lookup name"
        />
      </FormGroup>
      {renderTierInput()}
      <FormGroup className="lookup-label" label="Version:">
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
            label: 'Type :',
          },
          {
            name: 'map',
            type: 'json',
            label: 'Map :',
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
            onClick={() => onSubmit()}
            disabled={disableSubmit}
          />
        </div>
      </div>
    </Dialog>
  );
});
