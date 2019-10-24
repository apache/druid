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
import AceEditor from 'react-ace';

import { validJson } from '../../utils';

import './lookup-edit-dialog.scss';

export interface LookupEditDialogProps {
  onClose: () => void;
  onSubmit: () => void;
  onChange: (field: string, value: string) => void;
  lookupName: string;
  lookupTier: string;
  lookupVersion: string;
  lookupSpec: string;
  isEdit: boolean;
  allLookupTiers: string[];
}

export function LookupEditDialog(props: LookupEditDialogProps) {
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
    lookupName === '' || lookupVersion === '' || lookupTier === '' || !validJson(lookupSpec);

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

      <FormGroup className="lookup-label" label="Spec:" />

      <AceEditor
        className="lookup-edit-dialog-textarea"
        mode="hjson"
        theme="solarized_dark"
        onChange={(e: any) => onChange('lookupEditSpec', e)}
        fontSize={12}
        height="40vh"
        width="auto"
        showPrintMargin={false}
        showGutter={false}
        value={lookupSpec}
        editorProps={{ $blockScrolling: Infinity }}
        setOptions={{
          tabSize: 2,
        }}
        style={{}}
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
}
