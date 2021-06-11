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
import React, { useState } from 'react';

import { AutoForm, FormJsonSelector, FormJsonTabs, JsonInput } from '../../components';
import { isLookupInvalid, LOOKUP_FIELDS, LookupSpec } from '../../druid-models';

import './lookup-edit-dialog.scss';

export interface LookupEditDialogProps {
  onClose: () => void;
  onSubmit: (updateLookupVersion: boolean) => void;
  onChange: (field: 'name' | 'tier' | 'version' | 'spec', value: string | LookupSpec) => void;
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
  const [currentTab, setCurrentTab] = useState<FormJsonTabs>('form');
  const [updateVersionOnSubmit, setUpdateVersionOnSubmit] = useState(true);
  const [jsonError, setJsonError] = useState<Error | undefined>();

  const disableSubmit = Boolean(
    jsonError || isLookupInvalid(lookupName, lookupVersion, lookupTier, lookupSpec),
  );

  return (
    <Dialog
      className="lookup-edit-dialog"
      isOpen
      onClose={onClose}
      title={isEdit ? 'Edit lookup' : 'Add lookup'}
    >
      <div className="content">
        <FormGroup label="Name">
          <InputGroup
            value={lookupName}
            onChange={(e: any) => onChange('name', e.target.value)}
            intent={lookupName ? Intent.NONE : Intent.PRIMARY}
            disabled={isEdit}
            placeholder="Enter the lookup name"
          />
        </FormGroup>
        <FormGroup label="Tier">
          {isEdit ? (
            <InputGroup
              value={lookupTier}
              onChange={(e: any) => onChange('tier', e.target.value)}
              disabled
            />
          ) : (
            <HTMLSelect value={lookupTier} onChange={(e: any) => onChange('tier', e.target.value)}>
              {allLookupTiers.map(tier => (
                <option key={tier} value={tier}>
                  {tier}
                </option>
              ))}
            </HTMLSelect>
          )}
        </FormGroup>
        <FormGroup label="Version">
          <InputGroup
            value={lookupVersion}
            onChange={(e: any) => {
              setUpdateVersionOnSubmit(false);
              onChange('version', e.target.value);
            }}
            placeholder="Enter the lookup version"
            rightElement={
              <Button
                minimal
                text="Use ISO as version"
                onClick={() => onChange('version', new Date().toISOString())}
              />
            }
          />
        </FormGroup>
        <FormJsonSelector tab={currentTab} onChange={setCurrentTab} />
        {currentTab === 'form' ? (
          <AutoForm
            fields={LOOKUP_FIELDS}
            model={lookupSpec}
            onChange={m => {
              onChange('spec', m);
            }}
          />
        ) : (
          <JsonInput
            value={lookupSpec}
            height="80vh"
            onChange={m => {
              onChange('spec', m);
              setJsonError(undefined);
            }}
            onError={setJsonError}
          />
        )}
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" onClick={onClose} />
          <Button
            text="Submit"
            intent={Intent.PRIMARY}
            disabled={disableSubmit}
            onClick={() => {
              onSubmit(updateVersionOnSubmit && isEdit);
            }}
          />
        </div>
      </div>
    </Dialog>
  );
});
