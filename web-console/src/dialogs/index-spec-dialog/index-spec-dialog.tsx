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

import { Button, Classes, Dialog, Intent } from '@blueprintjs/core';
import React, { useState } from 'react';

import { AutoForm, FormJsonSelector, FormJsonTabs, JsonInput } from '../../components';
import { INDEX_SPEC_FIELDS, IndexSpec } from '../../druid-models';

import './index-spec-dialog.scss';

export interface IndexSpecDialogProps {
  title?: string;
  onClose: () => void;
  onSave: (indexSpec: IndexSpec) => void;
  indexSpec: IndexSpec | undefined;
}

export const IndexSpecDialog = React.memo(function IndexSpecDialog(props: IndexSpecDialogProps) {
  const { title, indexSpec, onSave, onClose } = props;

  const [currentTab, setCurrentTab] = useState<FormJsonTabs>('form');
  const [currentIndexSpec, setCurrentIndexSpec] = useState<IndexSpec>(indexSpec || {});
  const [jsonError, setJsonError] = useState<Error | undefined>();

  const issueWithCurrentIndexSpec = AutoForm.issueWithModel(currentIndexSpec, INDEX_SPEC_FIELDS);

  return (
    <Dialog
      className="index-spec-dialog"
      isOpen
      onClose={onClose}
      canOutsideClickClose={false}
      title={title ?? 'Index spec'}
    >
      <FormJsonSelector tab={currentTab} onChange={setCurrentTab} />
      <div className="content">
        {currentTab === 'form' ? (
          <AutoForm
            fields={INDEX_SPEC_FIELDS}
            model={currentIndexSpec}
            onChange={m => setCurrentIndexSpec(m)}
          />
        ) : (
          <JsonInput
            value={currentIndexSpec}
            onChange={v => {
              setCurrentIndexSpec(v);
              setJsonError(undefined);
            }}
            onError={setJsonError}
            issueWithValue={value => AutoForm.issueWithModel(value, INDEX_SPEC_FIELDS)}
            height="100%"
          />
        )}
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" onClick={onClose} />
          <Button
            text="Save"
            intent={Intent.PRIMARY}
            disabled={Boolean(jsonError || issueWithCurrentIndexSpec)}
            onClick={() => {
              onSave(currentIndexSpec);
              onClose();
            }}
          />
        </div>
      </div>
    </Dialog>
  );
});
