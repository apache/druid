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
import Hjson from 'hjson';
import * as JSONBig from 'json-bigint-native';
import React, { useState } from 'react';
import AceEditor from 'react-ace';

import type { QueryContext } from '../../druid-models';
import { AppToaster } from '../../singletons';

import './edit-context-dialog.scss';

function formatContext(context: QueryContext | undefined): string {
  const str = JSONBig.stringify(context || {}, undefined, 2);
  return str === '{}' ? '{\n\n}' : str;
}

export interface EditContextDialogProps {
  initQueryContext: QueryContext | undefined;
  onQueryContextChange(queryContext: QueryContext): void;
  onClose(): void;
}

export const EditContextDialog = React.memo(function EditContextDialog(
  props: EditContextDialogProps,
) {
  const { initQueryContext, onQueryContextChange, onClose } = props;
  const [queryContextString, setQueryContextString] = useState<string>(
    formatContext(initQueryContext),
  );

  return (
    <Dialog className="edit-context-dialog" isOpen onClose={onClose} title="Edit query context">
      <AceEditor
        mode="hjson"
        theme="solarized_dark"
        className="query-string"
        name="ace-editor"
        fontSize={12}
        width="100%"
        height="100%"
        showGutter
        showPrintMargin={false}
        value={queryContextString}
        onChange={v => setQueryContextString(v)}
      />
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" onClick={onClose} />
          <Button
            text="Save"
            intent={Intent.PRIMARY}
            onClick={() => {
              let queryContext: QueryContext;
              try {
                queryContext = Hjson.parse(queryContextString);
              } catch (e) {
                AppToaster.show({
                  message: e.message,
                  intent: Intent.DANGER,
                });
                return;
              }

              onQueryContextChange(queryContext);
              onClose();
            }}
          />
        </div>
      </div>
    </Dialog>
  );
});
