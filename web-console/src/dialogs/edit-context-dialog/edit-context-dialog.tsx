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

import { JsonInput } from '../../components';
import type { QueryContext } from '../../druid-models';

import './edit-context-dialog.scss';

export interface EditContextDialogProps {
  initQueryContext: QueryContext | undefined;
  onQueryContextChange(queryContext: QueryContext): void;
  onClose(): void;
}

export const EditContextDialog = React.memo(function EditContextDialog(
  props: EditContextDialogProps,
) {
  const { initQueryContext, onQueryContextChange, onClose } = props;
  const [queryContext, setQueryContext] = useState<QueryContext | undefined>(initQueryContext);
  const [jsonError, setJsonError] = useState<Error | undefined>();

  return (
    <Dialog className="edit-context-dialog" isOpen onClose={onClose} title="Edit query context">
      <JsonInput
        value={queryContext}
        onChange={setQueryContext}
        setError={setJsonError}
        height="100%"
        showLineNumbers
      />
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" onClick={onClose} />
          <Button
            text="Save"
            intent={Intent.PRIMARY}
            disabled={Boolean(jsonError)}
            onClick={() => {
              if (queryContext) {
                onQueryContextChange(queryContext);
              }
              onClose();
            }}
          />
        </div>
      </div>
    </Dialog>
  );
});
