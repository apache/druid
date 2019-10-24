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

import { Button, Callout, Classes, Dialog, Intent, TextArea } from '@blueprintjs/core';
import Hjson from 'hjson';
import React, { useState } from 'react';

import { QueryContext } from '../../utils/query-context';

import './edit-context-dialog.scss';

export interface EditContextDialogProps {
  queryContext: QueryContext;
  onQueryContextChange: (queryContext: QueryContext) => void;
  onClose: () => void;
}

export interface EditContextDialogState {
  queryContextString: string;
  queryContext?: QueryContext;
  error?: string;
}

export function EditContextDialog(props: EditContextDialogProps) {
  const { onQueryContextChange, onClose } = props;
  const [state, setState] = useState<EditContextDialogState>(() => ({
    queryContext: props.queryContext,
    queryContextString: Object.keys(props.queryContext).length
      ? JSON.stringify(props.queryContext, undefined, 2)
      : '{\n\n}',
  }));

  const { queryContext, queryContextString, error } = state;

  function handleTextChange(e: any) {
    const queryContextString = (e.target as HTMLInputElement).value;

    let error: string | undefined;
    let queryContext: QueryContext | undefined;
    try {
      queryContext = Hjson.parse(queryContextString);
    } catch (e) {
      error = e.message;
    }

    if (!error && (!queryContext || typeof queryContext !== 'object')) {
      error = 'Input is not a valid object';
      queryContext = undefined;
    }

    setState({
      queryContextString,
      queryContext,
      error,
    });
  }

  return (
    <Dialog className="edit-context-dialog" isOpen onClose={onClose} title={'Edit query context'}>
      <TextArea value={queryContextString} onChange={handleTextChange} autoFocus />
      <div className={Classes.DIALOG_FOOTER_ACTIONS}>
        {error && (
          <Callout intent={Intent.DANGER} className="edit-context-dialog-error">
            {error}
          </Callout>
        )}
        <div className={'edit-context-dialog-buttons'}>
          <Button text={'Close'} onClick={onClose} />
          <Button
            text={'Save'}
            intent={Intent.PRIMARY}
            disabled={Boolean(error)}
            onClick={
              queryContext
                ? () => {
                    onQueryContextChange(queryContext);
                    onClose();
                  }
                : undefined
            }
          />
        </div>
      </div>
    </Dialog>
  );
}
