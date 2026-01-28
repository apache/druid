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

import { Button, Classes, Dialog, HTMLSelect } from '@blueprintjs/core';
import * as JSONBig from 'json-bigint-native';
import React, { useState } from 'react';
import type { ReactDiffViewerStylesOverride } from 'react-diff-viewer';
import ReactDiffViewer from 'react-diff-viewer';

import './diff-dialog.scss';

const REACT_DIFF_STYLES: ReactDiffViewerStylesOverride = {
  variables: {
    dark: {
      diffViewerBackground: '#24283b', // $dark-gray2
      codeFoldBackground: '#181c2d', // $dark-gray1
      codeFoldGutterBackground: '#1f2134',
      gutterBackground: '#292d42',
      gutterBackgroundDark: '#8489a1', // $gray3
      gutterColor: '#ffffff',
    },
  },
};

export interface DiffVersion {
  label: string;
  value: unknown;
}

export interface DiffDialogProps {
  title?: string;
  onClose(): void;

  // Single value
  oldValue?: unknown;
  newValue?: unknown;

  // Versions
  versions?: DiffVersion[];
  initOldIndex?: number;
  initNewIndex?: number;
}

export const DiffDialog = React.memo(function DiffDialog(props: DiffDialogProps) {
  const { title, oldValue, newValue, versions, initOldIndex, initNewIndex, onClose } = props;

  const [leftIndex, setLeftIndex] = useState(initOldIndex || 0);
  const [rightIndex, setRightIndex] = useState(initNewIndex || 0);

  let oldValueString: string;
  let newValueString: string;
  if (Array.isArray(versions)) {
    if (versions.length) {
      const oldVersionValue = versions[leftIndex].value;
      const newVersionValue = versions[rightIndex].value;
      if (typeof oldVersionValue === 'string' && typeof newVersionValue === 'string') {
        oldValueString = oldVersionValue;
        newValueString = newVersionValue;
      } else {
        oldValueString = JSONBig.stringify(oldVersionValue, undefined, 2);
        newValueString = JSONBig.stringify(newVersionValue, undefined, 2);
      }
    } else {
      oldValueString = newValueString = 'Nothing to diff';
    }
  } else {
    if (typeof oldValue === 'string' && typeof newValue === 'string') {
      oldValueString = oldValue;
      newValueString = newValue;
    } else {
      oldValueString = JSONBig.stringify(oldValue, undefined, 2);
      newValueString = JSONBig.stringify(newValue, undefined, 2);
    }
  }

  return (
    <Dialog
      className="diff-dialog"
      isOpen
      onClose={onClose}
      canOutsideClickClose={false}
      title={title || 'Diff'}
    >
      <div className={Classes.DIALOG_BODY}>
        {versions && (
          <div className="version-selector-bar">
            <HTMLSelect
              className="left-selector"
              value={leftIndex}
              onChange={e => setLeftIndex(Number(e.target.value))}
            >
              {versions.map((version, i) => (
                <option key={i} value={i}>
                  {version.label}
                </option>
              ))}
            </HTMLSelect>
            <HTMLSelect
              className="right-selector"
              value={rightIndex}
              onChange={e => setRightIndex(Number(e.target.value))}
            >
              {versions.map((version, i) => (
                <option key={i} value={i}>
                  {version.label}
                </option>
              ))}
            </HTMLSelect>
          </div>
        )}
        <div className="diff-container">
          <ReactDiffViewer
            oldValue={oldValueString}
            newValue={newValueString}
            splitView
            useDarkTheme
            styles={REACT_DIFF_STYLES}
          />
        </div>
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" onClick={onClose} />
        </div>
      </div>
    </Dialog>
  );
});
