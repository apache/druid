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

import type { EditorView } from '@codemirror/view';
import classNames from 'classnames';
import Hjson from 'hjson';
import * as JSONBig from 'json-bigint-native';
import React, { useCallback, useEffect, useEffectEvent, useRef, useState } from 'react';

import { getHjsonCompletions } from '../../editor-completions/hjson-completions';
import type { JsonCompletionRule } from '../../utils';
import type { CompletionRequest } from '../code-editor/code-editor';
import { CodeEditor, focusEditorAt } from '../code-editor/code-editor';

import './json-input.scss';

function parseHjson(str: string): any {
  if (str.trim() === '') return;
  return Hjson.parse(str);
}

export function extractRowColumnFromHjsonError(
  error: Error,
): { row: number; column: number } | undefined {
  // Message would be something like:
  // `Found '}' where a key name was expected at line 26,7`
  // Use this to extract the row and column (subtract 1) and jump the cursor to the right place on click
  const m = /line (\d+),(\d+)/.exec(error.message);
  if (!m) return;

  return { row: Number(m[1]) - 1, column: Number(m[2]) - 1 };
}

function stringifyJson(item: any): string {
  if (item != null) {
    const str = JSONBig.stringify(item, undefined, 2);
    if (str === '{}') return '{\n\n}'; // Very special case for an empty object to make it more beautiful
    return str;
  } else {
    return '';
  }
}

// Not the best way to check for deep equality but good enough for what we need
function deepEqual(a: any, b: any): boolean {
  return JSONBig.stringify(a) === JSONBig.stringify(b);
}

interface InternalValue {
  lastShownValue: any;
  error?: Error;
  stringified: string;
}

interface JsonInputProps {
  value: any;
  onChange?: (value: any) => void;
  setError?: (error: Error | undefined) => void;
  placeholder?: string;
  focus?: boolean;
  width?: string;
  height?: string;
  showLineNumbers?: boolean;
  issueWithValue?: (value: any) => string | undefined;
  jsonCompletions?: JsonCompletionRule[];
}

export const JsonInput = React.memo(function JsonInput(props: JsonInputProps) {
  const {
    onChange,
    setError,
    placeholder,
    focus,
    width,
    height,
    showLineNumbers,
    value,
    issueWithValue,
    jsonCompletions,
  } = props;
  const [internalValue, setInternalValue] = useState<InternalValue>(() => ({
    lastShownValue: value,
    stringified: stringifyJson(value),
  }));
  const [showErrorIfNeeded, setShowErrorIfNeeded] = useState(false);
  const editorViewRef = useRef<EditorView | undefined>(undefined);

  const showValue = useEffectEvent((value: any) => {
    if (deepEqual(value, internalValue.lastShownValue)) return;
    setInternalValue({
      lastShownValue: value,
      stringified: stringifyJson(value),
    });
  });

  useEffect(() => {
    showValue(value);
  }, [value]);

  const getCompletions = useCallback(
    ({ prefix, charBeforePrefix, textBeforePrefix }: CompletionRequest) => {
      if (!jsonCompletions) return [];
      return getHjsonCompletions({
        jsonCompletions,
        textBefore: textBeforePrefix,
        charBeforePrefix,
        prefix,
      });
    },
    [jsonCompletions],
  );

  const handleInputChange = (inputJson: string) => {
    let value: any;
    let error: Error | undefined;
    try {
      value = parseHjson(inputJson);
    } catch (e) {
      error = e;
    }

    if (!error && issueWithValue) {
      const issue = issueWithValue(value);
      if (issue) {
        value = undefined;
        error = new Error(issue);
      }
    }

    setInternalValue({
      lastShownValue: value ?? internalValue.lastShownValue,
      error,
      stringified: inputJson,
    });

    setError?.(error);
    if (!error) {
      onChange?.(value);
    }

    if (showErrorIfNeeded) {
      setShowErrorIfNeeded(false);
    }
  };

  const internalValueError = internalValue.error;
  return (
    <div className={classNames('json-input', { invalid: showErrorIfNeeded && internalValueError })}>
      <CodeEditor
        ref={editorViewRef}
        mode="hjson"
        onChange={onChange ? handleInputChange : undefined}
        onBlur={() => setShowErrorIfNeeded(true)}
        autoFocus={focus}
        width={width || '100%'}
        height={height || '8vh'}
        showGutter={Boolean(showLineNumbers)}
        value={internalValue.stringified}
        placeholder={placeholder}
        getCompletions={jsonCompletions ? getCompletions : undefined}
      />
      {showErrorIfNeeded && internalValueError && (
        <div
          className="json-error"
          onClick={() => {
            if (!editorViewRef.current || !internalValueError) return;

            const rc = extractRowColumnFromHjsonError(internalValueError);
            if (!rc) return;

            focusEditorAt(editorViewRef.current, rc);
          }}
        >
          {internalValueError.message}
        </div>
      )}
    </div>
  );
});
