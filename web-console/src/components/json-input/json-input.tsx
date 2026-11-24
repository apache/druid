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
import React, { useEffect, useEffectEvent, useRef, useState } from 'react';

import { hjson } from '../../editor-languages/hjson';
import type { JsonCompletionRule, LineColumn } from '../../utils';
import type { EditorError } from '../code-editor/code-editor';
import { CodeEditor, focusEditorAt, showEditorError } from '../code-editor/code-editor';

import './json-input.scss';

function parseHjson(str: string): any {
  if (str.trim() === '') return;
  return Hjson.parse(str);
}

function lineColumnFromHjsonMessage(message: string): LineColumn | undefined {
  // Message would be something like:
  // `Found '}' where a key name was expected at line 26,7`
  const m = /line (\d+),(\d+)/.exec(message);
  if (!m) return;

  return { line: Number(m[1]), column: Number(m[2]) };
}

export function extractLineColumnFromHjsonError(error: Error): LineColumn | undefined {
  // Use this to extract the line and column and jump the cursor to the right place on click
  return lineColumnFromHjsonMessage(error.message);
}

/**
 * The error to mark in the editor for an Hjson error message, undefined if the message has no position
 */
export function getHjsonEditorError(message: string): EditorError | undefined {
  const position = lineColumnFromHjsonMessage(message);
  if (!position) return;

  // The mark shows where the error is, so the position (and the text after it, following ">>>") is left out
  const positionIndex = message.search(/\sat line \d+,\d+/);
  return {
    position,
    message: positionIndex === -1 ? message : message.slice(0, positionIndex).trimEnd(),
  };
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
  readOnly?: boolean;
  setError?: (error: Error | undefined) => void;
  placeholder?: string;
  autoFocus?: boolean;
  /** A CSS height, defaults to 8vh */
  height?: string;
  showLineNumbers?: boolean;
  issueWithValue?: (value: any) => string | undefined;
  jsonCompletions?: JsonCompletionRule[];
}

export const JsonInput = React.memo(function JsonInput(props: JsonInputProps) {
  const {
    onChange,
    readOnly,
    setError,
    placeholder,
    autoFocus,
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
  const shownError = showErrorIfNeeded ? internalValueError : undefined;
  useEffect(() => {
    if (!editorViewRef.current) return;
    showEditorError(
      editorViewRef.current,
      shownError ? getHjsonEditorError(shownError.message) : undefined,
    );
  }, [shownError]);

  return (
    <div className={classNames('json-input', { invalid: showErrorIfNeeded && internalValueError })}>
      <CodeEditor
        ref={editorViewRef}
        language={hjson({ jsonCompletions })}
        onChange={handleInputChange}
        readOnly={readOnly}
        onBlur={() => setShowErrorIfNeeded(true)}
        autoFocus={autoFocus}
        style={{ height: height || '8vh' }}
        showLineNumbers={showLineNumbers}
        value={internalValue.stringified}
        placeholder={placeholder}
      />
      {showErrorIfNeeded && internalValueError && (
        <div
          className="json-error"
          onClick={() => {
            if (!editorViewRef.current || !internalValueError) return;

            const position = extractLineColumnFromHjsonError(internalValueError);
            if (!position) return;

            focusEditorAt(editorViewRef.current, position);
          }}
        >
          {internalValueError.message}
        </div>
      )}
    </div>
  );
});
