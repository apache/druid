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

import { Editor } from 'brace';
import classNames from 'classnames';
import Hjson from 'hjson';
import * as JSONBig from 'json-bigint-native';
import React, { useEffect, useRef, useState } from 'react';
import AceEditor from 'react-ace';

import './json-input.scss';

function parseHjson(str: string) {
  // Throwing on empty input is more consistent with how JSON.parse works
  if (str.trim() === '') throw new Error('empty hjson');
  return Hjson.parse(str);
}

export function extractRowColumnFromHjsonError(
  error: Error,
): { row: number; column: number } | undefined {
  // Message would be something like:
  // `Found '}' where a key name was expected at line 26,7`
  // Use this to extract the row and column (subtract 1) and jump the cursor to the right place on click
  const m = error.message.match(/line (\d+),(\d+)/);
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
  value?: any;
  error?: Error;
  stringified: string;
}

interface JsonInputProps {
  value: any;
  onChange: (value: any) => void;
  placeholder?: string;
  focus?: boolean;
  width?: string;
  height?: string;
  issueWithValue?: (value: any) => string | undefined;
}

export const JsonInput = React.memo(function JsonInput(props: JsonInputProps) {
  const { onChange, placeholder, focus, width, height, value, issueWithValue } = props;
  const [internalValue, setInternalValue] = useState<InternalValue>(() => ({
    value,
    stringified: stringifyJson(value),
  }));
  const [showErrorIfNeeded, setShowErrorIfNeeded] = useState(false);
  const aceEditor = useRef<Editor | undefined>();

  useEffect(() => {
    if (deepEqual(value, internalValue.value)) return;
    setInternalValue({
      value,
      stringified: stringifyJson(value),
    });
  }, [value]);

  const internalValueError = internalValue.error;
  return (
    <div className={classNames('json-input', { invalid: showErrorIfNeeded && internalValueError })}>
      <AceEditor
        mode="hjson"
        theme="solarized_dark"
        onChange={(inputJson: string) => {
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
            value,
            error,
            stringified: inputJson,
          });

          if (!error) {
            onChange(value);
          }

          if (showErrorIfNeeded) {
            setShowErrorIfNeeded(false);
          }
        }}
        onBlur={() => setShowErrorIfNeeded(true)}
        focus={focus}
        fontSize={12}
        width={width || '100%'}
        height={height || '8vh'}
        showPrintMargin={false}
        showGutter={false}
        value={internalValue.stringified}
        placeholder={placeholder}
        editorProps={{
          $blockScrolling: Infinity,
        }}
        setOptions={{
          enableBasicAutocompletion: false,
          enableLiveAutocompletion: false,
          showLineNumbers: false,
          tabSize: 2,
        }}
        style={{}}
        onLoad={(editor: any) => {
          aceEditor.current = editor;
        }}
      />
      {showErrorIfNeeded && internalValueError && (
        <div
          className="json-error"
          onClick={() => {
            if (!aceEditor.current || !internalValueError) return;

            const rc = extractRowColumnFromHjsonError(internalValueError);
            if (!rc) return;

            aceEditor.current.focus(); // Grab the focus
            aceEditor.current.getSelection().moveCursorTo(rc.row, rc.column);
          }}
        >
          {internalValueError.message}
        </div>
      )}
    </div>
  );
});
