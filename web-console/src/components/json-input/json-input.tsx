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
import React, { useEffect, useRef, useState } from 'react';
import AceEditor from 'react-ace';

import './json-input.scss';

function parseHjson(str: string) {
  // Throwing on empty input is more consistent with how JSON.parse works
  if (str.trim() === '') throw new Error('empty hjson');
  return Hjson.parse(str);
}

function stringifyJson(item: any): string {
  if (item != null) {
    return JSON.stringify(item, null, 2);
  } else {
    return '';
  }
}

function deepEqual(a: any, b: any): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
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
}

export const JsonInput = React.memo(function JsonInput(props: JsonInputProps) {
  const { onChange, placeholder, focus, width, height, value } = props;
  const [internalValue, setInternalValue] = useState<InternalValue>(() => ({
    value,
    stringified: stringifyJson(value),
  }));
  const [showErrorIfNeeded, setShowErrorIfNeeded] = useState(false);
  const aceEditor = useRef<Editor | undefined>();

  useEffect(() => {
    if (!deepEqual(value, internalValue.value)) {
      console.log('FORCE CHANGE!');
      setInternalValue({
        value,
        stringified: stringifyJson(value),
      });
    }
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
            const m = internalValueError.message.match(/line (\d+),(\d+)/);
            if (!m) return;

            aceEditor.current.getSelection().moveCursorTo(Number(m[1]) - 1, Number(m[2]) - 1);
            aceEditor.current.focus(); // Grab the focus also
          }}
        >
          {internalValueError.message}
        </div>
      )}
    </div>
  );
});
