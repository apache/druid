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

import classNames = require('classnames');
import Hjson from 'hjson';
import React, { useState } from 'react';
import AceEditor from 'react-ace';

import './json-input.scss';

function parseHjson(str: string) {
  return str === '' ? null : Hjson.parse(str);
}

function stringifyJson(item: any): string {
  if (item != null) {
    return JSON.stringify(item, null, 2);
  } else {
    return '';
  }
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
  const stringifiedValue = stringifyJson(value);
  const [stringValue, setStringValue] = useState(stringifiedValue);
  const [blurred, setBlurred] = useState(false);

  let parsedValue: any;
  try {
    parsedValue = parseHjson(stringValue);
  } catch {}
  if (typeof parsedValue !== 'object') parsedValue = undefined;

  if (parsedValue !== undefined && stringifyJson(parsedValue) !== stringifiedValue) {
    setStringValue(stringifiedValue);
  }

  return (
    <AceEditor
      className={classNames('json-input', { invalid: parsedValue === undefined && blurred })}
      mode="hjson"
      theme="solarized_dark"
      onChange={(inputJson: string) => {
        try {
          const value = parseHjson(inputJson);
          onChange(value);
        } catch {}
        setStringValue(inputJson);
      }}
      onFocus={() => setBlurred(false)}
      onBlur={() => setBlurred(true)}
      focus={focus}
      fontSize={12}
      width={width || '100%'}
      height={height || '8vh'}
      showPrintMargin={false}
      showGutter={false}
      value={stringValue}
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
    />
  );
});
