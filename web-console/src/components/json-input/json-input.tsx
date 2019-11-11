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

import React from 'react';
import AceEditor from 'react-ace';

import { parseStringToJson, stringifyJson, validJson } from '../../utils';

interface JsonInputProps {
  onChange: (newJSONValue: any) => void;
  value: any;
  updateInputValidity?: (valueValid: boolean) => void;
  placeholder?: string;
  focus?: boolean;
  width?: string;
  height?: string;
}

interface JsonInputState {
  stringValue: string;
}

export class JsonInput extends React.PureComponent<JsonInputProps, JsonInputState> {
  constructor(props: JsonInputProps) {
    super(props);
    this.state = {
      stringValue: '',
    };
  }

  componentDidMount(): void {
    const { value } = this.props;
    const stringValue = stringifyJson(value);
    this.setState({
      stringValue,
    });
  }

  componentWillReceiveProps(nextProps: JsonInputProps): void {
    if (JSON.stringify(nextProps.value) !== JSON.stringify(this.props.value)) {
      this.setState({
        stringValue: stringifyJson(nextProps.value),
      });
    }
  }

  render(): JSX.Element {
    const { onChange, updateInputValidity, placeholder, focus, width, height } = this.props;
    const { stringValue } = this.state;
    return (
      <AceEditor
        key="hjson"
        mode="hjson"
        theme="solarized_dark"
        name="ace-editor"
        onChange={(e: string) => {
          this.setState({ stringValue: e });
          if (validJson(e) || e === '') onChange(parseStringToJson(e));
          if (updateInputValidity) updateInputValidity(validJson(e) || e === '');
        }}
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
  }
}
