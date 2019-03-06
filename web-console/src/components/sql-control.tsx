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

import * as React from 'react';
import * as classNames from 'classnames';
import * as ace from 'brace'
import AceEditor from "react-ace";
import 'brace/mode/sql';
import 'brace/mode/hjson';
import 'brace/theme/solarized_dark';
import 'brace/ext/language_tools';
import { Intent, Button } from "@blueprintjs/core";
import { IconNames } from './filler';

export interface SqlControlProps extends React.Props<any> {
  initSql: string | null;
  onRun: (query: string) => void;
}

export interface SqlControlState {
  query: string;
  autoCompleteOn: boolean;
}

export class SqlControl extends React.Component<SqlControlProps, SqlControlState> {
  constructor(props: SqlControlProps, context: any) {
    super(props, context);
    this.state = {
      query: props.initSql || '',
      autoCompleteOn: true
    };
  }

  private handleChange = (newValue: string): void => {
    this.setState({
      query: newValue
    })
  }

  render() {
    const { onRun } = this.props;
    const { query, autoCompleteOn } = this.state;

    const isRune = query.trim().startsWith('{');

    // Set the key in the AceEditor to force a rebind and prevent an error that happens otherwise
    return <div className="sql-control">
      <AceEditor
        key={isRune ? "hjson" : "sql"}
        mode={isRune ? "hjson" : "sql"}
        theme="solarized_dark"
        name="ace-editor"
        onChange={this.handleChange}
        focus={true}
        fontSize={14}
        width={'100%'}
        height={"30vh"}
        showPrintMargin={false}
        value={query}
        editorProps={{
          $blockScrolling: Infinity
        }}
        setOptions={{
          enableBasicAutocompletion: isRune ? false : autoCompleteOn,
          enableLiveAutocompletion: isRune ? false : autoCompleteOn,
          showLineNumbers: true,
          tabSize: 2,
        }}
      />
      <div className="buttons">
        <Button rightIconName={IconNames.CARET_RIGHT} onClick={() => onRun(query)}>{isRune ? 'Rune' : 'Run'}</Button>
      </div>
    </div>
  }
}

