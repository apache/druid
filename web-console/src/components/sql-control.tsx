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
import {
  TextArea,
  Intent,
  Button
} from "@blueprintjs/core";

export interface SqlControlProps extends React.Props<any> {
  initSql: string | null;
  onRun: (query: string) => void;
}

export interface SqlControlState {
  query: string;
}

export class SqlControl extends React.Component<SqlControlProps, SqlControlState> {
  constructor(props: SqlControlProps, context: any) {
    super(props, context);
    this.state = {
      query: props.initSql || ''
    };
  }

  private handleChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    this.setState({
      query: e.target.value
    });
  }

  render() {
    const { onRun } = this.props;
    const { query } = this.state;

    const isRune = query.trim().startsWith('{');

    // Maybe use: https://github.com/securingsincity/react-ace/blob/master/docs/Ace.md
    return <div className="sql-control">
      <TextArea
        className="bp3-fill"
        large={true}
        intent={Intent.PRIMARY}
        onChange={this.handleChange}
        value={query}
      />
      <div className="buttons">
        <Button rightIcon="caret-right" onClick={() => onRun(query)}>{isRune ? 'Rune' : 'Run'}</Button>
      </div>
    </div>
  }
}

