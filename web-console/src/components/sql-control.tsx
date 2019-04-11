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

import { Button, Checkbox, Classes, FormGroup, Intent, Menu, Popover, Position } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import axios from 'axios';
import * as ace from 'brace';
import 'brace/ext/language_tools';
import 'brace/mode/hjson';
import 'brace/mode/sql';
import 'brace/theme/solarized_dark';
import * as classNames from 'classnames';
import * as React from 'react';
import AceEditor from 'react-ace';
import * as ReactDOMServer from 'react-dom/server';

import { SQLFunctionDoc } from '../../lib/sql-function-doc';
import { AppToaster } from '../singletons/toaster';

import './sql-control.scss';

const langTools = ace.acequire('ace/ext/language_tools');

export interface SqlControlProps extends React.Props<any> {
  initSql: string | null;
  onRun: (query: string) => void;
  onExplain: (query: string) => void;
  queryElapsed: number | null;
}

export interface SqlControlState {
  query: string;
  autoCompleteOn: boolean;
  autoCompleteLoading: boolean;
}

export class SqlControl extends React.Component<SqlControlProps, SqlControlState> {
  constructor(props: SqlControlProps, context: any) {
    super(props, context);
    this.state = {
      query: props.initSql || '',
      autoCompleteOn: true,
      autoCompleteLoading: false
    };
  }

  private addDatasourceAutoCompleter = async (): Promise<any> => {
    const datasourceResp = await axios.post('/druid/v2/sql', { query: `SELECT datasource FROM sys.segments GROUP BY 1`});
    const datasourceList: any[] = datasourceResp.data.map((d: any) => {
      const datasourceName: string = d.datasource;
      return {
        value: datasourceName,
        score: 50,
        meta: 'datasource'
      };
    });

    const completer = {
      getCompletions: (editor: any, session: any, pos: any, prefix: any, callback: any) => {
        callback(null, datasourceList);
      }
    };

    langTools.addCompleter(completer);
  }

  private addColumnNameAutoCompleter = async (): Promise<any> => {
    const columnNameResp = await axios.post('/druid/v2/sql', {query: `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'druid'`});
    const columnNameList: any[] = columnNameResp.data.map((d: any) => {
      const columnName: string = d.COLUMN_NAME;
      return {
        value: columnName,
        score: 50,
        meta: 'column'
      };
    });

    const completer = {
      getCompletions: (editor: any, session: any, pos: any, prefix: any, callback: any) => {
        callback(null, columnNameList);
      }
    };

    langTools.addCompleter(completer);
  }

  private addFunctionAutoCompleter = (): void => {
    const functionList: any[] = SQLFunctionDoc.map((entry: any) => {
      let funcName: string = entry.syntax.replace(/\(.*\)/, '()');
      if (!funcName.includes('(')) funcName = funcName.substr(0, 10);
      return {
        value: funcName,
        score: 80,
        meta: 'function',
        syntax: entry.syntax,
        description: entry.description,
        completer: {
          insertMatch: (editor: any, data: any) => {
            editor.completer.insertMatch({value: data.caption});
            const pos = editor.getCursorPosition();
            editor.gotoLine(pos.row + 1, pos.column - 1);
          }
        }
      };
    });

    const completer = {
      getCompletions: (editor: any, session: any, pos: any, prefix: any, callback: any) => {
        callback(null, functionList);
      },
      getDocTooltip: (item: any) => {
        if (item.meta === 'function') {
          const functionName = item.caption.slice(0, -2);
          item.docHTML = ReactDOMServer.renderToStaticMarkup((
            <div className="function-doc">
              <div className="function-doc-name"><b>{functionName}</b></div>
              <hr/>
              <div><b>Syntax:</b></div>
              <div>{item.syntax}</div>
              <br/>
              <div><b>Description:</b></div>
              <div>{item.description}</div>
            </div>
          ));
        }
      }
    };
    langTools.addCompleter(completer);
  }

  private addCompleters = async () => {
    try {
      this.addFunctionAutoCompleter();
      await this.addDatasourceAutoCompleter();
      await this.addColumnNameAutoCompleter();
    } catch (e) {
      AppToaster.show({
        message: 'Failed to load SQL auto completer',
        intent: Intent.DANGER
      });
    }
  }

  componentDidMount(): void {
    this.addCompleters();
  }

  private handleChange = (newValue: string): void => {
    this.setState({
      query: newValue
    });
  }

  render() {
    const { onRun, onExplain, queryElapsed } = this.props;
    const { query, autoCompleteOn } = this.state;

    const isRune = query.trim().startsWith('{');

    const SqlControlPopover = <Popover position={Position.BOTTOM_LEFT}>
        <Button minimal icon={IconNames.MORE}/>
        <div className="sql-control-popover">
          <Checkbox
            checked={isRune ? false : autoCompleteOn}
            label="Auto complete"
            onChange={() => this.setState({autoCompleteOn: !autoCompleteOn})}
          />
          <Button
            icon={IconNames.CLEAN}
            className={Classes.POPOVER_DISMISS}
            text="Explain"
            onClick={() => onExplain(query)}
            minimal
          />
        </div>
      </Popover>;

    // Set the key in the AceEditor to force a rebind and prevent an error that happens otherwise
    return <div className="sql-control">
      <AceEditor
        key={isRune ? 'hjson' : 'sql'}
        mode={isRune ? 'hjson' : 'sql'}
        theme="solarized_dark"
        name="ace-editor"
        onChange={this.handleChange}
        focus
        fontSize={14}
        width="100%"
        height="30vh"
        showPrintMargin={false}
        value={query}
        editorProps={{
          $blockScrolling: Infinity
        }}
        setOptions={{
          enableBasicAutocompletion: isRune ? false : autoCompleteOn,
          enableLiveAutocompletion: isRune ? false : autoCompleteOn,
          showLineNumbers: true,
          tabSize: 2
        }}
      />
      <div className="buttons">
        <Button rightIcon={IconNames.CARET_RIGHT} onClick={() => onRun(query)}>
          {isRune ? 'Rune' : 'Run'}
        </Button>
        {!isRune && SqlControlPopover}
        {
          queryElapsed &&
          <span className={'query-elapsed'}> Last query took {(queryElapsed / 1000).toFixed(2)} seconds</span>
        }
      </div>
    </div>;
  }
}
