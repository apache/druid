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

import { ResizeSensor2 } from '@blueprintjs/popover2';
import { C, T } from '@druid-toolkit/query';
import type { Ace } from 'ace-builds';
import ace from 'ace-builds';
import classNames from 'classnames';
import escape from 'lodash.escape';
import React from 'react';
import AceEditor from 'react-ace';

import {
  SQL_CONSTANTS,
  SQL_DYNAMICS,
  SQL_EXPRESSION_PARTS,
  SQL_KEYWORDS,
} from '../../../../lib/keywords';
import { SQL_DATA_TYPES, SQL_FUNCTIONS } from '../../../../lib/sql-docs';
import { AceEditorStateCache } from '../../../singletons/ace-editor-state-cache';
import type { ColumnMetadata, RowColumn } from '../../../utils';
import { uniq } from '../../../utils';

import './flexible-query-input.scss';

const langTools = ace.require('ace/ext/language_tools');

const V_PADDING = 10;
const SCROLLBAR = 20;

const COMPLETER = {
  insertMatch: (editor: any, data: Ace.Completion) => {
    editor.completer.insertMatch({ value: data.name });
  },
};

interface ItemDescription {
  name: string;
  syntax: string;
  description: string;
}

export interface FlexibleQueryInputProps {
  queryString: string;
  onQueryStringChange?: (newQueryString: string) => void;
  autoHeight: boolean;
  minRows?: number;
  showGutter?: boolean;
  placeholder?: string;
  columnMetadata?: readonly ColumnMetadata[];
  currentSchema?: string;
  currentTable?: string;
  editorStateId?: string;
  leaveBackground?: boolean;
}

export interface FlexibleQueryInputState {
  // For reasons (https://github.com/securingsincity/react-ace/issues/415) react ace editor needs an explicit height
  // Since this component will grow and shrink dynamically we will measure its height and then set it.
  editorHeight: number;
  quotedCompletions: Ace.Completion[];
  unquotedCompletions: Ace.Completion[];
  prevColumnMetadata?: readonly ColumnMetadata[];
  prevCurrentTable?: string;
  prevCurrentSchema?: string;
}

export class FlexibleQueryInput extends React.PureComponent<
  FlexibleQueryInputProps,
  FlexibleQueryInputState
> {
  private aceEditor: Ace.Editor | undefined;

  static replaceDefaultAutoCompleter(): void {
    if (!langTools) return;

    const keywordList = ([] as Ace.Completion[]).concat(
      SQL_KEYWORDS.map(v => ({ name: v, value: v, score: 0, meta: 'keyword' })),
      SQL_EXPRESSION_PARTS.map(v => ({ name: v, value: v, score: 0, meta: 'keyword' })),
      SQL_CONSTANTS.map(v => ({ name: v, value: v, score: 0, meta: 'constant' })),
      SQL_DYNAMICS.map(v => ({ name: v, value: v, score: 0, meta: 'dynamic' })),
      Object.entries(SQL_DATA_TYPES).map(([name, [runtime, description]]) => ({
        name,
        value: name,
        score: 0,
        meta: 'type',
        syntax: `Druid runtime type: ${runtime}`,
        description,
      })),
    );

    langTools.setCompleters([
      langTools.snippetCompleter,
      langTools.textCompleter,
      {
        getCompletions: (
          _state: string,
          _session: Ace.EditSession,
          _pos: Ace.Point,
          _prefix: string,
          callback: any,
        ) => {
          return callback(null, keywordList);
        },
        getDocTooltip: (item: any) => {
          if (item.meta === 'type') {
            item.docHTML = FlexibleQueryInput.makeDocHtml(item);
          }
        },
      },
    ]);
  }

  static addFunctionAutoCompleter(): void {
    if (!langTools) return;

    const functionList: Ace.Completion[] = Object.entries(SQL_FUNCTIONS).flatMap(
      ([name, versions]) => {
        return versions.map(([args, description]) => ({
          name: name,
          value: versions.length > 1 ? `${name}(${args})` : name,
          score: 1100, // Use a high score to appear over the 'local' suggestions that have a score of 1000
          meta: 'function',
          syntax: `${name}(${args})`,
          description,
          completer: COMPLETER,
        }));
      },
    );

    langTools.addCompleter({
      getCompletions: (_editor: any, _session: any, _pos: any, _prefix: any, callback: any) => {
        callback(null, functionList);
      },
      getDocTooltip: (item: any) => {
        if (item.meta === 'function') {
          item.docHTML = FlexibleQueryInput.makeDocHtml(item);
        }
      },
    });
  }

  static makeDocHtml(item: ItemDescription) {
    return `
<div class="doc-name">${item.name}</div>
<div class="doc-syntax">${escape(item.syntax)}</div>
<div class="doc-description">${item.description}</div>`;
  }

  static getCompletions(
    columnMetadata: readonly ColumnMetadata[],
    currentSchema: string | undefined,
    currentTable: string | undefined,
    quote: boolean,
  ): Ace.Completion[] {
    return ([] as Ace.Completion[]).concat(
      uniq(columnMetadata.map(d => d.TABLE_SCHEMA)).map(v => ({
        value: quote ? String(T(v)) : v,
        score: 10,
        meta: 'schema',
      })),
      uniq(
        columnMetadata
          .filter(d => (currentSchema ? d.TABLE_SCHEMA === currentSchema : true))
          .map(d => d.TABLE_NAME),
      ).map(v => ({
        value: quote ? String(T(v)) : v,
        score: 49,
        meta: 'datasource',
      })),
      uniq(
        columnMetadata
          .filter(d =>
            currentTable && currentSchema
              ? d.TABLE_NAME === currentTable && d.TABLE_SCHEMA === currentSchema
              : true,
          )
          .map(d => d.COLUMN_NAME),
      ).map(v => ({
        value: quote ? String(C(v)) : v,
        score: 50,
        meta: 'column',
      })),
    );
  }

  static getDerivedStateFromProps(props: FlexibleQueryInputProps, state: FlexibleQueryInputState) {
    const { columnMetadata, currentSchema, currentTable } = props;

    if (
      columnMetadata &&
      (columnMetadata !== state.prevColumnMetadata ||
        currentSchema !== state.prevCurrentSchema ||
        currentTable !== state.prevCurrentTable)
    ) {
      return {
        quotedCompletions: FlexibleQueryInput.getCompletions(
          columnMetadata,
          currentSchema,
          currentTable,
          true,
        ),
        unquotedCompletions: FlexibleQueryInput.getCompletions(
          columnMetadata,
          currentSchema,
          currentTable,
          false,
        ),
        prevColumnMetadata: columnMetadata,
        prevCurrentSchema: currentSchema,
        prevCurrentTable: currentTable,
      };
    }
    return null;
  }

  constructor(props: FlexibleQueryInputProps) {
    super(props);
    this.state = {
      editorHeight: 200,
      quotedCompletions: [],
      unquotedCompletions: [],
    };
  }

  componentDidMount(): void {
    FlexibleQueryInput.replaceDefaultAutoCompleter();
    FlexibleQueryInput.addFunctionAutoCompleter();
    if (langTools) {
      langTools.addCompleter({
        getCompletions: (
          _state: string,
          session: Ace.EditSession,
          pos: Ace.Point,
          prefix: string,
          callback: any,
        ) => {
          const charBeforePrefix = session.getLine(pos.row)[pos.column - prefix.length - 1];
          callback(
            null,
            charBeforePrefix === '"'
              ? this.state.unquotedCompletions
              : this.state.quotedCompletions,
          );
        },
      });
    }
  }

  componentWillUnmount() {
    const { editorStateId } = this.props;
    if (editorStateId && this.aceEditor) {
      AceEditorStateCache.saveState(editorStateId, this.aceEditor);
    }
  }

  private readonly handleAceContainerResize = (entries: ResizeObserverEntry[]) => {
    if (entries.length !== 1) return;
    this.setState({ editorHeight: entries[0].contentRect.height });
  };

  private readonly handleChange = (value: string) => {
    const { onQueryStringChange } = this.props;
    if (!onQueryStringChange) return;
    onQueryStringChange(value);
  };

  public goToPosition(rowColumn: RowColumn) {
    const { aceEditor } = this;
    if (!aceEditor) return;
    aceEditor.focus(); // Grab the focus
    aceEditor.getSelection().moveCursorTo(rowColumn.row, rowColumn.column);
    if (rowColumn.endRow && rowColumn.endColumn) {
      aceEditor
        .getSelection()
        .selectToPosition({ row: rowColumn.endRow, column: rowColumn.endColumn });
    }
  }

  renderAce() {
    const {
      queryString,
      onQueryStringChange,
      autoHeight,
      minRows,
      showGutter,
      placeholder,
      editorStateId,
    } = this.props;
    const { editorHeight } = this.state;

    const jsonMode = queryString.trim().startsWith('{');

    let height: number;
    if (autoHeight) {
      height =
        Math.max(queryString.split('\n').length, minRows ?? 2) * 18 + 2 * V_PADDING + SCROLLBAR;
    } else {
      height = editorHeight;
    }

    return (
      <AceEditor
        mode={jsonMode ? 'hjson' : 'dsql'}
        theme="solarized_dark"
        className={classNames(
          'placeholder-padding',
          this.props.leaveBackground ? undefined : 'no-background',
        )}
        name="ace-editor"
        onChange={this.handleChange}
        focus
        fontSize={13}
        width="100%"
        height={height + 'px'}
        showGutter={showGutter}
        showPrintMargin={false}
        value={queryString}
        readOnly={!onQueryStringChange}
        editorProps={{
          $blockScrolling: Infinity,
        }}
        setOptions={{
          enableBasicAutocompletion: !jsonMode,
          enableLiveAutocompletion: !jsonMode,
          showLineNumbers: true,
          tabSize: 2,
          newLineMode: 'unix' as any, // This type is specified incorrectly in AceEditor
        }}
        style={{}}
        placeholder={placeholder || 'SELECT * FROM ...'}
        onLoad={(editor: Ace.Editor) => {
          editor.renderer.setPadding(V_PADDING);
          editor.renderer.setScrollMargin(V_PADDING, V_PADDING, 0, 0);

          if (editorStateId) {
            AceEditorStateCache.applyState(editorStateId, editor);
          }

          this.aceEditor = editor;
        }}
      />
    );
  }

  render() {
    const { autoHeight } = this.props;

    // Set the key in the AceEditor to force a rebind and prevent an error that happens otherwise
    return (
      <div className="flexible-query-input">
        {autoHeight ? (
          this.renderAce()
        ) : (
          <ResizeSensor2 onResize={this.handleAceContainerResize}>
            <div className="ace-container">{this.renderAce()}</div>
          </ResizeSensor2>
        )}
      </div>
    );
  }
}
