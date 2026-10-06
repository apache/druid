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

import { acceptCompletion, autocompletion } from '@codemirror/autocomplete';
import {
  defaultKeymap,
  history,
  historyField,
  historyKeymap,
  indentLess,
  indentMore,
} from '@codemirror/commands';
import type { LanguageSupport } from '@codemirror/language';
import { bracketMatching, indentUnit, syntaxHighlighting } from '@codemirror/language';
import { search, searchKeymap } from '@codemirror/search';
import type { ChangeSpec, Extension } from '@codemirror/state';
import {
  Annotation,
  Compartment,
  countColumn,
  EditorSelection,
  EditorState,
} from '@codemirror/state';
import type { Command, Rect } from '@codemirror/view';
import {
  drawSelection,
  EditorView,
  highlightActiveLine,
  highlightActiveLineGutter,
  keymap,
  lineNumbers,
  placeholder as placeholderExtension,
  tooltips,
} from '@codemirror/view';
import classNames from 'classnames';
import type React from 'react';
import { useEffect, useEffectEvent, useImperativeHandle, useLayoutEffect, useRef } from 'react';

import { useConstant, usePermanentCallback } from '../../hooks';
import { EditorStateCache } from '../../singletons/editor-state-cache';
import type { RowColumn } from '../../utils';

import {
  codeEditorHighlightStyle,
  codeEditorPaddingTheme,
  codeEditorTheme,
} from './code-editor-theme';

import './code-editor.scss';

const TAB_SIZE = 2;

/**
 * Marks the changes that come from a new value being passed in (as opposed to the user editing the text)
 */
const externalChange = Annotation.define<boolean>();

export interface CodeEditorProps {
  ref?: React.Ref<EditorView | undefined>;
  className?: string;
  value: string;
  /** Without an onChange the editor is read only */
  onChange?: (value: string) => void;
  onBlur?: () => void;
  /** The language (like dsql() or hjson()), it brings its own highlighting and completions. Plain text without one */
  language?: LanguageSupport;
  autoFocus?: boolean;
  width?: string;
  height?: string;
  showGutter?: boolean;
  /** Pads the text on all sides */
  padding?: number;
  transparentBackground?: boolean;
  placeholder?: string;
  /** Makes the editor remember its state (undo history, selection) between mounts */
  stateCacheId?: string;
  /** Additional extensions, only read when the editor is created */
  extensions?: Extension;
}

let tooltipHost: HTMLElement | undefined;

/**
 * Like Ace, show the tooltips (like the autocomplete list) in the body so that they are not clipped by the containers of
 * the editor. All editors share one host element that is put at the start of the body to stay out of the way of the
 * elements (like Blueprint portals) that are added to the end of it.
 */
function getTooltipHost(): HTMLElement {
  if (!tooltipHost) {
    tooltipHost = document.createElement('div');
    tooltipHost.className = 'code-editor-tooltips';
    document.body.prepend(tooltipHost);
  }
  return tooltipHost;
}

function gutterExtension(showGutter: boolean | undefined): Extension {
  return showGutter ? [lineNumbers(), highlightActiveLineGutter()] : [];
}

function readOnlyExtension(readOnly: boolean): Extension {
  return EditorState.readOnly.of(readOnly);
}

function placeholderTextExtension(placeholder: string | undefined): Extension {
  return placeholder ? placeholderExtension(placeholder) : [];
}

/**
 * Like Ace: with nothing selected Tab inserts spaces up to the next tab stop, otherwise it indents the selected lines
 */
const insertSoftTab: Command = view => {
  const { state } = view;
  if (state.readOnly) return false;
  if (state.selection.ranges.some(range => !range.empty)) return indentMore(view);

  view.dispatch(
    state.changeByRange(range => {
      const line = state.doc.lineAt(range.head);
      const column = countColumn(line.text.slice(0, range.head - line.from), TAB_SIZE);
      const spaces = ' '.repeat(TAB_SIZE - (column % TAB_SIZE));
      return {
        changes: { from: range.head, insert: spaces },
        range: EditorSelection.cursor(range.head + spaces.length),
      };
    }),
    { scrollIntoView: true, userEvent: 'input.indent' },
  );
  return true;
};

/**
 * Like Ace: show the documentation next to the completion list, aligned with its top
 */
function positionInfo(_view: EditorView, list: Rect, _option: Rect, info: Rect, space: Rect) {
  const infoWidth = info.right - info.left;
  const infoHeight = info.bottom - info.top;
  const spaceLeft = list.left - space.left;
  const spaceRight = space.right - list.right;
  const left = spaceRight < infoWidth && spaceLeft > spaceRight;
  const top = Math.max(space.top, Math.min(list.top, space.bottom - infoHeight)) - list.top;
  return {
    style: `top: ${top}px`,
    class: left ? 'cm-completionInfo-left' : 'cm-completionInfo-right',
  };
}

/**
 * The smallest change that turns one string into the other
 */
function diffStrings(from: string, to: string): ChangeSpec {
  const minLength = Math.min(from.length, to.length);
  let start = 0;
  while (start < minLength && from[start] === to[start]) start++;
  let fromEnd = from.length;
  let toEnd = to.length;
  while (fromEnd > start && toEnd > start && from[fromEnd - 1] === to[toEnd - 1]) {
    fromEnd--;
    toEnd--;
  }
  return { from: start, to: fromEnd, insert: to.slice(start, toEnd) };
}

/**
 * Focuses the editor and puts the cursor at the given (0 based) row and column
 */
export function focusEditorAt(view: EditorView, { row, column }: RowColumn): void {
  const { doc } = view.state;
  const line = doc.line(Math.min(Math.max(row + 1, 1), doc.lines));
  view.focus();
  view.dispatch({
    selection: { anchor: Math.min(line.from + Math.max(column, 0), line.to) },
    scrollIntoView: true,
  });
}

export function CodeEditor(props: CodeEditorProps) {
  const {
    ref,
    className,
    value,
    onChange,
    onBlur,
    language,
    autoFocus,
    width,
    height,
    showGutter,
    padding,
    transparentBackground,
    placeholder,
    stateCacheId,
    extensions,
  } = props;

  const containerRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<EditorView | undefined>(undefined);
  const compartments = useConstant(() => ({
    language: new Compartment(),
    readOnly: new Compartment(),
    gutter: new Compartment(),
    placeholder: new Compartment(),
  }));
  const readOnly = !onChange;

  const handleChange = usePermanentCallback((newValue: string) => onChange?.(newValue));
  const handleBlur = usePermanentCallback(() => onBlur?.());
  const createView = useEffectEvent((container: HTMLElement) => {
    const editorExtensions: Extension = [
      history(),
      drawSelection(),
      bracketMatching(),
      search(),
      highlightActiveLine(),
      syntaxHighlighting(codeEditorHighlightStyle),
      autocompletion({
        icons: false,
        positionInfo,
      }),
      tooltips({ parent: getTooltipHost() }),
      keymap.of([
        { key: 'Tab', run: acceptCompletion },
        { key: 'Tab', run: insertSoftTab, shift: indentLess },
        ...defaultKeymap,
        ...searchKeymap,
        ...historyKeymap,
      ]),
      EditorState.tabSize.of(TAB_SIZE),
      indentUnit.of(' '.repeat(TAB_SIZE)),
      codeEditorTheme,
      typeof padding === 'number' ? codeEditorPaddingTheme(padding) : [],
      compartments.language.of(language ?? []),
      compartments.readOnly.of(readOnlyExtension(readOnly)),
      compartments.gutter.of(gutterExtension(showGutter)),
      compartments.placeholder.of(placeholderTextExtension(placeholder)),
      EditorView.updateListener.of(update => {
        if (!update.docChanged) return;
        if (update.transactions.every(tr => tr.annotation(externalChange))) return;
        handleChange(update.state.doc.toString());
      }),
      EditorView.domEventHandlers({ blur: () => handleBlur() }),
      extensions ?? [],
    ];

    let state: EditorState | undefined;
    const cachedState = stateCacheId ? EditorStateCache.getState(stateCacheId) : undefined;
    if (cachedState) {
      try {
        state = EditorState.fromJSON(
          cachedState,
          { extensions: editorExtensions },
          { history: historyField },
        );
      } catch {
        // Fall back to a fresh state
      }
    }
    state ??= EditorState.create({ doc: value, extensions: editorExtensions });

    const view = new EditorView({ state, parent: container });
    const doc = state.doc.toString();
    if (doc !== value) {
      view.dispatch({
        changes: diffStrings(doc, value),
        annotations: externalChange.of(true),
      });
    }
    if (autoFocus) view.focus();
    return view;
  });

  const saveState = useEffectEvent((view: EditorView) => {
    if (!stateCacheId) return;
    EditorStateCache.saveState(stateCacheId, view.state.toJSON({ history: historyField }));
  });

  useLayoutEffect(() => {
    if (!containerRef.current) return;
    const view = createView(containerRef.current);
    viewRef.current = view;
    return () => {
      saveState(view);
      view.destroy();
      viewRef.current = undefined;
    };
  }, [stateCacheId]);

  useImperativeHandle(ref, () => viewRef.current);

  useEffect(() => {
    const view = viewRef.current;
    if (!view) return;
    const doc = view.state.doc.toString();
    if (doc === value) return;
    view.dispatch({
      changes: diffStrings(doc, value),
      annotations: externalChange.of(true),
    });
  }, [value]);

  useEffect(() => {
    viewRef.current?.dispatch({
      effects: compartments.language.reconfigure(language ?? []),
    });
  }, [compartments, language]);

  useEffect(() => {
    viewRef.current?.dispatch({
      effects: compartments.readOnly.reconfigure(readOnlyExtension(readOnly)),
    });
  }, [compartments, readOnly]);

  useEffect(() => {
    viewRef.current?.dispatch({
      effects: compartments.gutter.reconfigure(gutterExtension(showGutter)),
    });
  }, [compartments, showGutter]);

  useEffect(() => {
    viewRef.current?.dispatch({
      effects: compartments.placeholder.reconfigure(placeholderTextExtension(placeholder)),
    });
  }, [compartments, placeholder]);

  return (
    <div
      className={classNames('code-editor', className, { 'no-background': transparentBackground })}
      style={{ width, height }}
      ref={containerRef}
    />
  );
}
