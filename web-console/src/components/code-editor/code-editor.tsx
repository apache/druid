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
import type { LineColumn } from '../../utils';

import {
  codeEditorHighlightStyle,
  codeEditorPaddingTheme,
  codeEditorTheme,
  codeEditorTransparentTheme,
} from './code-editor-theme';
import { editorErrorField, lineColumnToOffset } from './error-mark';
import { createSearchPanel } from './search-panel';

import './code-editor.scss';

export type { EditorError } from './error-mark';
export { showEditorError } from './error-mark';

const TAB_SIZE = 2;

/**
 * Marks the changes that come from a new value being passed in (as opposed to the user editing the text)
 */
const externalChange = Annotation.define<boolean>();

export interface CodeEditorProps {
  ref?: React.Ref<EditorView | undefined>;
  className?: string;
  value: string;
  onChange?: (value: string) => void;
  readOnly?: boolean;
  onBlur?: () => void;
  /** The language (like dsql() or hjson()), it brings its own highlighting and completions. Plain text without one */
  language?: LanguageSupport;
  autoFocus?: boolean;
  /** For a size that changes, the rest is best set with CSS on className. With no height the editor grows with the text */
  style?: React.CSSProperties;
  showLineNumbers?: boolean;
  /** Pads the text on all sides (in px), the default is a bit of horizontal padding */
  padding?: number;
  /** Drops the editor's own background */
  transparentBackground?: boolean;
  placeholder?: string;
  /** Makes the editor remember its state (undo history, selection) between mounts, see forgetEditorState */
  stateCacheId?: string;
  /** Additional extensions, only read when the editor is created */
  extensions?: Extension;
}

/**
 * What EditorState.toJSON gives: the text, the selection and (with historyField) the undo history
 */
type SavedEditorState = Record<string, unknown>;

/**
 * The saved states of the unmounted editors that have a stateCacheId
 */
const savedEditorStates = new Map<string, SavedEditorState>();

/**
 * Forgets the state saved for an editor with the given stateCacheId (once that editor will not be shown again)
 */
export function forgetEditorState(stateCacheId: string): void {
  savedEditorStates.delete(stateCacheId);
}

let tooltipHost: HTMLElement | undefined;

/**
 * The element that the tooltips of all editors (the autocomplete list and its doc panel) are rendered in.
 *
 * - It is in the body so that the tooltips are not clipped by the containers of the editor: the
 *   editor wrapper itself (`overflow: hidden`), dialogs and popovers. CodeMirror's own z-index for tooltips puts them
 *   above Blueprint overlays.
 * - It is put at the start of the body rather than the end. Blueprint renders dialogs, popovers and toasts into portals
 *   appended to the body, and the dialog specs snapshot `document.body.lastChild` expecting that portal. Appending the
 *   host (it is created the first time an editor mounts) would make it the last child instead.
 * - All editors share it, so only one element is ever added.
 */
function getTooltipHost(): HTMLElement {
  if (!tooltipHost) {
    tooltipHost = document.createElement('div');
    tooltipHost.className = 'code-editor-tooltips';
    document.body.prepend(tooltipHost);
  }
  return tooltipHost;
}

function lineNumbersExtension(showLineNumbers: boolean | undefined): Extension {
  return showLineNumbers ? [lineNumbers(), highlightActiveLineGutter()] : [];
}

function paddingExtension(padding: number | undefined): Extension {
  return typeof padding === 'number' ? codeEditorPaddingTheme(padding) : [];
}

function transparentBackgroundExtension(transparentBackground: boolean | undefined): Extension {
  return transparentBackground ? codeEditorTransparentTheme : [];
}

function readOnlyExtension(readOnly: boolean): Extension {
  return EditorState.readOnly.of(readOnly);
}

function placeholderTextExtension(placeholder: string | undefined): Extension {
  return placeholder ? placeholderExtension(placeholder) : [];
}

/**
 * With nothing selected Tab inserts spaces up to the next tab stop (CodeMirror's indentWithTab would indent the whole
 * line), otherwise it indents the selected lines
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
 * Shows the documentation next to the completion list, aligned with its top (CodeMirror aligns it with the selected
 * option)
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
 * Focuses the editor and puts the cursor at the given (1-based) line and column, as reported by Druid and Hjson errors.
 * Positions past the end of the line or the text are clamped.
 */
export function focusEditorAt(view: EditorView, position: LineColumn): void {
  view.focus();
  view.dispatch({
    selection: { anchor: lineColumnToOffset(view.state.doc, position) },
    scrollIntoView: true,
  });
}

export function CodeEditor(props: CodeEditorProps) {
  const {
    ref,
    className,
    value,
    onChange,
    readOnly = false,
    onBlur,
    language,
    autoFocus,
    style,
    showLineNumbers,
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
    lineNumbers: new Compartment(),
    padding: new Compartment(),
    transparentBackground: new Compartment(),
    placeholder: new Compartment(),
  }));

  const handleChange = usePermanentCallback((newValue: string) => onChange?.(newValue));
  const handleBlur = usePermanentCallback(() => onBlur?.());
  const createView = useEffectEvent((container: HTMLElement) => {
    const editorExtensions: Extension = [
      history(),
      drawSelection(),
      bracketMatching(),
      search({ createPanel: createSearchPanel }),
      editorErrorField,
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
      compartments.padding.of(paddingExtension(padding)),
      compartments.transparentBackground.of(transparentBackgroundExtension(transparentBackground)),
      compartments.language.of(language ?? []),
      compartments.readOnly.of(readOnlyExtension(readOnly)),
      compartments.lineNumbers.of(lineNumbersExtension(showLineNumbers)),
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
    const cachedState = stateCacheId ? savedEditorStates.get(stateCacheId) : undefined;
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
    savedEditorStates.set(stateCacheId, view.state.toJSON({ history: historyField }));
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
      effects: compartments.lineNumbers.reconfigure(lineNumbersExtension(showLineNumbers)),
    });
  }, [compartments, showLineNumbers]);

  useEffect(() => {
    viewRef.current?.dispatch({
      effects: compartments.padding.reconfigure(paddingExtension(padding)),
    });
  }, [compartments, padding]);

  useEffect(() => {
    viewRef.current?.dispatch({
      effects: compartments.transparentBackground.reconfigure(
        transparentBackgroundExtension(transparentBackground),
      ),
    });
  }, [compartments, transparentBackground]);

  useEffect(() => {
    viewRef.current?.dispatch({
      effects: compartments.placeholder.reconfigure(placeholderTextExtension(placeholder)),
    });
  }, [compartments, placeholder]);

  return <div className={classNames('code-editor', className)} style={style} ref={containerRef} />;
}
