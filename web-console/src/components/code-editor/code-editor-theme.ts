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

import { HighlightStyle } from '@codemirror/language';
import { EditorView } from '@codemirror/view';
import { tags } from '@lezer/highlight';

import { editorTags } from '../../editor-languages/rule-parser';

// The editor is styled to look like the Ace editor (with the solarized_dark theme and some overrides) that the console
// used to use.

const MONOSPACE_FONT =
  "Monaco, Menlo, 'Ubuntu Mono', Consolas, 'Source Code Pro', source-code-pro, monospace";

// Colors from blueprint-overrides/common/_colors.scss
const DARK_GRAY1 = '24, 28, 45';
const DARK_GRAY4 = '#383d57';
const GRAY1 = '96, 101, 128';
const GRAY5 = '#bdc1d1';

const POPUP_SHADOW = '0 5px 15px rgba(15, 19, 32, 0.45)';

export const codeEditorTheme = EditorView.theme(
  {
    '&': {
      height: '100%',
      fontSize: '12px',
      color: '#c7dde0',
      backgroundColor: `rgba(${DARK_GRAY1}, 0.5)`,
    },
    '.no-background > &': {
      backgroundColor: 'transparent',
    },
    '&.cm-focused': {
      outline: 'none',
    },
    '.cm-scroller': {
      fontFamily: MONOSPACE_FONT,
      lineHeight: 'normal',
    },
    '.cm-content': {
      padding: '0',
    },
    '.cm-line': {
      padding: '0 4px',
    },
    '.cm-cursor, .cm-dropCursor': {
      borderLeft: '2px solid #d30102',
    },
    '&.cm-focused > .cm-scroller > .cm-selectionLayer .cm-selectionBackground, .cm-selectionBackground':
      {
        background: 'rgba(255, 255, 255, 0.1)',
      },
    '.cm-activeLine': {
      backgroundColor: 'rgba(255, 255, 255, 0.1)',
    },
    '.cm-gutters': {
      backgroundColor: DARK_GRAY4,
      color: GRAY5,
      border: 'none',
    },
    '.cm-activeLineGutter': {
      backgroundColor: `rgba(${GRAY1}, 0.6)`,
    },
    '.cm-lineNumbers .cm-gutterElement': {
      padding: '0 13px 0 21px',
      minWidth: '0',
    },
    '.cm-placeholder': {
      color: '#657b83',
      opacity: '0.7',
      fontStyle: 'italic',
      fontFamily: 'arial',
      transform: 'scale(0.9)',
      transformOrigin: 'left',
    },
    '&.cm-focused .cm-matchingBracket': {
      backgroundColor: 'transparent',
      outline: '1px solid rgba(147, 161, 161, 0.5)',
    },
    '&.cm-focused .cm-nonmatchingBracket': {
      backgroundColor: 'transparent',
    },
    '.cm-panels': {
      backgroundColor: DARK_GRAY4,
    },

    // Autocomplete
    '.cm-tooltip': {
      border: 'none',
      borderRadius: '2px',
      backgroundColor: DARK_GRAY4,
      boxShadow: POPUP_SHADOW,
    },
    '.cm-tooltip.cm-tooltip-autocomplete > ul': {
      width: '300px',
      maxHeight: `${8 * 1.4}em`,
      fontFamily: MONOSPACE_FONT,
      fontSize: '12px',
      lineHeight: '1.4',
      color: '#d4d4d4',
    },
    '.cm-tooltip.cm-tooltip-autocomplete > ul > li': {
      display: 'flex',
      padding: '0 4px',
      lineHeight: '1.4',
    },
    '.cm-tooltip.cm-tooltip-autocomplete > ul > li:hover': {
      backgroundColor: 'rgba(58, 103, 78, 0.62)',
      boxShadow: 'inset 0 0 0 1px rgba(109, 150, 13, 0.8)',
    },
    '.cm-tooltip-autocomplete ul li[aria-selected]': {
      backgroundColor: '#3a674e',
      color: 'inherit',
    },
    '.cm-completionLabel': {
      flex: '0 1 auto',
      overflow: 'hidden',
      textOverflow: 'ellipsis',
    },
    '.cm-completionMatchedText': {
      textDecoration: 'none',
      color: '#a2de14',
    },
    '.cm-completionDetail': {
      flex: 'none',
      marginLeft: 'auto',
      paddingLeft: '0.9em',
      fontStyle: 'normal',
      opacity: '0.5',
    },
    '.cm-tooltip.cm-completionInfo': {
      boxSizing: 'border-box',
      width: '500px',
      maxWidth: 'none',
      padding: '10px',
      whiteSpace: 'initial',
      color: '#c1ccd5',
      backgroundColor: DARK_GRAY4,
      boxShadow: POPUP_SHADOW,
    },
    '.cm-completionInfo > *': {
      filter: 'brightness(1.1)',
    },
    '.cm-completionInfo .doc-name': {
      fontSize: '18px',
      borderBottom: '2px solid rgba(193, 204, 213, 0.5)',
      paddingBottom: '4px',
      color: '#93ca12',
    },
    '.cm-completionInfo .doc-syntax': {
      paddingTop: '8px',
      paddingBottom: '10px',
      whiteSpace: 'pre-wrap', // One line per signature
    },
    '.cm-completionInfo .doc-name, .cm-completionInfo .doc-syntax': {
      fontFamily: MONOSPACE_FONT,
    },
  },
  { dark: true },
);

/**
 * Pads the text by the given amount (the default is a bit of horizontal padding)
 */
export function codeEditorPaddingTheme(padding: number) {
  // The selectors are more specific than the ones in codeEditorTheme so that they win
  return EditorView.theme({
    '.cm-scroller .cm-content': {
      padding: `${padding}px 0`,
    },
    '.cm-content .cm-line': {
      padding: `0 ${padding}px`,
    },
  });
}

// The colors were brightened from the solarized_dark ones with `filter: brightness(1.5) saturate(0.9)`
export const codeEditorHighlightStyle = HighlightStyle.define([
  { tag: tags.keyword, color: '#c8e315' },
  { tag: tags.function(tags.variableName), color: '#45cef7' },
  { tag: [tags.atom, tags.escape], color: '#facd14' },
  { tag: tags.typeName, color: '#49f943' },
  { tag: tags.number, color: '#f256bc' },
  { tag: tags.string, color: '#4deee1' },
  { tag: tags.comment, color: '#9ab8c3', fontStyle: 'italic' },
  {
    tag: editorTags.issue,
    color: '#f04d29',
    fontStyle: 'italic',
    textDecoration: 'underline wavy',
  },
  { tag: editorTags.column, color: '#51fbfb' },
]);
