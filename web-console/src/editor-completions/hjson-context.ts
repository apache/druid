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

import type { EditorState } from '@codemirror/state';
import type { SyntaxNode } from '@lezer/common';
import Hjson from 'hjson';

import { completeSyntaxTree } from '../components/code-editor/completion-source';

/**
 * Where the cursor is in an Hjson document
 */
export interface HjsonContext {
  /**
   * The path of keys leading to the object or array that the cursor is in, e.g. ["query", "dataSource"]. For arrays,
   * includes the index as a string key, e.g. ["filters", "0", "dimension"]. Empty at the root.
   */
  path: string[];

  /** Whether the cursor is where a key goes (true) or where a value goes (false) */
  isEditingKey: boolean;

  /** When editing a value, the key of that value (the index as a string in an array). Undefined when editing a key. */
  currentKey?: string;

  /**
   * The object that the cursor is in (for an array, the object that the array is in), without the property that is
   * being edited. It includes the properties after the cursor.
   */
  currentObject: Record<string, unknown>;
}

const VALUE_NODES = new Set([
  'True',
  'False',
  'Null',
  'Number',
  'String',
  'MultilineString',
  'QuotelessString',
  'Object',
  'Array',
]);

function valueChildren(node: SyntaxNode): SyntaxNode[] {
  const children: SyntaxNode[] = [];
  for (let child = node.firstChild; child; child = child.nextSibling) {
    if (VALUE_NODES.has(child.name)) children.push(child);
  }
  return children;
}

function isSameNode(a: SyntaxNode | undefined, b: SyntaxNode): boolean {
  return Boolean(a && a.from === b.from && a.to === b.to && a.name === b.name);
}

function stringValue(text: string): string {
  try {
    return JSON.parse(text);
  } catch {
    // Not closed yet (it ends at the end of the line), or with a bad escape
    return text.replace(/^"/, '').replace(/(?:"|\r?\n)$/, '');
  }
}

function propertyKey(state: EditorState, property: SyntaxNode): string | undefined {
  const name = property.getChild('PropertyName');
  // An empty name is what the error recovery makes of a missing key
  if (!name || name.from === name.to) return;
  const text = state.sliceDoc(name.from, name.to);
  return name.firstChild?.name === 'String' ? stringValue(text) : text;
}

/**
 * The value of a property, if it has a colon and a value
 */
function propertyValue(property: SyntaxNode): SyntaxNode | undefined {
  if (!property.getChild(':')) return;
  return valueChildren(property)[0];
}

/**
 * The value of a node, close enough for the completion rules to look at
 */
function nodeValue(state: EditorState, node: SyntaxNode): unknown {
  const text = state.sliceDoc(node.from, node.to);
  switch (node.name) {
    case 'Object':
      return objectValue(state, node);

    case 'Array':
      return valueChildren(node).map(child => nodeValue(state, child));

    case 'True':
      return true;

    case 'False':
      return false;

    case 'Null':
      return null;

    case 'Number':
      return Number(text);

    case 'String':
      return stringValue(text);

    case 'MultilineString': {
      // Hjson removes the indentation of the column that the string starts at
      const column = node.from - state.doc.lineAt(node.from).from;
      try {
        return Hjson.parse(`v:\n${' '.repeat(column)}${text}`).v;
      } catch {
        return text.replace(/^'''/, ''); // Not closed yet
      }
    }

    default:
      return text.trim(); // QuotelessString
  }
}

/**
 * The properties (that have a key and a value) of an object, or of the document without the root braces
 */
function objectValue(
  state: EditorState,
  node: SyntaxNode,
  skipProperty?: SyntaxNode,
): Record<string, unknown> {
  const value: Record<string, unknown> = {};
  for (const property of node.getChildren('Property')) {
    if (isSameNode(skipProperty, property)) continue;
    const key = propertyKey(state, property);
    const propertyValueNode = propertyValue(property);
    if (key === undefined || !propertyValueNode) continue;
    value[key] = nodeValue(state, propertyValueNode);
  }
  return value;
}

function isClosed(node: SyntaxNode): boolean {
  const last = node.lastChild?.name;
  return node.name === 'Object' ? last === '}' : last === ']';
}

function pathTo(state: EditorState, container: SyntaxNode): string[] {
  const path: string[] = [];
  for (let node = container; node.parent; node = node.parent) {
    const { parent } = node;
    if (parent.name === 'Property') {
      path.unshift(propertyKey(state, parent) ?? '');
    } else if (parent.name === 'Array') {
      path.unshift(String(valueChildren(parent).findIndex(child => isSameNode(child, node))));
    }
  }
  return path;
}

/**
 * Finds where the cursor is in an Hjson document. Needs the hjson language for the syntax tree.
 */
export function getHjsonContext(state: EditorState, pos: number): HjsonContext {
  // Go up from the cursor to the object or array that it is in (or the document, at the root without braces). A closed
  // object or array that ends at the cursor is a value that was just typed.
  let cursorProperty: SyntaxNode | undefined;
  let container: SyntaxNode = completeSyntaxTree(state).resolveInner(pos, -1);
  while (
    container.parent &&
    !(
      (container.name === 'Object' || container.name === 'Array') &&
      (pos < container.to || !isClosed(container))
    )
  ) {
    if (container.name === 'Property') cursorProperty ??= container;
    container = container.parent;
  }

  const path = pathTo(state, container);

  if (container.name === 'Array') {
    const arrayProperty = container.parent?.name === 'Property' ? container.parent : undefined;
    const outerObject = arrayProperty?.parent;
    return {
      path,
      isEditingKey: false,
      currentKey: String(valueChildren(container).filter(child => child.to < pos).length),
      currentObject: outerObject ? objectValue(state, outerObject, arrayProperty) : {},
    };
  }

  // The property that the cursor is in, or else the one before the cursor (the cursor can be after its colon)
  const property =
    cursorProperty ??
    container
      .getChildren('Property')
      .filter(child => child.to <= pos)
      .at(-1);
  const colon = property?.getChild(':');
  if (property && colon && colon.to <= pos) {
    const value = valueChildren(property)[0];
    // A value that starts after the cursor is on a line below (it is what is typed next)
    if (!value || pos <= value.to) {
      return {
        path,
        isEditingKey: false,
        currentKey: propertyKey(state, property),
        currentObject: objectValue(state, container, property),
      };
    }
  }

  return {
    path,
    isEditingKey: true,
    currentKey: undefined,
    currentObject: objectValue(state, container, cursorProperty),
  };
}
