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

export interface HjsonContext {
  /**
   * The path of keys leading to the current position, e.g., ["query", "dataSource"]
   * For arrays, includes the index as a string key, e.g., ["filters", "0", "dimension"]
   * Empty array if at root level
   */
  path: string[];

  /**
   * Whether the cursor is positioned where a key should be entered (true)
   * or where a value should be entered (false)
   */
  isEditingKey: boolean;

  /**
   * If editing a value (isEditingKey === false), this is the key for that value
   * If editing a key (isEditingKey === true), this is undefined
   */
  currentKey?: string;

  /**
   * Whether the cursor is positioned inside a comment (single-line or multi-line)
   */
  isEditingComment: boolean;

  /**
   * The current JSON object being edited at the cursor position.
   * This is the object that contains the property/value being typed.
   * For completions, this provides context about what properties already exist.
   */
  currentObject: any;
}

/**
 * Analyzes an Hjson string (from start to cursor position) and returns
 * context information about where the cursor is positioned within the JSON structure
 *
 * @param hjson - The Hjson text from the beginning of the document to the cursor position
 * @returns Context information about the cursor position
 */
export function getHjsonContext(hjson: string): HjsonContext {
  // Empty input
  if (!hjson.trim()) {
    return {
      path: [],
      isEditingKey: true,
      currentKey: undefined,
      isEditingComment: false,
      currentObject: {},
    };
  }

  // State machine state
  const path: string[] = [];
  const containerStack: { type: 'object' | 'array'; index: number }[] = [];
  const objectStack: any[] = [{}];

  let state: 'normal' | 'quoted-string' | 'single-line-comment' | 'multi-line-comment' = 'normal';
  let stringDelim = '';
  let token = '';
  let currentKey: string | undefined;
  let afterColon = false;

  // Process each character
  for (let i = 0; i < hjson.length; i++) {
    const ch = hjson[i];
    const next = hjson[i + 1];

    // State transitions
    if (state === 'quoted-string') {
      token += ch;
      if (ch === stringDelim && hjson[i - 1] !== '\\') {
        state = 'normal';
        // If in array expecting value, push completed string
        if (afterColon && currentKey && containerStack.length > 0) {
          const container = containerStack[containerStack.length - 1];
          if (container.type === 'array') {
            objectStack[objectStack.length - 1].push(parseValue(token));
            token = '';
            afterColon = false;
          }
        }
      }
      continue;
    }

    if (state === 'single-line-comment') {
      if (ch === '\n') state = 'normal';
      continue;
    }

    if (state === 'multi-line-comment') {
      if (ch === '*' && next === '/') {
        state = 'normal';
        i++; // Skip '/'
      }
      continue;
    }

    // Normal state processing

    // Check for comment start
    if (ch === '#') {
      state = 'single-line-comment';
      continue;
    }

    if (ch === '/' && next === '/') {
      state = 'single-line-comment';
      i++; // Skip second '/'
      continue;
    }

    if (ch === '/' && next === '*') {
      state = 'multi-line-comment';
      i++; // Skip '*'
      continue;
    }

    // String start
    if (ch === '"' || ch === "'") {
      state = 'quoted-string';
      stringDelim = ch;
      token += ch;
      continue;
    }

    // Structural characters
    switch (ch) {
      case '{': {
        // Handle Hjson no-comma case
        if (afterColon && currentKey && token.trim()) {
          getCurrentObject()[currentKey] = parseValue(token.trim());
          path.push(currentKey);
        } else if (token.trim()) {
          path.push(extractKey(token));
        } else if (afterColon && currentKey) {
          path.push(currentKey);
        } else if (
          containerStack.length > 0 &&
          containerStack[containerStack.length - 1].type === 'array'
        ) {
          path.push(String(containerStack[containerStack.length - 1].index));
        }

        containerStack.push({ type: 'object', index: 0 });
        objectStack.push({});
        token = '';
        currentKey = undefined;
        afterColon = false;
        break;
      }

      case '[': {
        if (token.trim()) {
          path.push(extractKey(token));
        } else if (afterColon && currentKey) {
          path.push(currentKey);
        }

        containerStack.push({ type: 'array', index: 0 });
        objectStack.push([]);
        token = '';
        currentKey = undefined;
        afterColon = false;
        break;
      }

      case '}':
      case ']': {
        // Complete pending value
        if (token.trim()) {
          if (
            containerStack.length > 0 &&
            containerStack[containerStack.length - 1].type === 'array'
          ) {
            // We're in an array, add the item
            const arr = objectStack[objectStack.length - 1] as any[];
            arr.push(parseValue(token.trim()));
          } else if (afterColon && currentKey) {
            // We're completing an object property
            getCurrentObject()[currentKey] = parseValue(token.trim());
          }
        }

        // Pop container
        const completed = objectStack.pop();
        containerStack.pop();
        if (path.length > 0) {
          const key = path.pop()!;
          if (objectStack.length > 0 && completed !== undefined) {
            const parent = objectStack[objectStack.length - 1];
            if (!Array.isArray(parent)) {
              parent[key] = completed;
            }
          }
        }

        token = '';
        currentKey = undefined;
        afterColon = false;
        break;
      }

      case ':': {
        currentKey = extractKey(token);
        afterColon = true;
        token = '';
        break;
      }

      case ',': {
        // Complete value
        if (token.trim()) {
          if (
            containerStack.length > 0 &&
            containerStack[containerStack.length - 1].type === 'array'
          ) {
            // We're in an array, add the item
            const arr = objectStack[objectStack.length - 1] as any[];
            arr.push(parseValue(token.trim()));
          } else if (afterColon && currentKey) {
            // We're completing an object property
            getCurrentObject()[currentKey] = parseValue(token.trim());
          }
        }

        // Update array index
        if (containerStack.length > 0) {
          const container = containerStack[containerStack.length - 1];
          if (container.type === 'array') {
            container.index++;
          }
        }

        token = '';
        currentKey = undefined;
        afterColon = false;
        break;
      }

      default: {
        if (/\s/.test(ch)) {
          // Newline can complete a value in Hjson
          if (ch === '\n' && afterColon && currentKey && token.trim()) {
            getCurrentObject()[currentKey] = parseValue(token.trim());
            token = '';
            currentKey = undefined;
            afterColon = false;
          }
        } else {
          token += ch;
        }
      }
    }
  }

  // Determine context
  let isEditingKey: boolean;
  let finalKey: string | undefined;

  if (containerStack.length === 0) {
    // Root level - partial keys should not be considered as currentKey
    isEditingKey = true;
    finalKey = undefined;
  } else {
    const container = containerStack[containerStack.length - 1];
    if (container.type === 'array') {
      isEditingKey = false;
      finalKey = String(container.index);
    } else {
      isEditingKey = !afterColon;
      if (afterColon) {
        finalKey = currentKey;
      } else if (
        token.trim() &&
        ((getCurrentObject() && Object.keys(getCurrentObject()).length > 0) ||
          containerStack.length > 1)
      ) {
        // Set currentKey for partial keys in non-empty objects (trailing comma case) or nested contexts
        finalKey = extractKey(token);
      }
    }
  }

  return {
    path,
    isEditingKey,
    currentKey: finalKey,
    isEditingComment: state === 'single-line-comment' || state === 'multi-line-comment',
    currentObject: getCurrentObject(),
  };

  function getCurrentObject(): any {
    if (objectStack.length === 0) return {};
    const current = objectStack[objectStack.length - 1];
    return Array.isArray(current) ? objectStack[objectStack.length - 2] || {} : current;
  }
}

function extractKey(token: string): string {
  if (
    (token.startsWith('"') && token.endsWith('"')) ||
    (token.startsWith("'") && token.endsWith("'"))
  ) {
    return token.slice(1, -1);
  }

  return token;
}

function parseValue(token: string): any {
  const trimmed = token.trim();

  if (
    (trimmed.startsWith('"') && trimmed.endsWith('"')) ||
    (trimmed.startsWith("'") && trimmed.endsWith("'"))
  ) {
    return trimmed.slice(1, -1);
  }

  if (trimmed === 'true') return true;
  if (trimmed === 'false') return false;
  if (trimmed === 'null') return null;

  if (/^-?\d+(?:\.\d+)?$/.test(trimmed)) {
    return trimmed.includes('.') ? parseFloat(trimmed) : parseInt(trimmed, 10);
  }

  return trimmed;
}
