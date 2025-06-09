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
  // Default context for empty input
  if (!hjson.trim()) {
    return {
      path: [],
      isEditingKey: true,
      currentKey: undefined,
      isEditingComment: false,
      currentObject: {},
    };
  }

  // Track our position in the JSON structure
  const path: string[] = [];
  let isEditingKey;
  let currentKey: string | undefined;
  let inString = false;
  let stringChar: string | null = null;
  let escapeNext = false;
  let currentToken = '';
  let expectingValue = false;
  let afterColon = false;

  // Track the current object being edited
  const objectStack: any[] = [{}]; // Stack of objects/arrays, starting with root
  let currentObjectProperties: Record<string, any> = {}; // Properties of the current object

  // Comment tracking
  let inSingleLineComment = false;
  let inMultiLineComment = false;

  // Context state before entering comment (preserved during comment parsing)
  let preCommentIsEditingKey: boolean | undefined;
  let preCommentCurrentKey: string | undefined;

  // Stack to track whether we're in an object or array
  const containerStack: { type: 'object' | 'array'; elementCount: number }[] = [];

  for (let i = 0; i < hjson.length; i++) {
    const char = hjson[i];
    const nextChar = i < hjson.length - 1 ? hjson[i + 1] : null;

    // Handle escape sequences
    if (escapeNext) {
      escapeNext = false;
      if (!inSingleLineComment && !inMultiLineComment) {
        currentToken += char;
      }
      continue;
    }

    if (char === '\\' && inString && !inSingleLineComment && !inMultiLineComment) {
      escapeNext = true;
      currentToken += char;
      continue;
    }

    // Handle comment detection and parsing
    if (inSingleLineComment) {
      // Exit single-line comment on newline
      if (char === '\n') {
        inSingleLineComment = false;
      }
      continue;
    }

    if (inMultiLineComment) {
      // Exit multi-line comment on */
      if (char === '*' && nextChar === '/') {
        inMultiLineComment = false;
        i++; // Skip the '/' as well
      }
      continue;
    }

    // Check for comment start (only when not in string)
    if (!inString) {
      // Single-line comment
      if (char === '/' && nextChar === '/') {
        // Save current context before entering comment
        preCommentIsEditingKey = determineIsEditingKey(containerStack, afterColon);
        preCommentCurrentKey = currentKey;
        inSingleLineComment = true;
        i++; // Skip the second '/'
        continue;
      }
      // Multi-line comment
      if (char === '/' && nextChar === '*') {
        // Save current context before entering comment
        preCommentIsEditingKey = determineIsEditingKey(containerStack, afterColon);
        preCommentCurrentKey = currentKey;
        inMultiLineComment = true;
        i++; // Skip the '*'
        continue;
      }
    }

    // Handle strings
    if (inString) {
      currentToken += char;
      if (char === stringChar) {
        inString = false;
        stringChar = null;

        // If we just completed a string and we're in an array expecting a value, add it immediately
        if (
          expectingValue &&
          containerStack.length > 0 &&
          containerStack[containerStack.length - 1].type === 'array'
        ) {
          const value = parseValue(currentToken.trim());
          const currentArray = objectStack[objectStack.length - 1] as any[];
          currentArray.push(value);
          currentToken = '';
        }
      }
      continue;
    }

    // Start of string
    if ((char === '"' || char === "'") && !inString) {
      inString = true;
      stringChar = char;
      currentToken += char;
      continue;
    }

    // Handle structural characters
    switch (char) {
      case '{': {
        if (currentToken.trim()) {
          // If we have a token before {, it's a key
          const key = extractKeyFromToken(currentToken);
          if (key) {
            path.push(key);
          }
        } else if (afterColon && currentKey) {
          // We're entering an object that is a value for a key
          path.push(currentKey);
        } else if (
          containerStack.length > 0 &&
          containerStack[containerStack.length - 1].type === 'array'
        ) {
          // We're in an array, add the index to path
          path.push(String(containerStack[containerStack.length - 1].elementCount));
        }
        containerStack.push({ type: 'object', elementCount: 0 });

        // Push new object context
        const newObject = {};
        objectStack.push(newObject);
        currentObjectProperties = newObject;

        expectingValue = false;
        afterColon = false;
        currentToken = '';
        currentKey = undefined;
        break;
      }

      case '[': {
        if (currentToken.trim()) {
          // If we have a token before [, it's a key
          const key = extractKeyFromToken(currentToken);
          if (key) {
            path.push(key);
          }
        } else if (afterColon && currentKey) {
          // We're entering an array that is a value for a key
          path.push(currentKey);
        }
        containerStack.push({ type: 'array', elementCount: 0 });

        // Push new array context
        const newArray: any[] = [];
        objectStack.push(newArray);
        currentObjectProperties = objectStack[objectStack.length - 2]; // Parent object

        expectingValue = true;
        afterColon = false;
        currentToken = '';
        currentKey = undefined;
        break;
      }

      case '}':
      case ']': {
        // Complete current token if any
        if (currentToken.trim() && expectingValue && currentKey) {
          // We were in the middle of a value - save it
          const value = parseValue(currentToken.trim());
          if (
            containerStack.length > 0 &&
            containerStack[containerStack.length - 1].type === 'array'
          ) {
            // Add to array
            const currentArray = objectStack[objectStack.length - 1] as any[];
            currentArray.push(value);
          } else if (afterColon) {
            // Add to object
            currentObjectProperties[currentKey] = value;
          }
        }

        // Get the completed object/array before popping
        const completedObject =
          char === '}' ? { ...currentObjectProperties } : objectStack[objectStack.length - 1];

        const container = containerStack.pop();
        let poppedKey: string | undefined;
        if (container) {
          if (path.length > 0) {
            poppedKey = path.pop();
          }
        }

        // Pop object context
        objectStack.pop();

        // Store the completed object in its parent if we have a key for it
        if (poppedKey && objectStack.length > 0) {
          const parentObject = objectStack[objectStack.length - 1];
          if (typeof parentObject === 'object' && !Array.isArray(parentObject)) {
            parentObject[poppedKey] = completedObject;
          }
        }

        // Update current object properties to the parent's properties
        const parentContext = objectStack[objectStack.length - 1];
        if (parentContext && typeof parentContext === 'object' && !Array.isArray(parentContext)) {
          currentObjectProperties = parentContext;
        } else {
          currentObjectProperties = objectStack[objectStack.length - 2] || {};
        }

        expectingValue =
          containerStack.length > 0 && containerStack[containerStack.length - 1]?.type === 'array';
        afterColon = false;
        currentToken = '';
        break;
      }

      case ':':
        if (!inString) {
          // Extract key from current token
          const key = extractKeyFromToken(currentToken);
          if (key) {
            currentKey = key;
            afterColon = true;
            expectingValue = true;
          }
          currentToken = '';
        } else {
          currentToken += char;
        }
        break;

      case ',':
        if (!inString) {
          // Complete current element
          if (currentToken.trim() && afterColon && currentKey) {
            // We just completed a key-value pair
            const value = parseValue(currentToken.trim());
            if (
              containerStack.length > 0 &&
              containerStack[containerStack.length - 1].type === 'array'
            ) {
              // Add to array
              const currentArray = objectStack[objectStack.length - 1] as any[];
              currentArray.push(value);
            } else {
              // Add to object
              currentObjectProperties[currentKey] = value;
            }
          }

          if (containerStack.length > 0) {
            const container = containerStack[containerStack.length - 1];
            if (container.type === 'array') {
              container.elementCount++;
              expectingValue = true;
              currentKey = String(container.elementCount);
            } else {
              expectingValue = false;
            }
          }
          afterColon = false;
          currentToken = '';
          if (
            containerStack.length === 0 ||
            containerStack[containerStack.length - 1].type !== 'array'
          ) {
            currentKey = undefined;
          }
        } else {
          currentToken += char;
        }
        break;

      default:
        // Skip whitespace unless in string
        if (!inString && /\s/.test(char)) {
          if (currentToken.trim() || afterColon) {
            // Keep building token
          }
        } else {
          currentToken += char;
        }
    }
  }

  // Handle Hjson's optional trailing commas - if we have a token that contains both
  // a completed value and the start of a new key, we need to split them
  if (afterColon && currentKey && currentToken.trim()) {
    const trimmedToken = currentToken.trim();
    // Look for patterns like:
    // - "value newkey" (with whitespace)
    // - "value"newkey (quoted value followed immediately by new key)
    // - value newkey (unquoted value with whitespace)
    const matchWithSpace = /^(".*?"|'.*?'|\S+)\s+([a-zA-Z_"'].*)$/.exec(trimmedToken);
    const matchNoSpace = /^(".*?"|'.*?')([a-zA-Z_].*)$/.exec(trimmedToken);
    
    const match = matchWithSpace || matchNoSpace;
    if (match) {
      const [, valuePart, keyPart] = match;
      
      // Complete the current key-value pair
      const value = parseValue(valuePart);
      currentObjectProperties[currentKey] = value;
      
      // Set up for the new key
      afterColon = false;
      currentToken = keyPart;
      currentKey = undefined;
    }
  }

  // Determine final context
  if (containerStack.length === 0) {
    isEditingKey = true;
    currentKey = undefined;
  } else {
    const currentContainer = containerStack[containerStack.length - 1];
    if (currentContainer.type === 'array') {
      isEditingKey = false;
      currentKey = String(currentContainer.elementCount);

      // Add array index to path if we're directly in the array
      if (expectingValue && !afterColon) {
        // Don't add to path yet, it will be added when the value is entered
      }
    } else {
      // In object
      if (afterColon) {
        isEditingKey = false;
        // currentKey is already set from the colon handling
      } else {
        isEditingKey = true;
        // If we have a current token that looks like a partial key AND
        // we have some existing properties (suggesting this is a trailing comma case), use it
        if (currentToken.trim() && Object.keys(currentObjectProperties).length > 0) {
          const extractedKey = extractKeyFromToken(currentToken);
          currentKey = extractedKey || undefined;
        } else {
          currentKey = undefined;
        }
      }
    }
  }

  // If we're in a comment, use the preserved context from before the comment
  const finalIsEditingComment = inSingleLineComment || inMultiLineComment;
  const finalIsEditingKey = finalIsEditingComment
    ? preCommentIsEditingKey ?? isEditingKey
    : isEditingKey;
  const finalCurrentKey = finalIsEditingComment ? preCommentCurrentKey : currentKey;

  // Calculate the final current object
  let finalCurrentObject = currentObjectProperties;

  // If we're in a comment, preserve the object state from before the comment
  if (finalIsEditingComment) {
    // Use the current object properties as they were before the comment
    finalCurrentObject = currentObjectProperties;
  }

  return {
    path,
    isEditingKey: finalIsEditingKey,
    currentKey: finalCurrentKey,
    isEditingComment: finalIsEditingComment,
    currentObject: finalCurrentObject,
  };
}

function determineIsEditingKey(
  containerStack: { type: 'object' | 'array'; elementCount: number }[],
  afterColon: boolean,
): boolean {
  if (containerStack.length === 0) {
    return true;
  }

  const currentContainer = containerStack[containerStack.length - 1];
  if (currentContainer.type === 'array') {
    return false; // In arrays we're always editing values (even if they're object indices)
  } else {
    // In object
    return !afterColon; // If after colon, we're editing value; otherwise editing key
  }
}

function extractKeyFromToken(token: string): string {
  const trimmed = token.trim();

  // Remove trailing colon if present
  const withoutColon = trimmed.replace(/:$/, '').trim();

  // Handle quoted keys
  if (
    (withoutColon.startsWith('"') && withoutColon.endsWith('"')) ||
    (withoutColon.startsWith("'") && withoutColon.endsWith("'"))
  ) {
    return withoutColon.slice(1, -1);
  }

  // Handle unquoted keys (Hjson feature)
  // Take everything up to whitespace or special chars
  const regex = /^([a-zA-Z_$][a-zA-Z0-9_$]*)/;
  const match = regex.exec(withoutColon);
  if (match) {
    return match[1];
  }

  return withoutColon;
}

function parseValue(token: string): any {
  const trimmed = token.trim();

  // Handle quoted strings
  if (
    (trimmed.startsWith('"') && trimmed.endsWith('"')) ||
    (trimmed.startsWith("'") && trimmed.endsWith("'"))
  ) {
    return trimmed.slice(1, -1);
  }

  // Handle boolean values
  if (trimmed === 'true') return true;
  if (trimmed === 'false') return false;
  if (trimmed === 'null') return null;

  // Handle numbers
  const numMatch = /^-?\d+(\.\d+)?$/.exec(trimmed);
  if (numMatch) {
    return trimmed.includes('.') ? parseFloat(trimmed) : parseInt(trimmed, 10);
  }

  // Return as unquoted string (Hjson feature)
  return trimmed;
}
