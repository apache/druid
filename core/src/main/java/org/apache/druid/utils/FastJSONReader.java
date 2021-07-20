/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.utils;

import javax.annotation.Nullable;

// For fast scalar value extraction, FastJSONReader doesn't support the full specification of types other than String.
public class FastJSONReader
{
  String json;
  String[] key;
  int pos;
  int keyDepth;
  int depth;
  int matchedDepth;

  public FastJSONReader(String _json)
  {
    json = _json;
  }

  private void reset()
  {
    pos = 0;
    keyDepth = key.length - 1; // -1 for array indexing
    depth = 0;
    matchedDepth = 0;
  }

  public String get(String[] _path)
  {
    key = _path;
    reset();
    return get(false);
  }

  // The depth is 1 at first call
  @Nullable
  private String get(boolean needResult)
  {
    if (json.isEmpty()) {
      return null;
    }

    skipWhitespaces();
    int begin_index = pos;

    if (eat('{') == false) {
      return null;
    }

    while (pos < json.length()) {
      skipWhitespaces();

      if (eat('"') == false) {
        return null;
      }

      boolean keyMatched = (depth < key.length) ? isMatched(key[depth]) : isMatched(null);
      boolean matched = (matchedDepth == key.length - 1) && keyMatched;

      skipWhitespaces();
      if (eat(':') == false) {
        return null;
      }
      skipWhitespaces();

      String result;
      char c = safeCharAt();
      if (c == '"') {
        result = stringValue(matched);
      } else if (c == '{') {
        depth++;
        if (keyMatched) {
          matchedDepth++;
        }
        result = get(matched);
        depth--;
        if (keyMatched) {
          matchedDepth--;
        }
      } else if (c == '[') {
        result = arrayValueAsString(matched);
      } else {
        result = othersAsString(matched);
      }

      if (result != null) {
        return result;
      }

      skipWhitespaces();

      if (!remains()) {
        return null;
      }

      c = json.charAt(pos);
      if (c == '}') {
        pos++;
        break;
      }

      if (!eat(',')) {
        return null;
      }
    }

    if (needResult) {
      return json.substring(begin_index, pos);
    } else {
      return null;
    }
  }

  private void skipWhitespaces()
  {
    for (; pos < json.length(); pos++) {
      if (Character.isWhitespace(json.charAt(pos)) == false) {
        break;
      }
    }
  }

  private char safeCharAt()
  {
    if (pos >= json.length()) {
      return '\u0000';
    } else {
      return json.charAt(pos);
    }
  }

  private boolean eat(char c)
  {
    if (pos >= json.length()) {
      return false;
    }

    // It doesn't validate boundary of the json for micro optimization
    return json.charAt(pos++) == c;
  }

  private int advance(char c)
  {
    int start = pos;
    for (; pos < json.length(); pos++) {
      if (json.charAt(pos) == c) {
        return pos - start;
      }
    }
    return -1;
  }

  @Nullable
  private boolean isMatched(String expected)
  {
    int begin_index = pos;
    int length = advance('"');

    if (expected == null) {
      pos++;
      return false;
    }

    if (expected.length() != length) {
      pos++;  // skip the closing quote
      return false;
    }

    String key = json.substring(begin_index, pos);
    pos++;  // skip the closing quote
    return key.equals(expected);
  }

  @Nullable
  private String stringValue(boolean needResult)
  {
    pos++; // skip the opening quote

    int begin_index = pos;

    for (; pos < json.length(); pos++) {
      char c = json.charAt(pos);
      if (c == '\\') {
        continue;
      } else if (c == '"') {
        if (needResult) {
          if (begin_index == pos) {
            pos++;
            return "";
          } else {
            return json.substring(begin_index, pos++); // skip the closing quote
          }
        } else {
          pos++;
          return null;
        }
      }
    }

    return null;
  }

  // It loosely parses other types. Because a caller can cast the result to an appropriate type.
  @Nullable
  private String othersAsString(boolean needResult)
  {
    int begin_index = pos;

    char c = safeCharAt();
    // disit, boolean, null
    if (('0' <= c && c <= '9') || c == '-' ||
        c == 't' || c == 'f' ||
        c == 'n') {
      for (; pos < json.length(); pos++) {
        if (json.charAt(pos) == ',' || json.charAt(pos) == '}' || json.charAt(pos) == ']') {
          break;
        }
      }

      if (begin_index != pos) {
        if (needResult) {
          return json.substring(begin_index, pos);
        }
      }
    }

    return null;
  }

  // It read whole values in an array as a string
  @Nullable
  private String arrayValueAsString(boolean needResult)
  {
    int begin_index = pos;
    pos++; // Skip '['

    while (pos < json.length()) {
      skipWhitespaces();

      char c = safeCharAt();
      if (c == '"') {
        stringValue(false);
      } else if (c == '{') {
        depth++;
        get(false);
        depth--;
      } else if (c == '[') {
        arrayValueAsString(false);
      } else {
        othersAsString(false);
      }

      skipWhitespaces();

      if (safeCharAt() == ']') {
        pos++; // Skip ']'
        if (needResult) {
          return json.substring(begin_index, pos);
        } else {
          return null;
        }
      }

      if (!eat(',')) {
        return null;
      }
    }

    return null;
  }

  private boolean remains()
  {
    return pos < json.length();
  }
}
