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

package org.apache.druid.segment.nested;

import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class NestedPathFinder
{
  public static final String JSON_PATH_ROOT = "$";
  public static final String JQ_PATH_ROOT = ".";

  /**
   * Dig through a thing to find stuff
   */
  @Nullable
  public static Object find(@Nullable Object data, List<NestedPathPart> path)
  {
    Object currentObject = data;
    for (NestedPathPart pathPart : path) {
      Object objectAtPath = pathPart.find(currentObject);
      if (objectAtPath == null) {
        return null;
      }
      currentObject = objectAtPath;
    }
    return currentObject;
  }

  /**
   * find the list of 'keys' at some path.
   * - if the thing at a path is an object (map), return the fields
   * - if the thing at a path is an array (list or array), return the 0 based element numbers
   * - if the thing is a simple value, return null
   */
  @Nullable
  public static Object[] findKeys(@Nullable Object data, List<NestedPathPart> path)
  {
    Object currentObject = find(data, path);
    if (currentObject instanceof Map) {
      return ((Map) currentObject).keySet().toArray();
    }
    if (currentObject instanceof List) {
      return IntStream.range(0, ((List) currentObject).size()).mapToObj(Integer::toString).toArray();
    }
    if (currentObject instanceof Object[]) {
      return IntStream.range(0, ((Object[]) currentObject).length).mapToObj(Integer::toString).toArray();
    }
    return null;
  }

  public static String toNormalizedJsonPath(List<NestedPathPart> paths)
  {
    if (paths.isEmpty()) {
      return JSON_PATH_ROOT;
    }
    StringBuilder bob = new StringBuilder();
    boolean first = true;
    for (NestedPathPart partFinder : paths) {
      if (partFinder instanceof NestedPathField) {
        if (first) {
          bob.append(JSON_PATH_ROOT);
        }
        final String id = partFinder.getPartIdentifier();
        if (id.contains(".") || id.contains("'") || id.contains("\"") || id.contains("[") || id.contains("]")) {
          bob.append("['").append(id).append("']");
        } else {
          bob.append(".");
          bob.append(id);
        }
      } else if (partFinder instanceof NestedPathArrayElement) {
        if (first) {
          bob.append(JSON_PATH_ROOT);
        }
        bob.append("[").append(partFinder.getPartIdentifier()).append("]");
      }
      first = false;
    }
    return bob.toString();
  }

  /**
   * split a JSONPath path into a series of extractors to find things in stuff
   */
  public static List<NestedPathPart> parseJsonPath(@Nullable String path)
  {
    if (path == null || path.isEmpty()) {
      return Collections.emptyList();
    }
    List<NestedPathPart> parts = new ArrayList<>();

    if (!path.startsWith(JSON_PATH_ROOT)) {
      badFormatJsonPath(path, "it must start with '$'");
    }

    if (path.length() == 1) {
      return Collections.emptyList();
    }

    int partMark = -1;  // position to start the next substring to build the path part
    int dotMark = -1;   // last position where a '.' character was encountered, indicating the start of a new part
    int arrayMark = -1; // position of leading '[' indicating start of array (or field name if ' immediately follows)
    int quoteMark = -1; // position of leading ', indicating a quoted field name

    // start at position 1 since $ is special
    for (int i = 1; i < path.length(); i++) {
      final char current = path.charAt(i);
      if (current == '.' && arrayMark < 0 && quoteMark < 0) {
        if (dotMark >= 0) {
          parts.add(new NestedPathField(getPathSubstring(path, partMark, i)));
        }
        dotMark = i;
        partMark = i + 1;
      } else if (current == '[' && arrayMark < 0 && quoteMark < 0) {
        if (dotMark == (i - 1) && dotMark != 0) {
          badFormatJsonPath(path, "found '[' at invalid position [%s], must not follow '.' or must be contained with '", i);
        }
        if (dotMark >= 0 && i > 1) {
          parts.add(new NestedPathField(getPathSubstring(path, partMark, i)));
          dotMark = -1;
        }
        arrayMark = i;
        partMark = i + 1;
      } else if (current == ']' && arrayMark >= 0 && quoteMark < 0) {
        String maybeNumber = getPathSubstring(path, partMark, i);
        try {
          int index = Integer.parseInt(maybeNumber);
          parts.add(new NestedPathArrayElement(index));
          dotMark = -1;
          arrayMark = -1;
          partMark = i + 1;
        }
        catch (NumberFormatException ignored) {
          badFormatJsonPath(path, "array specifier [%s] should be a number, it was not.  Use ' if this value was meant to be a field name", maybeNumber);
        }
      } else if (dotMark == -1 && arrayMark == -1) {
        badFormatJsonPath(path, "path parts must be separated with '.'");
      } else if (current == '\'' && quoteMark < 0) {
        if (arrayMark != i - 1) {
          badFormatJsonPath(path, "single-quote (') must be immediately after '['");
        }
        quoteMark = i;
        partMark = i + 1;
      } else if (current == '\'' && quoteMark >= 0 && path.charAt(i - 1) != '\\') {
        if (path.charAt(i + 1) != ']') {
          if (arrayMark >= 0) {
            continue;
          }
          badFormatJsonPath(path, "closing single-quote (') must immediately precede ']'");
        }

        parts.add(new NestedPathField(getPathSubstring(path, partMark, i)));
        dotMark = -1;
        quoteMark = -1;
        // chomp to next char to eat closing array
        if (++i == path.length()) {
          break;
        }
        partMark = i + 1;
        arrayMark = -1;
      }
    }
    // add the last element, this should never be an array because they close themselves
    if (partMark < path.length()) {
      if (quoteMark != -1) {
        badFormatJsonPath(path, "unterminated single-quote (')");
      }
      if (arrayMark != -1) {
        badFormatJsonPath(path, "unterminated '['");
      }
      parts.add(new NestedPathField(path.substring(partMark)));
    }

    return parts;
  }

  /**
   * Given a list of part finders, convert it to a "normalized" 'jq' path format that is consistent with how
   * {@link StructuredDataProcessor} constructs field path names
   */
  public static String toNormalizedJqPath(List<NestedPathPart> paths)
  {
    if (paths.isEmpty()) {
      return JQ_PATH_ROOT;
    }
    StringBuilder bob = new StringBuilder();
    boolean first = true;
    for (NestedPathPart partFinder : paths) {
      if (partFinder instanceof NestedPathField) {
        bob.append(".");
        bob.append("\"").append(partFinder.getPartIdentifier()).append("\"");
      } else {
        if (first) {
          bob.append(JQ_PATH_ROOT);
        }
        bob.append("[").append(partFinder.getPartIdentifier()).append("]");
      }
      first = false;
    }
    return bob.toString();
  }

  /**
   * split a jq path into a series of extractors to find things in stuff
   */
  public static List<NestedPathPart> parseJqPath(@Nullable String path)
  {
    if (path == null || path.isEmpty()) {
      return Collections.emptyList();
    }
    List<NestedPathPart> parts = new ArrayList<>();

    if (path.charAt(0) != '.') {
      badFormat(path, "it must start with '.'");
    }

    int partMark = -1;  // position to start the next substring to build the path part
    int dotMark = -1;   // last position where a '.' character was encountered, indicating the start of a new part
    int arrayMark = -1; // position of leading '[' indicating start of array (or field name if '"' immediately follows)
    int quoteMark = -1; // position of leading '"', indicating a quoted field name
    for (int i = 0; i < path.length(); i++) {
      final char current = path.charAt(i);
      if (current == '.' && arrayMark < 0 && quoteMark < 0) {
        if (dotMark >= 0) {
          parts.add(new NestedPathField(getPathSubstring(path, partMark, i)));
        }
        dotMark = i;
        partMark = i + 1;
      } else if (current == '?' && quoteMark < 0) {
        // eat optional marker
        if (partMark != i) {
          if (dotMark >= 0) {
            parts.add(new NestedPathField(getPathSubstring(path, partMark, i)));
            dotMark = -1;
          } else {
            badFormat(path, "found '?' at invalid position [%s]", i);
          }
        }
        partMark = i + 1;
      } else if (current == '[' && arrayMark < 0 && quoteMark < 0) {
        if (dotMark == (i - 1) && dotMark != 0) {
          badFormat(path, "found '[' at invalid position [%s], must not follow '.' or must be contained with '\"'", i);
        }
        if (dotMark >= 0 && i > 1) {
          parts.add(new NestedPathField(getPathSubstring(path, partMark, i)));
          dotMark = -1;
        }
        arrayMark = i;
        partMark = i + 1;
      } else if (current == ']' && arrayMark >= 0 && quoteMark < 0) {
        String maybeNumber = getPathSubstring(path, partMark, i);
        try {
          int index = Integer.parseInt(maybeNumber);
          parts.add(new NestedPathArrayElement(index));
          dotMark = -1;
          arrayMark = -1;
          partMark = i + 1;
        }
        catch (NumberFormatException ignored) {
          badFormat(path, "array specifier [%s] should be a number, it was not.  Use \"\" if this value was meant to be a field name", maybeNumber);
        }
      } else if (dotMark == -1 && arrayMark == -1) {
        badFormat(path, "path parts must be separated with '.'");
      } else if (current == '"' && quoteMark < 0) {
        if (partMark != i) {
          badFormat(path, "found '\"' at invalid position [%s], it must immediately follow '.' or '['", i);
        }
        if (arrayMark > 0 && arrayMark != i - 1) {
          badFormat(path, "'\"' within '[', must be immediately after");
        }
        quoteMark = i;
        partMark = i + 1;
      } else if (current == '"' && quoteMark >= 0 && path.charAt(i - 1) != '\\') {
        parts.add(new NestedPathField(getPathSubstring(path, partMark, i)));
        dotMark = -1;
        quoteMark = -1;
        if (arrayMark > 0) {
          // chomp to next char to eat closing array
          if (++i == path.length()) {
            break;
          }
          if (path.charAt(i) != ']') {
            badFormat(path, "closing '\"' must immediately precede ']'");
          }
          partMark = i + 1;
          arrayMark = -1;
        } else {
          partMark = i + 1;
        }
      }
    }
    // add the last element, this should never be an array because they close themselves
    if (partMark < path.length()) {
      if (quoteMark != -1) {
        badFormat(path, "unterminated '\"'");
      }
      if (arrayMark != -1) {
        badFormat(path, "unterminated '['");
      }
      parts.add(new NestedPathField(path.substring(partMark)));
    }

    return parts;
  }

  private static String getPathSubstring(String path, int start, int end)
  {
    if (end - start < 1) {
      badFormat(path, "path parts separated by '.' must not be empty");
    }
    return path.substring(start, end);
  }

  private static void badFormat(String path, String message, Object... args)
  {
    throw InvalidInput.exception("jq path [%s] is invalid, %s", path, StringUtils.format(message, args));
  }

  private static void badFormatJsonPath(String path, String message, Object... args)
  {
    throw InvalidInput.exception("JSONPath [%s] is invalid, %s", path, StringUtils.format(message, args));
  }
}
