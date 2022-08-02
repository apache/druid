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

import org.apache.druid.java.util.common.IAE;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class NestedPathFinder
{
  public static String toNormalizedJsonPath(List<NestedPathPart> paths)
  {
    if (paths.isEmpty()) {
      return "$";
    }
    StringBuilder bob = new StringBuilder();
    boolean first = true;
    for (NestedPathPart partFinder : paths) {
      if (partFinder instanceof NestedPathField) {
        if (first) {
          bob.append("$");
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
          bob.append("$");
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

    if (!path.startsWith("$")) {
      badFormatJsonPath(path, "must start with '$'");
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
          badFormatJsonPath(path, "invalid position " + i + " for '[', must not follow '.' or must be contained with '");
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
          badFormatJsonPath(path, "expected number for array specifier got " + maybeNumber + " instead. Use ' if this value was meant to be a field name");
        }
      } else if (dotMark == -1 && arrayMark == -1) {
        badFormatJsonPath(path, "path parts must be separated with '.'");
      } else if (current == '\'' && quoteMark < 0) {
        if (arrayMark != i - 1) {
          badFormatJsonPath(path, "' must be immediately after '['");
        }
        quoteMark = i;
        partMark = i + 1;
      } else if (current == '\'' && quoteMark >= 0 && path.charAt(i - 1) != '\\') {
        if (path.charAt(i + 1) != ']') {
          if (arrayMark >= 0) {
            continue;
          }
          badFormatJsonPath(path, "closing ' must immediately precede ']'");
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
        badFormatJsonPath(path, "unterminated '");
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
      return StructuredDataProcessor.ROOT_LITERAL;
    }
    StringBuilder bob = new StringBuilder();
    boolean first = true;
    for (NestedPathPart partFinder : paths) {
      if (partFinder instanceof NestedPathField) {
        bob.append(".");
        bob.append("\"").append(partFinder.getPartIdentifier()).append("\"");
      } else {
        if (first) {
          bob.append(".");
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
      badFormat(path, "must start with '.'");
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
            badFormat(path, "invalid position " + i + " for '?'");
          }
        }
        partMark = i + 1;
      } else if (current == '[' && arrayMark < 0 && quoteMark < 0) {
        if (dotMark == (i - 1) && dotMark != 0) {
          badFormat(path, "invalid position " + i + " for '[', must not follow '.' or must be contained with '\"'");
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
          badFormat(path, "expected number for array specifier got " + maybeNumber + " instead. Use \"\" if this value was meant to be a field name");
        }
      } else if (dotMark == -1 && arrayMark == -1) {
        badFormat(path, "path parts must be separated with '.'");
      } else if (current == '"' && quoteMark < 0) {
        if (partMark != i) {
          badFormat(path, "invalid position " + i + " for '\"', must immediately follow '.' or '['");
        }
        if (arrayMark > 0 && arrayMark != i - 1) {
          badFormat(path, "'\"' within '[' must be immediately after");
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

  private static void badFormat(String path, String message)
  {
    throw new IAE("Bad format, '%s' is not a valid 'jq' path: %s", path, message);
  }

  private static void badFormatJsonPath(String path, String message)
  {
    throw new IAE("Bad format, '%s' is not a valid JSONPath path: %s", path, message);
  }

  /**
   * Dig through a thing to find stuff, if that stuff is a not nested itself
   */
  @Nullable
  public static String findStringLiteral(@Nullable Object data, List<NestedPathPart> path)
  {
    Object currentObject = find(data, path);
    if (currentObject instanceof Map || currentObject instanceof List || currentObject instanceof Object[]) {
      return null;
    } else {
      // a literal of some sort, huzzah!
      if (currentObject == null) {
        return null;
      }
      return String.valueOf(currentObject);
    }
  }

  @Nullable
  public static Object findLiteral(@Nullable Object data, List<NestedPathPart> path)
  {
    Object currentObject = find(data, path);
    if (currentObject instanceof Map || currentObject instanceof List || currentObject instanceof Object[]) {
      return null;
    } else {
      // a literal of some sort, huzzah!
      if (currentObject == null) {
        return null;
      }
      return currentObject;
    }
  }

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

}
