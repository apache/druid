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

package org.apache.druid.indexer.path;

import com.google.common.base.Functions;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.List;

//Note: This class has been created to workaround https://issues.apache.org/jira/browse/MAPREDUCE-5061
public class HadoopGlobPathSplitter
{

  /**
   * Splits given hadoop glob path by commas.
   * e.g. splitGlob("/a,/b") -> ["/a","/b"]
   * splitGlob("/a/{c,d}") -> ["/a/c", "/a/d"]
   */
  public static Iterable<String> splitGlob(String path)
  {
    return Iterables.transform(splitGlob(new CharStream(path)), Functions.toStringFunction());
  }

  private static List<StringBuilder> splitGlob(CharStream path)
  {
    List<StringBuilder> result = new ArrayList<>();

    List<StringBuilder> current = new ArrayList<>();
    current.add(new StringBuilder());

    while (path.hasMore()) {
      char c = path.next();
      switch (c) {
        case '{':
          List<StringBuilder> childResult = splitGlob(path);
          List<StringBuilder> oldCurrent = current;
          current = new ArrayList<>();

          for (StringBuilder sb1 : oldCurrent) {
            for (StringBuilder sb2 : childResult) {
              StringBuilder sb3 = new StringBuilder();
              sb3.append(sb1);
              sb3.append(sb2);
              current.add(sb3);
            }
          }
          break;
        case '}':
          result.addAll(current);
          return result;
        case ',':
          result.addAll(current);
          current = new ArrayList<>();
          current.add(new StringBuilder());
          break;
        default:
          for (StringBuilder sb : current) {
            sb.append(c);
          }
      }
    }

    result.addAll(current);
    return result;
  }
}

class CharStream
{
  private String string;
  private int offset;

  public CharStream(String string)
  {
    super();
    this.string = string;
    this.offset = 0;
  }

  public boolean hasMore()
  {
    return offset < string.length();
  }

  public char next()
  {
    return string.charAt(offset++);
  }
}
