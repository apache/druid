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

package org.apache.druid.jdbc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


public class StringUtilsTest
{
  /**
   * Same semantics as String.replace: left to right, whole matches, replacement text never rescanned.
   */
  @Test
  public void testReplace()
  {
    Assertions.assertEquals("a'b", StringUtils.replace("a''b", "''", "'"));
    Assertions.assertEquals("a''b", StringUtils.replace("a'b", "'", "''"));
    Assertions.assertEquals("%20x%20", StringUtils.replace("+x+", "+", "%20"));
    Assertions.assertEquals("abc", StringUtils.replace("abc", "x", "y"));
    Assertions.assertEquals("", StringUtils.replace("", "x", "y"));
    Assertions.assertEquals("--", StringUtils.replace("aaaa", "aa", "-"));
    Assertions.assertEquals("xxx", StringUtils.replace("aaa", "a", "x"));
    Assertions.assertEquals("", StringUtils.replace("aa", "aa", ""));
    Assertions.assertEquals("''", StringUtils.replace("'", "'", "''"));
  }

  /**
   * An empty target inserts the replacement at every position, as String.replace does.
   */
  @Test
  public void testReplaceEmptyTarget()
  {
    Assertions.assertEquals("xaxbxcx", StringUtils.replace("abc", "", "x"));
  }
}
