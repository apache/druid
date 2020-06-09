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

package org.apache.druid.query;

import org.apache.druid.java.util.common.guava.Sequence;
import org.junit.Assert;

import java.util.List;

public class QueryToolChestTestHelper
{
  public static void assertArrayResultsEquals(final List<Object[]> expected, final Sequence<Object[]> actual)
  {
    final List<Object[]> actualList = actual.toList();
    Assert.assertEquals("number of results", expected.size(), actualList.size());
    for (int i = 0; i < actualList.size(); i++) {
      Assert.assertArrayEquals("result #" + i, expected.get(i), actualList.get(i));
    }
  }
}
