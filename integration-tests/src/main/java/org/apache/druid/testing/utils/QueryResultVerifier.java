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

package org.apache.druid.testing.utils;

import java.util.Iterator;
import java.util.Map;

public class QueryResultVerifier
{
  public static boolean compareResults(
      Iterable<Map<String, Object>> actual,
      Iterable<Map<String, Object>> expected
  )
  {
    Iterator<Map<String, Object>> actualIter = actual.iterator();
    Iterator<Map<String, Object>> expectedIter = expected.iterator();

    while (actualIter.hasNext() && expectedIter.hasNext()) {
      Map<String, Object> actualRes = actualIter.next();
      Map<String, Object> expRes = expectedIter.next();

      if (!actualRes.equals(expRes)) {
        return false;
      }
    }

    if (actualIter.hasNext() || expectedIter.hasNext()) {
      return false;
    }
    return true;
  }
}
