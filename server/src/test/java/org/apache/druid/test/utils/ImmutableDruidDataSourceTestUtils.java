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

package org.apache.druid.test.utils;

import org.apache.druid.client.ImmutableDruidDataSource;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;

public class ImmutableDruidDataSourceTestUtils
{

  /**
   * This method is to check equality of {@link ImmutableDruidDataSource} objects to be called from test code.
   * @param expected expected object
   * @param actual actual object
   *
   */
  public static void assertEqualsImmutableDruidDataSource(Object expected, Object actual)
  {
    if (equalsRegardingNull(expected, actual)) {
      return;
    } else {
      throw new AssertionError("Expected and actual objects are not equal as per equals() method");
    }
  }

  private static boolean equalsRegardingNull(Object expected, Object actual)
  {
    if (expected == null) {
      return actual == null;
    }

    if (expected instanceof ArrayList<?> && actual instanceof ArrayList<?>) {
      List<ImmutableDruidDataSource> expected1 = (ArrayList<ImmutableDruidDataSource>) expected;
      List<ImmutableDruidDataSource> actual1 = (ArrayList<ImmutableDruidDataSource>) actual;
      Assert.assertEquals("expected and actual ImmutableDruidDataSource lists should be of equal size", expected1.size(), actual1.size());

      for (int i = 0; i < expected1.size(); i++) {
        if (!expected1.get(i).equalsForTesting(actual1.get(i))) {
          return false;
        }
      }
      return true;
    } else if (expected instanceof ImmutableDruidDataSource && actual instanceof ImmutableDruidDataSource) {
      return ((ImmutableDruidDataSource) expected).equalsForTesting(actual);
    }

    return false;
  }

}
