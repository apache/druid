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

import javax.annotation.Nullable;
import java.util.List;

public class ImmutableDruidDataSourceTestUtils
{

  /**
   * This method is to check equality of {@link ImmutableDruidDataSource} objects to be called from test code.
   * @param expected expected object
   * @param actual actual object
   */
  public static void assertEquals(ImmutableDruidDataSource expected, ImmutableDruidDataSource actual)
  {
    if (checkEquals(expected, actual)) {
      return;
    } else {
      throw new AssertionError("Expected and actual objects are not equal as per ImmutableDruidDataSource's " +
        "equalsForTesting() method");
    }
  }

  private static boolean checkEquals(
      @Nullable ImmutableDruidDataSource expected,
      @Nullable ImmutableDruidDataSource actual
  )
  {
    if (expected == null) {
      return actual == null;
    }

    return expected.equalsForTesting(actual);
  }

  /**
   * This method is to check the equality of a list of {@link ImmutableDruidDataSource} objects to be called from
   * test code
   * @param expected expected list
   * @param actual actual list
   * @return
   */
  public static boolean assertEquals(List<ImmutableDruidDataSource> expected, List<ImmutableDruidDataSource> actual)
  {
    if (expected == null) {
      return actual == null;
    }

    Assert.assertEquals("expected and actual ImmutableDruidDataSource lists should be of equal size",
        expected.size(), actual.size());

    for (ImmutableDruidDataSource e : expected) {
      if (!contains(e, actual)) {
        throw new AssertionError("Expected and actual objects are not equal as per " +
          "ImmutableDruidDataSource's equalsForTesting()" + " method");
      }
    }
    return true;
  }

  private static boolean contains(ImmutableDruidDataSource expected, List<ImmutableDruidDataSource> actualList)
  {
    // Iterate over actual list to see if the element expected is present, if not return false
    for (ImmutableDruidDataSource ds : actualList) {
      if (ds.equalsForTesting(expected)) {
        return true;
      }
    }
    return false;
  }

}
