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

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

public class StructuredDataTest
{
  @Test
  public void testCompareTo()
  {
    StructuredData sd0 = new StructuredData(null);
    StructuredData sd0Dup = new StructuredData(null);
    StructuredData sd1 = new StructuredData("hello");
    StructuredData sd1Dup = new StructuredData("hello");
    StructuredData sd2 = new StructuredData("world");
    StructuredData sd3 = new StructuredData(1L);
    StructuredData sd3Dup = new StructuredData(1L);
    StructuredData sd4 = new StructuredData(2L);
    StructuredData sd5 = new StructuredData(1.1);
    StructuredData sd6 = new StructuredData(3.3);
    StructuredData sd6Dup = new StructuredData(3.3);
    StructuredData sd7 = new StructuredData(ImmutableMap.of("x", 1, "y", "hello"));
    StructuredData sd8 = new StructuredData(ImmutableMap.of("x", 1, "y", "hello"));
    StructuredData sd9 = new StructuredData(ImmutableMap.of("x", 12, "y", "world"));

    // equals
    Assert.assertEquals(0, sd0.compareTo(sd0Dup));
    Assert.assertEquals(sd0, sd0Dup);
    Assert.assertEquals(0, sd1.compareTo(sd1Dup));
    Assert.assertEquals(sd1, sd1Dup);
    Assert.assertEquals(0, sd3.compareTo(sd3Dup));
    Assert.assertEquals(sd3, sd3Dup);
    Assert.assertEquals(0, sd6.compareTo(sd6Dup));
    Assert.assertEquals(sd6, sd6Dup);
    Assert.assertEquals(0, sd7.compareTo(sd8));
    Assert.assertEquals(sd7, sd8);
    Assert.assertEquals(0, sd8.compareTo(sd7));
    Assert.assertEquals(sd8, sd7);

    // null comparison
    Assert.assertEquals(-1, sd0.compareTo(sd1));
    Assert.assertEquals(1, sd1.compareTo(sd0));

    // string comparison
    Assert.assertTrue(0 > sd1.compareTo(sd2));
    Assert.assertTrue(0 < sd2.compareTo(sd1));

    // long comparison
    Assert.assertEquals(-1, sd3.compareTo(sd4));
    Assert.assertEquals(1, sd4.compareTo(sd3));

    // double comparison
    Assert.assertEquals(-1, sd5.compareTo(sd6));
    Assert.assertEquals(1, sd6.compareTo(sd5));

    // number comparison
    Assert.assertEquals(-1, sd3.compareTo(sd5));
    Assert.assertEquals(1, sd5.compareTo(sd3));

    // object hash comparison
    Assert.assertEquals(1, sd7.compareTo(sd9));
    Assert.assertEquals(-1, sd9.compareTo(sd7));

    // test transitive
    Assert.assertEquals(-1, sd1.compareTo(sd3));
    Assert.assertEquals(1, sd3.compareTo(sd1));
    Assert.assertEquals(-1, sd2.compareTo(sd3));
    Assert.assertEquals(1, sd3.compareTo(sd2));

    Assert.assertEquals(-1, sd1.compareTo(sd9));
    Assert.assertEquals(1, sd9.compareTo(sd1));
    Assert.assertEquals(-1, sd2.compareTo(sd9));
    Assert.assertEquals(1, sd9.compareTo(sd2));

    Assert.assertEquals(-1, sd3.compareTo(sd9));
    Assert.assertEquals(1, sd9.compareTo(sd3));
    Assert.assertEquals(-1, sd4.compareTo(sd9));
    Assert.assertEquals(1, sd9.compareTo(sd4));
    Assert.assertEquals(-1, sd5.compareTo(sd9));
    Assert.assertEquals(1, sd9.compareTo(sd5));
    Assert.assertEquals(-1, sd6.compareTo(sd9));
    Assert.assertEquals(1, sd9.compareTo(sd6));

    // not equal
    StructuredData[] sds = {sd0, sd1, sd2, sd3, sd4, sd5, sd6, sd7, sd9};
    for (int i = 0; i < sds.length; i++) {
      for (int j = i + 1; j < sds.length; j++) {
        Assert.assertNotEquals(sds[i], sds[j]);
      }
    }
  }

  @Test
  public void testDifferentJSONOrder()
  {
    StructuredData sd0 = new StructuredData(
        ImmutableMap.of("D", 0.0, "E", 0.0, "F", 0.0, "A", 0.0, "B", 0.0, "C", 0.0)
    );
    StructuredData sd1 = new StructuredData(
        ImmutableMap.of("A", 0.0, "B", 0.0, "C", 0.0, "D", 0.0, "E", 0.0, "F", 0.0)
    );
    StructuredData sd2 = new StructuredData(
        ImmutableMap.of("A", 0.0, "B", 0.0, "C", 0.0, "D", 34304.0, "E", 34304.0, "F", 34304.0)
    );
    // The comparison here is based on the hash of the object, sd0 and sd1 have the same hash since WRITER uses ORDER_MAP_ENTRIES_BY_KEYS.
    Assert.assertEquals(1, sd0.compareTo(sd2));
    Assert.assertEquals(1, sd1.compareTo(sd2));
    Assert.assertEquals(0, sd0.compareTo(sd1));

    Assert.assertNotEquals(sd0, sd2);
    Assert.assertNotEquals(sd1, sd2);
    Assert.assertEquals(sd0, sd1);
    Assert.assertEquals(sd1, sd0);
    Assert.assertEquals(sd1.hashCode(), sd0.hashCode());

    Assert.assertEquals(88, sd0.getSizeEstimate());
    Assert.assertEquals(88, sd1.getSizeEstimate());
    Assert.assertEquals(88, sd2.getSizeEstimate());
  }
}
