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

package org.apache.druid.indexing.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class CategoryCapacityInfoTest
{
  @Test
  public void testSerde() throws Exception
  {
    CategoryCapacityInfo categoryCapacityInfo = new CategoryCapacityInfo(
        ImmutableList.of("kill", "compact"),
        10
    );
    ObjectMapper mapper = new DefaultObjectMapper();
    final CategoryCapacityInfo serde = mapper.readValue(
        mapper.writeValueAsString(categoryCapacityInfo),
        CategoryCapacityInfo.class
    );
    Assert.assertEquals(categoryCapacityInfo, serde);
  }

  @Test
  public void testEqualsAndSerde()
  {
    // Everything equal
    assertEqualsAndHashCode(new CategoryCapacityInfo(

        ImmutableList.of("kill", "compact"),
        10
    ), new CategoryCapacityInfo(
        ImmutableList.of("kill", "compact"),
        10
    ), true);

    // same task type different
    assertEqualsAndHashCode(new CategoryCapacityInfo(

        ImmutableList.of("kill", "compact1"),
        10
    ), new CategoryCapacityInfo(
        ImmutableList.of("kill", "compact"),
        10
    ), false);

    // capacity different
    assertEqualsAndHashCode(new CategoryCapacityInfo(

        ImmutableList.of("kill", "compact"),
        10
    ), new CategoryCapacityInfo(
        ImmutableList.of("kill", "compact"),
        11
    ), false);

    // capacity and task type different
    assertEqualsAndHashCode(new CategoryCapacityInfo(

        ImmutableList.of("kill", "compact1"),
        10
    ), new CategoryCapacityInfo(
        ImmutableList.of("kill", "compact"),
        11
    ), false);
  }

  private void assertEqualsAndHashCode(CategoryCapacityInfo o1, CategoryCapacityInfo o2, boolean shouldMatch)
  {
    if (shouldMatch) {
      Assert.assertTrue(o1.equals(o2));
      Assert.assertEquals(o1.hashCode(), o2.hashCode());
    } else {
      Assert.assertFalse(o1.equals(o2));
      Assert.assertNotEquals(o1.hashCode(), o2.hashCode());
    }
  }
}
