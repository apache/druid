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

package org.apache.druid.timeline.partition;

import com.google.common.collect.ImmutableSet;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class TypedValueSetTest
{
  @Test
  public void testContainsAndGetters()
  {
    final TypedValueSet set = new TypedValueSet(ImmutableSet.of("1", "2"), ColumnType.LONG);
    Assert.assertEquals(ColumnType.LONG, set.getType());
    Assert.assertEquals(ImmutableSet.of("1", "2"), set.getValues());
    Assert.assertTrue(set.contains("1"));
    Assert.assertTrue(set.contains("2"));
    Assert.assertFalse(set.contains("3"));
    Assert.assertFalse(set.contains(null));
  }

  @Test
  public void testNullElementIsRetained()
  {
    final Set<String> withNull = new HashSet<>(Arrays.asList("1", null));
    final TypedValueSet set = new TypedValueSet(withNull, ColumnType.LONG);
    Assert.assertTrue(set.contains(null));
    Assert.assertTrue(set.contains("1"));
  }

  @Test
  public void testDefensiveCopy()
  {
    final Set<String> mutable = new HashSet<>(Arrays.asList("1"));
    final TypedValueSet set = new TypedValueSet(mutable, ColumnType.LONG);
    mutable.add("2");
    // Later mutation of the source must not leak in.
    Assert.assertFalse(set.contains("2"));
  }

  @Test
  public void testValuesUnmodifiable()
  {
    final TypedValueSet set = new TypedValueSet(ImmutableSet.of("1"), ColumnType.LONG);
    Assert.assertThrows(UnsupportedOperationException.class, () -> set.getValues().add("2"));
  }

  @Test
  public void testEqualsAndHashCode()
  {
    final TypedValueSet a = new TypedValueSet(ImmutableSet.of("1", "2"), ColumnType.LONG);
    final TypedValueSet b = new TypedValueSet(ImmutableSet.of("2", "1"), ColumnType.LONG);
    final TypedValueSet differentValues = new TypedValueSet(ImmutableSet.of("1", "3"), ColumnType.LONG);
    final TypedValueSet differentType = new TypedValueSet(ImmutableSet.of("1", "2"), ColumnType.DOUBLE);

    Assert.assertEquals(a, b);
    Assert.assertEquals(a.hashCode(), b.hashCode());
    Assert.assertNotEquals(a, differentValues);
    Assert.assertNotEquals(a, differentType);
  }
}
