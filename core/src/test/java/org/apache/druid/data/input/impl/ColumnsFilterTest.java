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

package org.apache.druid.data.input.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.data.input.ColumnsFilter;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

public class ColumnsFilterTest
{
  private static final List<String> COLUMNS = ImmutableList.of("a", "b", "c");

  @Test
  public void testAll()
  {
    Assert.assertEquals(
        ImmutableList.of("a", "b", "c"),
        apply(ColumnsFilter.all(), COLUMNS)
    );
  }

  @Test
  public void testInclusionBased()
  {
    Assert.assertEquals(
        ImmutableList.of("b"),
        apply(ColumnsFilter.inclusionBased(ImmutableSet.of("b")), COLUMNS)
    );
  }

  @Test
  public void testInclusionBasedPlus()
  {
    Assert.assertEquals(
        ColumnsFilter.inclusionBased(ImmutableSet.of("a", "b", "c")),
        ColumnsFilter.inclusionBased(ImmutableSet.of("b", "c")).plus("a").plus("c")
    );
  }

  @Test
  public void testExclusionBased()
  {
    Assert.assertEquals(
        ImmutableList.of("a", "c"),
        apply(ColumnsFilter.exclusionBased(ImmutableSet.of("b")), COLUMNS)
    );
  }

  @Test
  public void testExclusionBasedPlus()
  {
    Assert.assertEquals(
        ColumnsFilter.exclusionBased(ImmutableSet.of("b")),
        ColumnsFilter.exclusionBased(ImmutableSet.of("b", "c")).plus("a").plus("c")
    );
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(ColumnsFilter.InclusionBased.class).usingGetClass().verify();
    EqualsVerifier.forClass(ColumnsFilter.ExclusionBased.class).usingGetClass().verify();
  }

  private List<String> apply(ColumnsFilter columnsFilter, List<String> columns)
  {
    return columns.stream().filter(columnsFilter::apply).collect(Collectors.toList());
  }
}
