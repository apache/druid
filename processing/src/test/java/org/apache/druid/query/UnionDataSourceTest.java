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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.List;

public class UnionDataSourceTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final UnionDataSource unionDataSource = new UnionDataSource(
      ImmutableList.of(
          new TableDataSource("foo"),
          new TableDataSource("bar")
      )
  );

  private final UnionDataSource unionDataSourceWithDuplicates = new UnionDataSource(
      ImmutableList.of(
          new TableDataSource("bar"),
          new TableDataSource("foo"),
          new TableDataSource("bar")
      )
  );

  @Test
  public void test_getTableNames()
  {
    Assert.assertEquals(ImmutableSet.of("foo", "bar"), unionDataSource.getTableNames());
  }

  @Test
  public void test_getTableNames_withDuplicates()
  {
    Assert.assertEquals(ImmutableSet.of("foo", "bar"), unionDataSourceWithDuplicates.getTableNames());
  }

  @Test
  public void test_getChildren()
  {
    Assert.assertEquals(
        ImmutableList.of(new TableDataSource("foo"), new TableDataSource("bar")),
        unionDataSource.getChildren()
    );
  }

  @Test
  public void test_getChildren_withDuplicates()
  {
    Assert.assertEquals(
        ImmutableList.of(new TableDataSource("bar"), new TableDataSource("foo"), new TableDataSource("bar")),
        unionDataSourceWithDuplicates.getChildren()
    );
  }

  @Test
  public void test_isCacheable()
  {
    Assert.assertFalse(unionDataSource.isCacheable());
  }

  @Test
  public void test_isGlobal()
  {
    Assert.assertFalse(unionDataSource.isGlobal());
  }

  @Test
  public void test_isConcrete()
  {
    Assert.assertTrue(unionDataSource.isConcrete());
  }

  @Test
  public void test_withChildren_empty()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Expected [2] children, got [0]");

    unionDataSource.withChildren(Collections.emptyList());
  }

  @Test
  public void test_withChildren_sameNumber()
  {
    final List<TableDataSource> newDataSources = ImmutableList.of(
        new TableDataSource("baz"),
        new TableDataSource("qux")
    );

    //noinspection unchecked
    Assert.assertEquals(
        new UnionDataSource(newDataSources),
        unionDataSource.withChildren((List) newDataSources)
    );
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(UnionDataSource.class).usingGetClass().withNonnullFields("dataSources").verify();
  }

  @Test
  public void test_serde() throws Exception
  {
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    final UnionDataSource deserialized = (UnionDataSource) jsonMapper.readValue(
        jsonMapper.writeValueAsString(unionDataSource),
        DataSource.class
    );

    Assert.assertEquals(unionDataSource, deserialized);
  }
}
