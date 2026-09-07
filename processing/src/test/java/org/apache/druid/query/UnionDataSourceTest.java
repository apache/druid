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
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.NoopDataSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

public class UnionDataSourceTest
{
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

  private final UnionDataSource tableAndInlineUniondataSource = new UnionDataSource(
      ImmutableList.of(
          new TableDataSource("foo"),
          InlineDataSource.fromIterable(
              Collections.emptyList(),
              RowSignature.empty()
          )
      )
  );

  @Test
  public void test_constructor_empty()
  {
    IllegalStateException e = Assertions.assertThrows(
        IllegalStateException.class,
        //noinspection ResultOfObjectAllocationIgnored
        () -> new UnionDataSource(Collections.emptyList())
    );
    Assertions.assertTrue(e.getMessage().contains("'dataSources' must be non-null and non-empty for 'union'"));
  }

  @Test
  public void test_isCompatible()
  {
    TableDataSource tableDataSource = new TableDataSource("foo");
    InlineDataSource inlineDataSource = InlineDataSource.fromIterable(Collections.emptyList(), RowSignature.empty());

    Assertions.assertTrue(UnionDataSource.isCompatibleDataSource(tableDataSource));
    Assertions.assertTrue(UnionDataSource.isCompatibleDataSource(inlineDataSource));
    Assertions.assertFalse(UnionDataSource.isCompatibleDataSource(new NoopDataSource()));
  }

  @Test
  public void test_isTableBased()
  {
    Assertions.assertTrue(unionDataSource.isTableBased());
    Assertions.assertTrue(unionDataSourceWithDuplicates.isTableBased());

    Assertions.assertFalse(tableAndInlineUniondataSource.isTableBased());
  }

  @Test
  public void test_getTableNames()
  {
    Assertions.assertEquals(ImmutableSet.of("foo", "bar"), unionDataSource.getTableNames());
  }

  @Test
  public void test_getTableNames_withDuplicates()
  {
    Assertions.assertEquals(ImmutableSet.of("foo", "bar"), unionDataSourceWithDuplicates.getTableNames());
  }

  @Test
  public void test_getChildren()
  {
    Assertions.assertEquals(
        ImmutableList.of(new TableDataSource("foo"), new TableDataSource("bar")),
        unionDataSource.getChildren()
    );
  }

  @Test
  public void test_getChildren_withDuplicates()
  {
    Assertions.assertEquals(
        ImmutableList.of(new TableDataSource("bar"), new TableDataSource("foo"), new TableDataSource("bar")),
        unionDataSourceWithDuplicates.getChildren()
    );
  }

  @Test
  public void test_isCacheable()
  {
    Assertions.assertFalse(unionDataSource.isCacheable(true));
    Assertions.assertFalse(unionDataSource.isCacheable(false));
  }

  @Test
  public void test_isGlobal()
  {
    Assertions.assertFalse(unionDataSource.isGlobal());
  }

  @Test
  public void test_isConcrete()
  {
    Assertions.assertTrue(unionDataSource.isProcessable());
  }

  @Test
  public void test_withChildren_empty()
  {
    IllegalArgumentException e = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> unionDataSource.withChildren(Collections.emptyList())
    );
    Assertions.assertTrue(e.getMessage().contains("Expected [2] children, got [0]"));
  }

  @Test
  public void test_withChildren_sameNumber()
  {
    final List<DataSource> newDataSources = ImmutableList.of(
        new TableDataSource("baz"),
        new TableDataSource("qux")
    );

    //noinspection unchecked
    Assertions.assertEquals(
        new UnionDataSource(newDataSources),
        unionDataSource.withChildren(newDataSources)
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

    Assertions.assertEquals(unionDataSource, deserialized);
  }
}
