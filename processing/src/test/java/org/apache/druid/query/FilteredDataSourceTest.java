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
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;

public class FilteredDataSourceTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final TableDataSource fooDataSource = new TableDataSource("foo");
  private final TableDataSource barDataSource = new TableDataSource("bar");
  private final FilteredDataSource filteredFooDataSource = FilteredDataSource.create(fooDataSource, null);
  private final FilteredDataSource filteredBarDataSource = FilteredDataSource.create(barDataSource, null);

  @Test
  public void test_getTableNames()
  {
    Assert.assertEquals(Collections.singleton("foo"), filteredFooDataSource.getTableNames());
  }

  @Test
  public void test_getChildren()
  {
    Assert.assertEquals(Collections.singletonList(fooDataSource), filteredFooDataSource.getChildren());
  }

  @Test
  public void test_isCacheable()
  {
    Assert.assertFalse(filteredFooDataSource.isCacheable(true));
  }

  @Test
  public void test_isGlobal()
  {
    Assert.assertFalse(filteredFooDataSource.isGlobal());
  }

  @Test
  public void test_isConcrete()
  {
    Assert.assertTrue(filteredFooDataSource.isConcrete());
  }

  @Test
  public void test_withChildren_empty()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Expected [1] child");
    Assert.assertSame(filteredFooDataSource, filteredFooDataSource.withChildren(Collections.emptyList()));
  }

  @Test
  public void test_withChildren_nonEmpty()
  {
    FilteredDataSource newFilteredDataSource = (FilteredDataSource) filteredFooDataSource.withChildren(ImmutableList.of(
        new TableDataSource("bar")));
    Assert.assertTrue(newFilteredDataSource.getBase().equals(barDataSource));

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Expected [1] child");
    filteredFooDataSource.withChildren(ImmutableList.of(fooDataSource, barDataSource));
  }

  @Test
  public void test_withUpdatedDataSource()
  {
    FilteredDataSource newFilteredDataSource = (FilteredDataSource) filteredFooDataSource.withUpdatedDataSource(
        new TableDataSource("bar"));
    Assert.assertTrue(newFilteredDataSource.getBase().equals(barDataSource));
  }

  @Test
  public void test_withAnalysis()
  {
    Assert.assertTrue(filteredFooDataSource.getAnalysis().equals(fooDataSource.getAnalysis()));
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(FilteredDataSource.class).usingGetClass().withNonnullFields("base").verify();
  }

  @Test
  public void test_serde_roundTrip() throws Exception
  {
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    final FilteredDataSource deserialized = (FilteredDataSource) jsonMapper.readValue(
        jsonMapper.writeValueAsString(filteredFooDataSource),
        DataSource.class
    );

    Assert.assertEquals(filteredFooDataSource, deserialized);
    Assert.assertNotEquals(fooDataSource, deserialized);
  }

  @Test
  public void test_deserialize_fromObject() throws Exception
  {
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();

    final FilteredDataSource deserializedFilteredDataSource = jsonMapper.readValue(
        "{\"type\":\"filter\",\"base\":{\"type\":\"table\",\"name\":\"foo\"},\"filter\":null}",
        FilteredDataSource.class
    );

    Assert.assertEquals(filteredFooDataSource, deserializedFilteredDataSource);
    Assert.assertNotEquals(fooDataSource, deserializedFilteredDataSource);
  }

  @Test
  public void test_serialize() throws Exception
  {
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    final String s = jsonMapper.writeValueAsString(filteredFooDataSource);
    Assert.assertEquals("{\"type\":\"filter\",\"base\":{\"type\":\"table\",\"name\":\"foo\"},\"filter\":null}", s);
  }

  @Test
  public void testStringRep()
  {
    Assert.assertFalse(filteredFooDataSource.toString().equals(filteredBarDataSource.toString()));
  }
}
