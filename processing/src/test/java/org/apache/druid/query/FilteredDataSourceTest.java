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
import org.apache.druid.query.filter.DimFilters;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class FilteredDataSourceTest extends InitializedNullHandlingTest
{
  private final TableDataSource fooDataSource = new TableDataSource("foo");
  private final TableDataSource barDataSource = new TableDataSource("bar");
  private final FilteredDataSource filteredFooDataSource = FilteredDataSource.create(fooDataSource, DimFilters.dimEquals("col", "value"));
  private final FilteredDataSource filteredBarDataSource = FilteredDataSource.create(barDataSource, DimFilters.dimEquals("col", "value"));

  @Test
  public void test_getTableNames()
  {
    Assertions.assertEquals(Collections.singleton("foo"), filteredFooDataSource.getTableNames());
  }

  @Test
  public void test_getChildren()
  {
    Assertions.assertEquals(Collections.singletonList(fooDataSource), filteredFooDataSource.getChildren());
  }

  @Test
  public void test_isCacheable()
  {
    Assertions.assertFalse(filteredFooDataSource.isCacheable(true));
  }

  @Test
  public void test_isGlobal()
  {
    Assertions.assertFalse(filteredFooDataSource.isGlobal());
  }

  @Test
  public void test_isConcrete()
  {
    Assertions.assertTrue(filteredFooDataSource.isProcessable());
  }

  @Test
  public void test_withChildren_empty()
  {
    IllegalArgumentException e = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> filteredFooDataSource.withChildren(Collections.emptyList())
    );
    Assertions.assertTrue(e.getMessage().contains("Expected [1] child"));
  }

  @Test
  public void test_withChildren_nonEmpty()
  {
    FilteredDataSource newFilteredDataSource = (FilteredDataSource) filteredFooDataSource.withChildren(ImmutableList.of(
        new TableDataSource("bar")));
    Assertions.assertEquals(newFilteredDataSource.getBase(), barDataSource);

    IllegalArgumentException e = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> filteredFooDataSource.withChildren(ImmutableList.of(fooDataSource, barDataSource))
    );
    Assertions.assertTrue(e.getMessage().contains("Expected [1] child"));
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

    Assertions.assertEquals(filteredFooDataSource, deserialized);
    Assertions.assertNotEquals(fooDataSource, deserialized);
  }

  @Test
  public void test_deserialize_fromObject() throws Exception
  {
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();

    final FilteredDataSource deserializedFilteredDataSource = jsonMapper.readValue(
        "{\"type\":\"filter\",\"base\":{\"type\":\"table\",\"name\":\"foo\"},\"filter\":{\"type\":\"selector\",\"dimension\":\"col\",\"value\":\"value\"}}",
        FilteredDataSource.class
    );

    Assertions.assertEquals(filteredFooDataSource, deserializedFilteredDataSource);
    Assertions.assertNotEquals(fooDataSource, deserializedFilteredDataSource);
  }

  @Test
  public void test_serialize() throws Exception
  {
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    final String s = jsonMapper.writeValueAsString(filteredFooDataSource);
    Assertions.assertEquals(
        "{\"type\":\"filter\",\"base\":{\"type\":\"table\",\"name\":\"foo\"},\"filter\":{\"type\":\"selector\",\"dimension\":\"col\",\"value\":\"value\"}}",
        s
    );
  }

  @Test
  public void testStringRep()
  {
    Assertions.assertNotEquals(filteredFooDataSource.toString(), filteredBarDataSource.toString());
  }
}
