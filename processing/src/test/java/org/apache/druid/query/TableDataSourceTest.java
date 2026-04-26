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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class TableDataSourceTest
{
  private final TableDataSource fooDataSource = new TableDataSource("foo");
  private final TableDataSource barDataSource = new TableDataSource("bar");

  @Test
  public void test_getTableNames()
  {
    Assertions.assertEquals(Collections.singleton("foo"), fooDataSource.getTableNames());
  }

  @Test
  public void test_getChildren()
  {
    Assertions.assertEquals(Collections.emptyList(), fooDataSource.getChildren());
  }

  @Test
  public void test_isCacheable()
  {
    Assertions.assertTrue(fooDataSource.isCacheable(true));
    Assertions.assertTrue(fooDataSource.isCacheable(false));
  }

  @Test
  public void test_isGlobal()
  {
    Assertions.assertFalse(fooDataSource.isGlobal());
  }

  @Test
  public void test_isConcrete()
  {
    Assertions.assertTrue(fooDataSource.isProcessable());
  }

  @Test
  public void test_withChildren_empty()
  {
    Assertions.assertSame(fooDataSource, fooDataSource.withChildren(Collections.emptyList()));
  }

  @Test
  public void test_withChildren_nonEmpty()
  {
    final IllegalArgumentException e = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> fooDataSource.withChildren(ImmutableList.of(new TableDataSource("bar")))
    );
    Assertions.assertTrue(e.getMessage().contains("Cannot accept children"));
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(TableDataSource.class).usingGetClass().withNonnullFields("name").verify();
  }

  @Test
  public void test_serde_roundTrip() throws Exception
  {
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    final TableDataSource deserialized = (TableDataSource) jsonMapper.readValue(
        jsonMapper.writeValueAsString(fooDataSource),
        DataSource.class
    );

    Assertions.assertEquals(fooDataSource, deserialized);
    Assertions.assertNotEquals(barDataSource, deserialized);
  }

  @Test
  public void test_deserialize_fromObject() throws Exception
  {
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    final TableDataSource deserialized = (TableDataSource) jsonMapper.readValue(
        "{\"type\":\"table\",\"name\":\"foo\"}",
        DataSource.class
    );

    Assertions.assertEquals(fooDataSource, deserialized);
    Assertions.assertNotEquals(barDataSource, deserialized);
  }

  @Test
  public void test_deserialize_fromString() throws Exception
  {
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    final TableDataSource deserialized = (TableDataSource) jsonMapper.readValue(
        "\"foo\"",
        DataSource.class
    );

    Assertions.assertEquals(fooDataSource, deserialized);
    Assertions.assertNotEquals(barDataSource, deserialized);
  }

  @Test
  public void test_serialize() throws Exception
  {
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    final String s = jsonMapper.writeValueAsString(fooDataSource);
    Assertions.assertEquals("{\"type\":\"table\",\"name\":\"foo\"}", s);
  }
}
