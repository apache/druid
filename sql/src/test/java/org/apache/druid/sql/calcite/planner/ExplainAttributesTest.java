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

package org.apache.druid.sql.calcite.planner;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.sql.destination.TableDestination;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ExplainAttributesTest
{
  private static final ObjectMapper DEFAULT_OBJECT_MAPPER = new DefaultObjectMapper();

  @Test
  void simpleGetters()
  {
    ExplainAttributes selectAttributes = new ExplainAttributes("SELECT", null, null, null, null);
    assertEquals("SELECT", selectAttributes.getStatementType());
    assertNull(selectAttributes.getTargetDataSource());
    assertNull(selectAttributes.getPartitionedBy());
    assertNull(selectAttributes.getClusteredBy());
    assertNull(selectAttributes.getReplaceTimeChunks());
  }

  @Test
  void serializeSelectAttributes() throws JsonProcessingException
  {
    ExplainAttributes selectAttributes = new ExplainAttributes(
        "SELECT",
        null,
        null,
        null,
        null
    );
    final String expectedAttributes = "{"
                                      + "\"statementType\":\"SELECT\""
                                      + "}";
    assertEquals(expectedAttributes, DEFAULT_OBJECT_MAPPER.writeValueAsString(selectAttributes));
  }

  @Test
  void serializeInsertAttributes() throws JsonProcessingException
  {
    ExplainAttributes insertAttributes = new ExplainAttributes(
        "INSERT",
        new TableDestination("foo"),
        Granularities.DAY,
        null,
        null
    );
    final String expectedAttributes = "{"
                                      + "\"statementType\":\"INSERT\","
                                      + "\"targetDataSource\":{\"type\":\"table\",\"tableName\":\"foo\"},"
                                      + "\"partitionedBy\":\"DAY\""
                                      + "}";
    assertEquals(expectedAttributes, DEFAULT_OBJECT_MAPPER.writeValueAsString(insertAttributes));
  }

  @Test
  void serializeInsertAllAttributes() throws JsonProcessingException
  {
    ExplainAttributes insertAttributes = new ExplainAttributes(
        "INSERT",
        new TableDestination("foo"),
        Granularities.ALL,
        null,
        null
    );
    final String expectedAttributes = "{"
                                      + "\"statementType\":\"INSERT\","
                                      + "\"targetDataSource\":{\"type\":\"table\",\"tableName\":\"foo\"},"
                                      + "\"partitionedBy\":{\"type\":\"all\"}"
                                      + "}";
    assertEquals(expectedAttributes, DEFAULT_OBJECT_MAPPER.writeValueAsString(insertAttributes));
  }

  @Test
  void serializeReplaceAttributes() throws JsonProcessingException
  {
    ExplainAttributes replaceAttributes1 = new ExplainAttributes(
        "REPLACE",
        new TableDestination("foo"),
        Granularities.HOUR,
        null,
        "ALL"
    );
    final String expectedAttributes1 = "{"
        + "\"statementType\":\"REPLACE\","
        + "\"targetDataSource\":{\"type\":\"table\",\"tableName\":\"foo\"},"
        + "\"partitionedBy\":\"HOUR\","
        + "\"replaceTimeChunks\":\"ALL\""
        + "}";
    assertEquals(expectedAttributes1, DEFAULT_OBJECT_MAPPER.writeValueAsString(replaceAttributes1));


    ExplainAttributes replaceAttributes2 = new ExplainAttributes(
        "REPLACE",
        new TableDestination("foo"),
        Granularities.HOUR,
        null,
        "2019-08-25T02:00:00.000Z/2019-08-25T03:00:00.000Z"
    );
    final String expectedAttributes2 = "{"
                                      + "\"statementType\":\"REPLACE\","
                                      + "\"targetDataSource\":{\"type\":\"table\",\"tableName\":\"foo\"},"
                                      + "\"partitionedBy\":\"HOUR\","
                                      + "\"replaceTimeChunks\":\"2019-08-25T02:00:00.000Z/2019-08-25T03:00:00.000Z\""
                                      + "}";
    assertEquals(expectedAttributes2, DEFAULT_OBJECT_MAPPER.writeValueAsString(replaceAttributes2));
  }

  @Test
  void serializeReplaceWithClusteredByAttributes() throws JsonProcessingException
  {
    ExplainAttributes replaceAttributes1 = new ExplainAttributes(
        "REPLACE",
        new TableDestination("foo"),
        Granularities.HOUR,
        Arrays.asList("foo", "CEIL(`f2`)"),
        "ALL"
    );
    final String expectedAttributes1 = "{"
                                       + "\"statementType\":\"REPLACE\","
                                       + "\"targetDataSource\":{\"type\":\"table\",\"tableName\":\"foo\"},"
                                       + "\"partitionedBy\":\"HOUR\","
                                       + "\"clusteredBy\":[\"foo\",\"CEIL(`f2`)\"],"
                                       + "\"replaceTimeChunks\":\"ALL\""
                                       + "}";
    assertEquals(expectedAttributes1, DEFAULT_OBJECT_MAPPER.writeValueAsString(replaceAttributes1));


    ExplainAttributes replaceAttributes2 = new ExplainAttributes(
        "REPLACE",
        new TableDestination("foo"),
        Granularities.HOUR,
        Arrays.asList("foo", "boo"),
        "2019-08-25T02:00:00.000Z/2019-08-25T03:00:00.000Z"
    );
    final String expectedAttributes2 = "{"
                                       + "\"statementType\":\"REPLACE\","
                                       + "\"targetDataSource\":{\"type\":\"table\",\"tableName\":\"foo\"},"
                                       + "\"partitionedBy\":\"HOUR\","
                                       + "\"clusteredBy\":[\"foo\",\"boo\"],"
                                       + "\"replaceTimeChunks\":\"2019-08-25T02:00:00.000Z/2019-08-25T03:00:00.000Z\""
                                       + "}";
    assertEquals(expectedAttributes2, DEFAULT_OBJECT_MAPPER.writeValueAsString(replaceAttributes2));
  }
}
