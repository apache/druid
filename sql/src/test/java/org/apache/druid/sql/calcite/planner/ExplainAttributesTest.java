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
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ExplainAttributesTest
{
  private static final ObjectMapper DEFAULT_OBJECT_MAPPER = new DefaultObjectMapper();
  private static final SqlNode DATA_SOURCE = Mockito.mock(SqlNode.class);
  private static final SqlNodeList CLUSTERED_BY = Mockito.mock(SqlNodeList.class);
  private static final SqlNode TIME_CHUNKS = Mockito.mock(SqlNode.class);

  @Before
  public void setup()
  {
    Mockito.when(DATA_SOURCE.toString()).thenReturn("foo");
    Mockito.when(CLUSTERED_BY.toString()).thenReturn("`bar`, `jazz`");
    Mockito.when(TIME_CHUNKS.toString()).thenReturn("ALL");
  }

  @Test
  public void testSimpleGetters()
  {
    ExplainAttributes selectAttributes = new ExplainAttributes("SELECT", null, null, null, null);
    Assert.assertEquals("SELECT", selectAttributes.getStatementType());
    Assert.assertNull(selectAttributes.getTargetDataSource());
    Assert.assertNull(selectAttributes.getPartitionedBy());
    Assert.assertNull(selectAttributes.getClusteredBy());
    Assert.assertNull(selectAttributes.getReplaceTimeChunks());
  }

  @Test
  public void testSerializeSelectAttributes() throws JsonProcessingException
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
    Assert.assertEquals(expectedAttributes, DEFAULT_OBJECT_MAPPER.writeValueAsString(selectAttributes));
  }

  @Test
  public void testSerializeInsertAttributes() throws JsonProcessingException
  {
    ExplainAttributes insertAttributes = new ExplainAttributes(
        "INSERT",
        DATA_SOURCE,
        Granularities.DAY,
        null,
        null
    );
    final String expectedAttributes = "{"
                                      + "\"statementType\":\"INSERT\","
                                      + "\"targetDataSource\":\"foo\","
                                      + "\"partitionedBy\":\"DAY\""
                                      + "}";
    Assert.assertEquals(expectedAttributes, DEFAULT_OBJECT_MAPPER.writeValueAsString(insertAttributes));
  }

  @Test
  public void testSerializeInsertAllAttributes() throws JsonProcessingException
  {
    ExplainAttributes insertAttributes = new ExplainAttributes(
        "INSERT",
        DATA_SOURCE,
        Granularities.ALL,
        null,
        null
    );
    final String expectedAttributes = "{"
                                      + "\"statementType\":\"INSERT\","
                                      + "\"targetDataSource\":\"foo\","
                                      + "\"partitionedBy\":{\"type\":\"all\"}"
                                      + "}";
    Assert.assertEquals(expectedAttributes, DEFAULT_OBJECT_MAPPER.writeValueAsString(insertAttributes));
  }

  @Test
  public void testSerializeReplaceAttributes() throws JsonProcessingException
  {
    ExplainAttributes replaceAttributes = new ExplainAttributes(
        "REPLACE",
        DATA_SOURCE,
        Granularities.HOUR,
        CLUSTERED_BY,
        TIME_CHUNKS
    );
    final String expectedAttributes = "{"
        + "\"statementType\":\"REPLACE\","
        + "\"targetDataSource\":\"foo\","
        + "\"partitionedBy\":\"HOUR\","
        + "\"clusteredBy\":\"`bar`, `jazz`\","
        + "\"replaceTimeChunks\":\"ALL\""
        + "}";
    Assert.assertEquals(expectedAttributes, DEFAULT_OBJECT_MAPPER.writeValueAsString(replaceAttributes));
  }
}
