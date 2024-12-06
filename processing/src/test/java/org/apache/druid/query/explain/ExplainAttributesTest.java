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

package org.apache.druid.query.explain;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.error.DruidException;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class ExplainAttributesTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  @Test
  public void testGetters()
  {
    final ExplainAttributes selectAttributes = new ExplainAttributes("SELECT", null, null, null, null);
    assertEquals("SELECT", selectAttributes.getStatementType());
    Assert.assertNull(selectAttributes.getTargetDataSource());
    Assert.assertNull(selectAttributes.getPartitionedBy());
    Assert.assertNull(selectAttributes.getClusteredBy());
    Assert.assertNull(selectAttributes.getReplaceTimeChunks());
  }

  @Test
  public void testSerdeOfSelectAttributes()
  {
    final ExplainAttributes selectAttributes = new ExplainAttributes(
        "SELECT",
        null,
        null,
        null,
        null
    );
    final String expectedAttributes = "{"
                                      + "\"statementType\":\"SELECT\""
                                      + "}";

    testSerde(selectAttributes, expectedAttributes);
  }

  @Test
  public void testSerdeOfInsertAttributes()
  {
    final ExplainAttributes insertAttributes = new ExplainAttributes(
        "INSERT",
        "foo",
        Granularities.DAY,
        null,
        null
    );
    final String expectedAttributes = "{"
                                      + "\"statementType\":\"INSERT\","
                                      + "\"targetDataSource\":\"foo\","
                                      + "\"partitionedBy\":\"DAY\""
                                      + "}";
    testSerde(insertAttributes, expectedAttributes);
  }

  @Test
  public void testSerdeOfInsertAllAttributes()
  {
    final ExplainAttributes insertAttributes = new ExplainAttributes(
        "INSERT",
        "foo",
        Granularities.ALL,
        null,
        null
    );
    final String expectedAttributes = "{"
                                      + "\"statementType\":\"INSERT\","
                                      + "\"targetDataSource\":\"foo\","
                                      + "\"partitionedBy\":{\"type\":\"all\"}"
                                      + "}";
    testSerde(insertAttributes, expectedAttributes);
  }

  @Test
  public void testSerdeOfReplaceAttributes()
  {
    final ExplainAttributes replaceAttributes = new ExplainAttributes(
        "REPLACE",
        "foo",
        Granularities.HOUR,
        null,
        "ALL"
    );
    final String expectedAttributes = "{"
                                       + "\"statementType\":\"REPLACE\","
                                       + "\"targetDataSource\":\"foo\","
                                       + "\"partitionedBy\":\"HOUR\","
                                       + "\"replaceTimeChunks\":\"ALL\""
                                       + "}";
    testSerde(replaceAttributes, expectedAttributes);

  }

  @Test
  public void testSerdeOfReplaceAttributesWithTimeChunks()
  {
    final ExplainAttributes replaceAttributes = new ExplainAttributes(
        "REPLACE",
        "foo",
        Granularities.HOUR,
        null,
        "2019-08-25T02:00:00.000Z/2019-08-25T03:00:00.000Z"
    );
    final String expectedAttributes = "{"
                                       + "\"statementType\":\"REPLACE\","
                                       + "\"targetDataSource\":\"foo\","
                                       + "\"partitionedBy\":\"HOUR\","
                                       + "\"replaceTimeChunks\":\"2019-08-25T02:00:00.000Z/2019-08-25T03:00:00.000Z\""
                                       + "}";
    testSerde(replaceAttributes, expectedAttributes);
  }

  @Test
  public void testReplaceAttributesWithClusteredBy()
  {
    final ExplainAttributes replaceAttributes = new ExplainAttributes(
        "REPLACE",
        "foo",
        Granularities.HOUR,
        Arrays.asList("foo", "CEIL(`f2`)"),
        "ALL"
    );
    final String expectedAttributes = "{"
                                       + "\"statementType\":\"REPLACE\","
                                       + "\"targetDataSource\":\"foo\","
                                       + "\"partitionedBy\":\"HOUR\","
                                       + "\"clusteredBy\":[\"foo\",\"CEIL(`f2`)\"],"
                                       + "\"replaceTimeChunks\":\"ALL\""
                                       + "}";
    testSerde(replaceAttributes, expectedAttributes);
  }

  @Test
  public void testReplaceAttributesWithClusteredByAndTimeChunks()
  {
    final ExplainAttributes replaceAttributes = new ExplainAttributes(
        "REPLACE",
        "foo",
        Granularities.HOUR,
        Arrays.asList("foo", "boo"),
        "2019-08-25T02:00:00.000Z/2019-08-25T03:00:00.000Z"
    );
    final String expectedAttributes = "{"
                                       + "\"statementType\":\"REPLACE\","
                                       + "\"targetDataSource\":\"foo\","
                                       + "\"partitionedBy\":\"HOUR\","
                                       + "\"clusteredBy\":[\"foo\",\"boo\"],"
                                       + "\"replaceTimeChunks\":\"2019-08-25T02:00:00.000Z/2019-08-25T03:00:00.000Z\""
                                       + "}";
    testSerde(replaceAttributes, expectedAttributes);
  }

  private void testSerde(final ExplainAttributes explainAttributes, final String expectedSerializedAttributes)
  {
    final ExplainAttributes observedAttributes;
    try {
      final String observedSerializedAttributes = MAPPER.writeValueAsString(explainAttributes);
      assertEquals(expectedSerializedAttributes, observedSerializedAttributes);
      observedAttributes = MAPPER.readValue(observedSerializedAttributes, ExplainAttributes.class);
    }
    catch (Exception e) {
      throw DruidException.defensive(e, "Error serializing/deserializing explain plan[%s].", explainAttributes);
    }
    assertEquals(explainAttributes, observedAttributes);
  }

}
