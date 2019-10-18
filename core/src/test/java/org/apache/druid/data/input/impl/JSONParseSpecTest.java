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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.TestObjectMapper;
import org.apache.druid.java.util.common.parsers.JSONExplodeSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.Parser;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JSONParseSpecTest
{
  private final ObjectMapper jsonMapper = new TestObjectMapper();

  @Test
  public void testParseRow()
  {
    final JSONParseSpec parseSpec = new JSONParseSpec(
        new TimestampSpec("timestamp", "iso", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("bar", "foo")), null, null),
        new JSONPathSpec(
            true,
            ImmutableList.of(
                new JSONPathFieldSpec(JSONPathFieldType.ROOT, "root_baz", "baz"),
                new JSONPathFieldSpec(JSONPathFieldType.ROOT, "root_baz2", "baz2"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "path_omg", "$.o.mg"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "path_omg2", "$.o.mg2"),
                new JSONPathFieldSpec(JSONPathFieldType.JQ, "jq_omg", ".o.mg"),
                new JSONPathFieldSpec(JSONPathFieldType.JQ, "jq_omg2", ".o.mg2")
            )
        ),
        null,
        null
    );

    final Map<String, Object> expected = new HashMap<>();
    expected.put("foo", "x");
    expected.put("baz", 4L);
    expected.put("root_baz", 4L);
    expected.put("root_baz2", null);
    expected.put("path_omg", 1L);
    expected.put("path_omg2", null);
    expected.put("jq_omg", 1L);
    expected.put("jq_omg2", null);

    final Parser<String, Object> parser = parseSpec.makeParser();
    final Map<String, Object> parsedRow = parser.parseToMap("{\"bar\":null,\"foo\":\"x\",\"baz\":4,\"o\":{\"mg\":1}}");
    Assert.assertNotNull(parsedRow);
    Assert.assertEquals(expected, parsedRow);
    Assert.assertNull(parsedRow.get("bar"));
    Assert.assertNull(parsedRow.get("buzz"));
    Assert.assertNull(parsedRow.get("root_baz2"));
    Assert.assertNull(parsedRow.get("jq_omg2"));
    Assert.assertNull(parsedRow.get("path_omg2"));
  }

  @Test
  public void testParseRowWithConditional()
  {
    final JSONParseSpec parseSpec = new JSONParseSpec(
        new TimestampSpec("timestamp", "iso", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("foo")), null, null),
        new JSONPathSpec(
            true,
            ImmutableList.of(
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "foo", "$.[?(@.maybe_object)].maybe_object.foo.test"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "baz", "$.maybe_object_2.foo.test"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "bar", "$.[?(@.something_else)].something_else.foo")
            )
        ),
        null,
        null
    );

    final Map<String, Object> expected = new HashMap<>();
    expected.put("foo", new ArrayList());
    expected.put("baz", null);
    expected.put("bar", Collections.singletonList("test"));

    final Parser<String, Object> parser = parseSpec.makeParser();
    final Map<String, Object> parsedRow = parser.parseToMap("{\"something_else\": {\"foo\": \"test\"}}");

    Assert.assertNotNull(parsedRow);
    Assert.assertEquals(expected, parsedRow);
  }

  @Test
  public void testSerde() throws IOException
  {
    HashMap<String, Boolean> feature = new HashMap<String, Boolean>();
    feature.put("ALLOW_UNQUOTED_CONTROL_CHARS", true);
    JSONParseSpec spec = new JSONParseSpec(
        new TimestampSpec("timestamp", "iso", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("bar", "foo")), null, null),
        null,
        null,
        feature
    );

    final JSONParseSpec serde = (JSONParseSpec) jsonMapper.readValue(
        jsonMapper.writeValueAsString(spec),
        ParseSpec.class
    );
    Assert.assertEquals("timestamp", serde.getTimestampSpec().getTimestampColumn());
    Assert.assertEquals("iso", serde.getTimestampSpec().getTimestampFormat());

    Assert.assertEquals(Arrays.asList("bar", "foo"), serde.getDimensionsSpec().getDimensionNames());
    Assert.assertEquals(feature, serde.getFeatureSpec());
  }


  @Test
  public void testParseNoExplode()
  {
    final JSONParseSpec parseSpec = new JSONParseSpec(
        new TimestampSpec("timestamp", "iso", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("bar", "foo")), null, null),
        null,
        null,
        null
    );

    final String input = "{\"time\":\"2014-01-01T00:00:10Z\",\"dim\":\"a\",\"dimLong\":2,\"dimFloat\":3.0,\"val\":1}\n";
    final Map<String, Object> explodeMap1 = new HashMap<>();

    explodeMap1.put("time", "2014-01-01T00:00:10Z");
    explodeMap1.put("dim", "a");
    explodeMap1.put("dimLong", (long) 2);
    explodeMap1.put("dimFloat", 3.0);
    explodeMap1.put("val", (long) 1);


    final Parser<String, Object> parser = parseSpec.makeParser();
    final List<Map<String, Object>> parsedRows = parser.parseToMapList(input);

    Assert.assertNotNull(parsedRows);
    Assert.assertTrue(explodeMap1.equals(parsedRows.get(0)));
    final Map<String, Object> parsedRow1 = parsedRows.get(0);

    Assert.assertNull(parsedRow1.get("jq_omg2"));
    Assert.assertNull(parsedRow1.get("path_omg2"));
  }

  @Test
  public void testParseSimpleExplodeNoFlatten()
  {
    final JSONParseSpec parseSpec = new JSONParseSpec(
        new TimestampSpec("timestamp", "iso", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("bar", "foo")), null, null),
        null,
        ImmutableList.of(
            new JSONExplodeSpec("$.bidders", null)
        ),
        null
    );

    final String input = "{\"bidders\":[\"Baron\",\"Caro\",\"Daro\"]}";
    final Map<String, Object> explodeMap1 = new HashMap<>();
    final Map<String, Object> explodeMap2 = new HashMap<>();
    final Map<String, Object> explodeMap3 = new HashMap<>();

    explodeMap1.put("bidders", "Baron");
    explodeMap2.put("bidders", "Caro");
    explodeMap3.put("bidders", "Daro");

    final List<Map<String, Object>> expectedRows = ImmutableList.of(explodeMap1, explodeMap2, explodeMap3);


    final Parser<String, Object> parser = parseSpec.makeParser();
    final List<Map<String, Object>> parsedRows = parser.parseToMapList(input);
    Assert.assertNotNull(parsedRows);

    for (int i = 0; i < parsedRows.size(); i++) {
      Assert.assertTrue(expectedRows.get(i).equals(parsedRows.get(i)));
    }
    Assert.assertTrue(expectedRows.equals(parsedRows));
    final Map<String, Object> parsedRow1 = parsedRows.get(0);
    final Map<String, Object> parsedRow2 = parsedRows.get(1);

    Assert.assertNull(parsedRow1.get("jq_omg2"));
    Assert.assertNull(parsedRow1.get("path_omg2"));
    Assert.assertNull(parsedRow2.get("bar"));
    Assert.assertNull(parsedRow2.get("buzz"));
  }

  @Test
  public void testParseSimpleExplodeFlatten()
  {
    final JSONParseSpec parseSpec = new JSONParseSpec(
        new TimestampSpec("timestamp", "iso", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("bar", "foo")), null, null),
        new JSONPathSpec(
            true,
            ImmutableList.of(
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "bids_bidder", "$.bids.bidder"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "bids_bid", "$.bids.bid"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "bids_valid", "$.bids.valid"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "bids_winner", "$.bids.winner")
            )
        ),
        ImmutableList.of(
            new JSONExplodeSpec("$.bids", null)
        ),
        null
    );

    final String input = "{\"bids\":[{\"bidder\":\"Caroline\",\"bid\":28.00,\"valid\":true,\"winner\":false},{\"bidder\":\"Robert\",\"bid\":30.00,\"valid\":true,\"winner\":true}]}";
    final Map<String, Object> explodeMap1 = new HashMap<>();
    final Map<String, Object> explodeMap2 = new HashMap<>();

    explodeMap1.put("bids_bidder", "Caroline");
    explodeMap1.put("bids_bid", 28.00);
    explodeMap1.put("bids_valid", true);
    explodeMap1.put("bids_winner", false);

    explodeMap2.put("bids_bidder", "Robert");
    explodeMap2.put("bids_bid", 30.00);
    explodeMap2.put("bids_valid", true);
    explodeMap2.put("bids_winner", true);

    final List<Map<String, Object>> expectedRows = ImmutableList.of(explodeMap1, explodeMap2);


    final Parser<String, Object> parser = parseSpec.makeParser();
    final List<Map<String, Object>> parsedRows = parser.parseToMapList(input);
    Assert.assertNotNull(parsedRows);

    for (int i = 0; i < parsedRows.size(); i++) {
      Assert.assertTrue(expectedRows.get(i).equals(parsedRows.get(i)));
    }
    Assert.assertTrue(expectedRows.equals(parsedRows));
    final Map<String, Object> parsedRow1 = parsedRows.get(0);
    final Map<String, Object> parsedRow2 = parsedRows.get(1);

    Assert.assertNull(parsedRow1.get("jq_omg2"));
    Assert.assertNull(parsedRow1.get("path_omg2"));
    Assert.assertNull(parsedRow2.get("bar"));
    Assert.assertNull(parsedRow2.get("buzz"));
  }

  @Test
  public void testParseNestedExplode()
  {
    final JSONParseSpec parseSpec = new JSONParseSpec(
        new TimestampSpec("timestamp", "iso", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("bar", "foo")), null, null),
        new JSONPathSpec(
            true,
            ImmutableList.of(
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "bids_bidder", "$.data.bids.bidder"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "bids_bid", "$.data.bids.bid"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "bids_valid", "$.data.bids.valid"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "bids_winner", "$.data.bids.winner"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "bids_info_bst", "$.data.bids.info.bidStartTime"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "bids_info_round", "$.data.bids.info.round")
            )
        ),
        ImmutableList.of(
            new JSONExplodeSpec("$.data.bids", null),
            new JSONExplodeSpec("$.data.bids.info", null)
        ),
        null
    );

    final String input = "{\"data\":{\"bids\":[{\"bidder\":\"Caroline\",\"bid\":28.00,\"valid\":true,\"winner\":false,\"info\":[{\"bidStartTime\":\"2019-05-08T11:35:00\",\"round\":\"1\"}]},{\"bidder\":\"Robert\",\"bid\":30.00,\"valid\":true,\"winner\":false,\"info\":[{\"bidStartTime\":\"2019-05-08T11:36:00\",\"round\":\"2\"}]}]}}";
    final Map<String, Object> explodeMap1 = new HashMap<>();
    final Map<String, Object> explodeMap2 = new HashMap<>();

    explodeMap1.put("bids_bidder", "Caroline");
    explodeMap1.put("bids_bid", 28.00);
    explodeMap1.put("bids_valid", true);
    explodeMap1.put("bids_winner", false);
    explodeMap1.put("bids_info_bst", "2019-05-08T11:35:00");
    explodeMap1.put("bids_info_round", "1");

    explodeMap2.put("bids_bidder", "Robert");
    explodeMap2.put("bids_bid", 30.00);
    explodeMap2.put("bids_valid", true);
    explodeMap2.put("bids_winner", false);
    explodeMap2.put("bids_info_bst", "2019-05-08T11:36:00");
    explodeMap2.put("bids_info_round", "2");

    final List<Map<String, Object>> expectedRows = ImmutableList.of(explodeMap1, explodeMap2);

    final Parser<String, Object> parser = parseSpec.makeParser();
    final List<Map<String, Object>> parsedRows = parser.parseToMapList(input);

    Assert.assertNotNull(parsedRows);

    for (int i = 0; i < parsedRows.size(); i++) {
      Assert.assertTrue(expectedRows.get(i).equals(parsedRows.get(i)));
    }
    Assert.assertTrue(expectedRows.equals(parsedRows));
    final Map<String, Object> parsedRow1 = parsedRows.get(0);
    final Map<String, Object> parsedRow2 = parsedRows.get(1);

    Assert.assertNull(parsedRow1.get("jq_omg2"));
    Assert.assertNull(parsedRow1.get("path_omg2"));
    Assert.assertNull(parsedRow2.get("bar"));
    Assert.assertNull(parsedRow2.get("buzz"));
  }

  @Test
  public void testParseNestedExplodeImbalaced()
  {
    final JSONParseSpec parseSpec = new JSONParseSpec(
        new TimestampSpec("timestamp", "iso", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("bar", "foo")), null, null),
        new JSONPathSpec(
            true,
            ImmutableList.of(
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "bids_bidder", "$.data.bids.bidder"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "bids_bid", "$.data.bids.bid"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "bids_valid", "$.data.bids.valid"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "bids_winner", "$.data.bids.winner"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "bids_info_bst", "$.data.bids.info.bidStartTime"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "bids_info_round", "$.data.bids.info.round")
            )
        ),
        ImmutableList.of(
            new JSONExplodeSpec("$.data.bids", null),
            new JSONExplodeSpec("$.data.bids.info", null)
        ),
        null
    );
    final String input = "{\"data\":{\"bids\":[{\"bidder\":\"Caroline\",\"bid\":28.00,\"valid\":true,\"winner\":false,"
                         + "\"info\":[{\"bidStartTime\":\"2019-05-08T11:35:00\",\"round\":\"1\"}]},{\"bidder\":"
                         + "\"Robert\",\"bid\":30.00,\"valid\":true,\"winner\":false,\"info\":[{\"bidStartTime\":"
                         + "\"2019-05-08T11:36:00\",\"round\":\"2\"},{\"bidStartTime\":\"2019-05-08T11:36:00\","
                         + "\"round\":\"3\"}]}]}}";
    final Map<String, Object> explodeMap1 = new HashMap<>();
    final Map<String, Object> explodeMap2 = new HashMap<>();
    final Map<String, Object> explodeMap3 = new HashMap<>();

    explodeMap1.put("bids_bidder", "Caroline");
    explodeMap1.put("bids_bid", 28.00);
    explodeMap1.put("bids_valid", true);
    explodeMap1.put("bids_winner", false);
    explodeMap1.put("bids_info_bst", "2019-05-08T11:35:00");
    explodeMap1.put("bids_info_round", "1");

    explodeMap2.put("bids_bidder", "Robert");
    explodeMap2.put("bids_bid", 30.00);
    explodeMap2.put("bids_valid", true);
    explodeMap2.put("bids_winner", false);
    explodeMap2.put("bids_info_bst", "2019-05-08T11:36:00");
    explodeMap2.put("bids_info_round", "2");

    explodeMap3.put("bids_bidder", "Robert");
    explodeMap3.put("bids_bid", 30.00);
    explodeMap3.put("bids_valid", true);
    explodeMap3.put("bids_winner", false);
    explodeMap3.put("bids_info_bst", "2019-05-08T11:36:00");
    explodeMap3.put("bids_info_round", "3");

    final List<Map<String, Object>> expectedRows = ImmutableList.of(explodeMap1, explodeMap2, explodeMap3);

    final Parser<String, Object> parser = parseSpec.makeParser();
    final List<Map<String, Object>> parsedRows = parser.parseToMapList(input);
    Assert.assertNotNull(parsedRows);

    for (int i = 0; i < parsedRows.size(); i++) {
      Assert.assertTrue(expectedRows.get(i).equals(parsedRows.get(i)));
    }
    Assert.assertTrue(expectedRows.equals(parsedRows));
    final Map<String, Object> parsedRow1 = parsedRows.get(0);
    final Map<String, Object> parsedRow2 = parsedRows.get(1);

    Assert.assertNull(parsedRow1.get("jq_omg2"));
    Assert.assertNull(parsedRow1.get("path_omg2"));
    Assert.assertNull(parsedRow2.get("bar"));
    Assert.assertNull(parsedRow2.get("buzz"));
  }
}

