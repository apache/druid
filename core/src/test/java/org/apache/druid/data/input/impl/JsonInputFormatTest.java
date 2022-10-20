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

import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class JsonInputFormatTest
{
  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper mapper = new ObjectMapper();
    final JsonInputFormat format = new JsonInputFormat(
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
        ImmutableMap.of(Feature.ALLOW_COMMENTS.name(), true, Feature.ALLOW_UNQUOTED_FIELD_NAMES.name(), false),
        true,
        false,
        false
    );
    final byte[] bytes = mapper.writeValueAsBytes(format);
    final JsonInputFormat fromJson = (JsonInputFormat) mapper.readValue(bytes, InputFormat.class);
    Assert.assertEquals(format, fromJson);
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(JsonInputFormat.class)
              .usingGetClass()
              .withPrefabValues(
              ObjectMapper.class,
              new ObjectMapper(),
              new ObjectMapper()
              )
              .withIgnoredFields("objectMapper")
              .verify();
  }

  @Test
  public void test_unsetUseFieldDiscovery_unsetKeepNullColumnsByDefault()
  {
    final JsonInputFormat format = new JsonInputFormat(
        new JSONPathSpec(false, null),
        null,
        null,
        null,
        null
    );
    Assert.assertFalse(format.isKeepNullColumns());
  }

  @Test
  public void testUseFieldDiscovery_setKeepNullColumnsByDefault()
  {
    final JsonInputFormat format = new JsonInputFormat(
        new JSONPathSpec(true, null),
        null,
        null,
        null,
        null
    );
    Assert.assertTrue(format.isKeepNullColumns());
  }

  @Test
  public void testUseFieldDiscovery_doNotChangeKeepNullColumnsUserSets()
  {
    final JsonInputFormat format = new JsonInputFormat(
        new JSONPathSpec(true, null),
        null,
        false,
        null,
        null
    );
    Assert.assertFalse(format.isKeepNullColumns());
  }
}
