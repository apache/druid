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
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JSONPathSpecTest
{
  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Test
  public void testSerde() throws IOException
  {
    List<JSONPathFieldSpec> fields = new ArrayList<>();
    fields.add(JSONPathFieldSpec.createNestedField("foobar1", "$.foo.bar1"));
    fields.add(JSONPathFieldSpec.createNestedField("baz0", "$.baz[0]"));
    fields.add(JSONPathFieldSpec.createNestedField("hey0barx", "$.hey[0].barx"));
    fields.add(JSONPathFieldSpec.createRootField("timestamp"));
    fields.add(JSONPathFieldSpec.createRootField("foo.bar1"));
    fields.add(JSONPathFieldSpec.createJqField("foobar1", ".foo.bar1"));
    fields.add(JSONPathFieldSpec.createJqField("baz0", ".baz[0]"));
    fields.add(JSONPathFieldSpec.createJqField("hey0barx", ".hey[0].barx"));

    JSONPathSpec flattenSpec = new JSONPathSpec(true, fields);

    final JSONPathSpec serde = jsonMapper.readValue(
        jsonMapper.writeValueAsString(flattenSpec),
        JSONPathSpec.class
    );
    Assert.assertTrue(serde.isUseFieldDiscovery());
    List<JSONPathFieldSpec> serdeFields = serde.getFields();
    JSONPathFieldSpec foobar1 = serdeFields.get(0);
    JSONPathFieldSpec baz0 = serdeFields.get(1);
    JSONPathFieldSpec hey0barx = serdeFields.get(2);
    JSONPathFieldSpec timestamp = serdeFields.get(3);
    JSONPathFieldSpec foodotbar1 = serdeFields.get(4);
    JSONPathFieldSpec jqFoobar1 = serdeFields.get(5);
    JSONPathFieldSpec jqBaz0 = serdeFields.get(6);
    JSONPathFieldSpec jqHey0barx = serdeFields.get(7);

    Assert.assertEquals(JSONPathFieldType.PATH, foobar1.getType());
    Assert.assertEquals("foobar1", foobar1.getName());
    Assert.assertEquals("$.foo.bar1", foobar1.getExpr());

    Assert.assertEquals(JSONPathFieldType.PATH, baz0.getType());
    Assert.assertEquals("baz0", baz0.getName());
    Assert.assertEquals("$.baz[0]", baz0.getExpr());

    Assert.assertEquals(JSONPathFieldType.PATH, hey0barx.getType());
    Assert.assertEquals("hey0barx", hey0barx.getName());
    Assert.assertEquals("$.hey[0].barx", hey0barx.getExpr());

    Assert.assertEquals(JSONPathFieldType.JQ, jqFoobar1.getType());
    Assert.assertEquals("foobar1", jqFoobar1.getName());
    Assert.assertEquals(".foo.bar1", jqFoobar1.getExpr());

    Assert.assertEquals(JSONPathFieldType.JQ, jqBaz0.getType());
    Assert.assertEquals("baz0", jqBaz0.getName());
    Assert.assertEquals(".baz[0]", jqBaz0.getExpr());

    Assert.assertEquals(JSONPathFieldType.JQ, jqHey0barx.getType());
    Assert.assertEquals("hey0barx", jqHey0barx.getName());
    Assert.assertEquals(".hey[0].barx", jqHey0barx.getExpr());

    Assert.assertEquals(JSONPathFieldType.ROOT, timestamp.getType());
    Assert.assertEquals("timestamp", timestamp.getName());
    Assert.assertEquals("timestamp", timestamp.getExpr());

    Assert.assertEquals(JSONPathFieldType.ROOT, foodotbar1.getType());
    Assert.assertEquals("foo.bar1", foodotbar1.getName());
    Assert.assertEquals("foo.bar1", foodotbar1.getExpr());
  }
}
