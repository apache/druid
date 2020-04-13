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
import org.junit.Assert;
import org.junit.Test;

public class DimensionSchemaTest
{
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void testStringDimensionSchemaSerde() throws Exception
  {
    final StringDimensionSchema schema1 = new StringDimensionSchema("foo");
    Assert.assertEquals(
        schema1,
        OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsString(schema1), DimensionSchema.class)
    );

    final StringDimensionSchema schema2 = new StringDimensionSchema(
        "foo",
        DimensionSchema.MultiValueHandling.ARRAY,
        false
    );
    Assert.assertEquals(
        schema2,
        OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsString(schema2), DimensionSchema.class)
    );
  }
}
