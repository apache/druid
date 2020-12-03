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

package org.apache.druid.mapStringString;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.segment.column.ValueType;
import org.junit.Assert;
import org.junit.Test;

public class MapStringStringDimensionSchemaTest
{
  @Test
  public void testSerde() throws Exception
  {
    ObjectMapper jsonMapper = new ObjectMapper();
    jsonMapper.registerModules(new MapStringStringDruidModule().getJacksonModules());

    MapStringStringDimensionSchema schema = new MapStringStringDimensionSchema("test", null);

    MapStringStringDimensionSchema schemaAfterSerde = (MapStringStringDimensionSchema) jsonMapper.readValue(
        jsonMapper.writeValueAsString(schema),
        DimensionSchema.class
    );

    Assert.assertEquals("test", schemaAfterSerde.getName());
    Assert.assertEquals(true, schemaAfterSerde.hasBitmapIndex());
    Assert.assertEquals(MapStringStringDruidModule.TYPE_NAME, schemaAfterSerde.getTypeName());
    Assert.assertEquals(ValueType.COMPLEX, schemaAfterSerde.getValueType());
  }
}
