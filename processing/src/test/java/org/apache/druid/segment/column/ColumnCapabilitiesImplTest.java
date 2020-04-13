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

package org.apache.druid.segment.column;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

public class ColumnCapabilitiesImplTest
{
  private final ObjectMapper mapper = TestHelper.makeJsonMapper();

  @Test
  public void testSerde() throws Exception
  {
    String json = mapper.writeValueAsString(new ColumnCapabilitiesImpl()
                                                .setDictionaryEncoded(true)
                                                .setHasBitmapIndexes(true)
                                                .setHasMultipleValues(true)
                                                .setHasSpatialIndexes(true)
                                                .setType(ValueType.COMPLEX)
                                                .setFilterable(true));

    Assert.assertFalse(json.contains("filterable"));
    
    ColumnCapabilities cc = mapper.readValue(json, ColumnCapabilitiesImpl.class);

    Assert.assertEquals(ValueType.COMPLEX, cc.getType());
    Assert.assertTrue(cc.isDictionaryEncoded());
    Assert.assertFalse(cc.isRunLengthEncoded());
    Assert.assertTrue(cc.hasSpatialIndexes());
    Assert.assertTrue(cc.hasMultipleValues());
    Assert.assertTrue(cc.hasBitmapIndexes());
    Assert.assertFalse(cc.isFilterable());
  }

  @Test
  public void testDeserialization() throws Exception
  {
    String json = "{\n"
                  + "  \"type\":\"COMPLEX\",\n"
                  + "  \"dictionaryEncoded\":true,\n"
                  + "  \"runLengthEncoded\":true,\n"
                  + "  \"hasSpatialIndexes\":true,\n"
                  + "  \"hasMultipleValues\":true,\n"
                  + "  \"hasBitmapIndexes\":true,\n"
                  + "  \"filterable\":true\n"
                  + "}";


    ColumnCapabilities cc = mapper.readValue(json, ColumnCapabilitiesImpl.class);

    Assert.assertEquals(ValueType.COMPLEX, cc.getType());
    Assert.assertTrue(cc.isDictionaryEncoded());
    Assert.assertTrue(cc.isRunLengthEncoded());
    Assert.assertTrue(cc.hasSpatialIndexes());
    Assert.assertTrue(cc.hasMultipleValues());
    Assert.assertTrue(cc.hasBitmapIndexes());
    Assert.assertFalse(cc.isFilterable());
  }
}
