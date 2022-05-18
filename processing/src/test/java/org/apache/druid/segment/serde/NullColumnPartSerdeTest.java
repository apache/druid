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

package org.apache.druid.segment.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;

public class NullColumnPartSerdeTest extends InitializedNullHandlingTest
{
  @Test
  public void testSerde() throws JsonProcessingException
  {
    final ObjectMapper mapper = new DefaultObjectMapper();

    final NullColumnPartSerde partSerde = new NullColumnPartSerde(10);
    final String json = mapper.writeValueAsString(partSerde);
    Assert.assertEquals(partSerde, mapper.readValue(json, ColumnPartSerde.class));
  }

  @Test
  public void testDeserializer()
  {
    final NullColumnPartSerde partSerde = new NullColumnPartSerde(10);
    final ColumnBuilder builder = new ColumnBuilder().setType(ValueType.DOUBLE);
    partSerde.getDeserializer().read(Mockito.mock(ByteBuffer.class), builder, Mockito.mock(ColumnConfig.class));
    final ColumnCapabilities columnCapabilities = builder.build().getCapabilities();
    Assert.assertTrue(columnCapabilities.hasNulls().isTrue());
    Assert.assertTrue(columnCapabilities.hasMultipleValues().isFalse());
    Assert.assertTrue(columnCapabilities.isFilterable());
    Assert.assertTrue(columnCapabilities.hasBitmapIndexes());
    Assert.assertTrue(columnCapabilities.isDictionaryEncoded().isTrue());
    Assert.assertTrue(columnCapabilities.areDictionaryValuesSorted().isTrue());
    Assert.assertTrue(columnCapabilities.areDictionaryValuesUnique().isTrue());
  }
}
