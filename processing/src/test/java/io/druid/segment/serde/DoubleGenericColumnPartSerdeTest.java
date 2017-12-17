/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.segment.TestHelper;
import io.druid.segment.data.RoaringBitmapSerdeFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteOrder;

public class DoubleGenericColumnPartSerdeTest
{
  private static ObjectMapper jsonMapper = TestHelper.makeJsonMapper();

  @Test
  public void testSerdeV2() throws IOException
  {
    DoubleGenericColumnPartSerdeV2 object = DoubleGenericColumnPartSerdeV2.getDoubleGenericColumnPartSerde(
        ByteOrder.BIG_ENDIAN,
        new RoaringBitmapSerdeFactory(true)
    );
    ColumnPartSerde columnPartSerde = jsonMapper.readValue(
        jsonMapper.writeValueAsString(object),
        ColumnPartSerde.class
    );
    Assert.assertTrue(columnPartSerde instanceof DoubleGenericColumnPartSerdeV2);
    Assert.assertEquals(
        object.getBitmapSerdeFactory(),
        ((DoubleGenericColumnPartSerdeV2) columnPartSerde).getBitmapSerdeFactory()
    );

  }
}
