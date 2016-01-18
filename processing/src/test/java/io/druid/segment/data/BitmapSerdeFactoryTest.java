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

package io.druid.segment.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class BitmapSerdeFactoryTest
{
  @Test
  public void testSerialization() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    Assert.assertEquals("{\"type\":\"roaring\"}", mapper.writeValueAsString(new RoaringBitmapSerdeFactory()));
    Assert.assertEquals("{\"type\":\"concise\"}", mapper.writeValueAsString(new ConciseBitmapSerdeFactory()));
    Assert.assertEquals("{\"type\":\"concise\"}", mapper.writeValueAsString(BitmapSerde.createLegacyFactory()));
    Assert.assertEquals("{\"type\":\"concise\"}", mapper.writeValueAsString(new BitmapSerde.DefaultBitmapSerdeFactory()));
    Assert.assertEquals("{\"type\":\"concise\"}", mapper.writeValueAsString(new BitmapSerde.LegacyBitmapSerdeFactory()));
  }

  @Test
  public void testDeserialization() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    Assert.assertTrue(mapper.readValue("{\"type\":\"roaring\"}", BitmapSerdeFactory.class) instanceof RoaringBitmapSerdeFactory);
    Assert.assertTrue(mapper.readValue("{\"type\":\"concise\"}", BitmapSerdeFactory.class) instanceof ConciseBitmapSerdeFactory);
    Assert.assertTrue(mapper.readValue("{\"type\":\"BitmapSerde$SomeRandomClass\"}", BitmapSerdeFactory.class) instanceof ConciseBitmapSerdeFactory);
  }
}
