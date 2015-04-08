/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.segment;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.ConciseBitmapSerdeFactory;
import io.druid.segment.data.RoaringBitmapSerdeFactory;
import org.junit.Assert;
import org.junit.Test;

public class IndexSpecTest
{
  @Test
  public void testConfiguredBitmap() throws Exception
  {
    // this is just to make sure testSerde correctly tests the bitmap type override
    Assert.assertEquals(new ConciseBitmapSerdeFactory(), IndexIO.CONFIGURED_BITMAP_SERDE_FACTORY);
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    final String json = "{ \"bitmap\" : { \"type\" : \"roaring\" }, \"dimensionCompression\" : \"lz4\", \"metricCompression\" : \"lzf\" }";

    final IndexSpec spec = objectMapper.readValue(json, IndexSpec.class);
    Assert.assertEquals(new RoaringBitmapSerdeFactory(), spec.getBitmapSerdeFactory());
    Assert.assertEquals(CompressedObjectStrategy.CompressionStrategy.LZ4, spec.getDimensionCompression());
    Assert.assertEquals(CompressedObjectStrategy.CompressionStrategy.LZF, spec.getMetricCompression());

    Assert.assertEquals(spec, objectMapper.readValue(objectMapper.writeValueAsBytes(spec), IndexSpec.class));
  }

  @Test
  public void testDefaults() throws Exception
  {
    final IndexSpec spec = new IndexSpec();
    Assert.assertEquals(IndexIO.CONFIGURED_BITMAP_SERDE_FACTORY, spec.getBitmapSerdeFactory());
    Assert.assertEquals(null, spec.getDimensionCompression());
    Assert.assertEquals(CompressedObjectStrategy.CompressionStrategy.LZ4, spec.getMetricCompression());
  }
}
