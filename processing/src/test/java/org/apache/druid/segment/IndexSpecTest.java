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

package org.apache.druid.segment;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.junit.Assert;
import org.junit.Test;

public class IndexSpecTest
{
  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    final String json = "{ \"bitmap\" : { \"type\" : \"roaring\" }, \"dimensionCompression\" : \"lz4\", \"metricCompression\" : \"lzf\""
                        + ", \"longEncoding\" : \"auto\" }";

    final IndexSpec spec = objectMapper.readValue(json, IndexSpec.class);
    Assert.assertEquals(new RoaringBitmapSerdeFactory(null), spec.getBitmapSerdeFactory());
    Assert.assertEquals(CompressionStrategy.LZ4, spec.getDimensionCompression());
    Assert.assertEquals(CompressionStrategy.LZF, spec.getMetricCompression());
    Assert.assertEquals(CompressionFactory.LongEncodingStrategy.AUTO, spec.getLongEncoding());

    Assert.assertEquals(spec, objectMapper.readValue(objectMapper.writeValueAsBytes(spec), IndexSpec.class));
  }

  @Test
  public void testSerdeUncompressed() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    final String json = "{ \"dimensionCompression\" : \"uncompressed\" }";

    final IndexSpec spec = objectMapper.readValue(json, IndexSpec.class);

    Assert.assertEquals(CompressionStrategy.UNCOMPRESSED, spec.getDimensionCompression());
    Assert.assertEquals(spec, objectMapper.readValue(objectMapper.writeValueAsBytes(spec), IndexSpec.class));
  }

  @Test
  public void testDefaults()
  {
    final IndexSpec spec = new IndexSpec();
    Assert.assertEquals(CompressionStrategy.LZ4, spec.getDimensionCompression());
    Assert.assertEquals(CompressionStrategy.LZ4, spec.getMetricCompression());
    Assert.assertEquals(CompressionFactory.LongEncodingStrategy.LONGS, spec.getLongEncoding());
  }
}
