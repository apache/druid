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
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.column.StringBitmapIndexType;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.data.CompressionFactory.LongEncodingStrategy;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class IndexSpecTest
{
  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    final String json =
        "{ \"bitmap\" : { \"type\" : \"roaring\" }, \"dimensionCompression\" : \"lz4\", \"metricCompression\" : \"lzf\""
        + ", \"longEncoding\" : \"auto\", \"stringDictionaryEncoding\":{\"type\":\"frontCoded\", \"bucketSize\":16}}";

    final IndexSpec spec = objectMapper.readValue(json, IndexSpec.class);
    Assertions.assertEquals(RoaringBitmapSerdeFactory.getInstance(), spec.getBitmapSerdeFactory());
    Assertions.assertEquals(CompressionStrategy.LZ4, spec.getDimensionCompression());
    Assertions.assertEquals(CompressionStrategy.LZF, spec.getMetricCompression());
    Assertions.assertEquals(CompressionFactory.LongEncodingStrategy.AUTO, spec.getLongEncoding());

    Assertions.assertEquals(spec, objectMapper.readValue(objectMapper.writeValueAsBytes(spec), IndexSpec.class));
  }

  @Test
  public void testSerdeUncompressed() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    final String json = "{ \"dimensionCompression\" : \"uncompressed\" }";

    final IndexSpec spec = objectMapper.readValue(json, IndexSpec.class);

    Assertions.assertEquals(CompressionStrategy.UNCOMPRESSED, spec.getDimensionCompression());
    Assertions.assertEquals(spec, objectMapper.readValue(objectMapper.writeValueAsBytes(spec), IndexSpec.class));
  }

  @Test
  public void testDefaults()
  {
    final IndexSpec spec = IndexSpec.getDefault().getEffectiveSpec();
    Assertions.assertEquals(CompressionStrategy.LZ4, spec.getDimensionCompression());
    Assertions.assertEquals(CompressionStrategy.LZ4, spec.getMetricCompression());
    Assertions.assertEquals(LongEncodingStrategy.LONGS, spec.getLongEncoding());
  }

  @Test
  public void testSerdeWithStringColumnFormatSpec() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    final String json = "{ \"stringColumnFormatSpec\" : { \"maxStringLength\" : 50 } }";

    final IndexSpec spec = objectMapper.readValue(json, IndexSpec.class);
    Assertions.assertNotNull(spec.getStringColumnFormatSpec());
    Assertions.assertEquals(Integer.valueOf(50), spec.getStringColumnFormatSpec().getMaxStringLength());

    Assertions.assertEquals(spec, objectMapper.readValue(objectMapper.writeValueAsBytes(spec), IndexSpec.class));
  }

  @Test
  public void testCompressionLevelsRoundTrip() throws Exception
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    final String json = "{\"dimensionCompression\":\"lz4\",\"dimensionCompressionLevel\":0,"
                        + "\"metricCompression\":\"zstd\",\"metricCompressionLevel\":3}";
    final IndexSpec spec = mapper.readValue(json, IndexSpec.class);

    Assertions.assertEquals(Integer.valueOf(0), spec.getDimensionCompressionLevel());
    Assertions.assertEquals(Integer.valueOf(3), spec.getMetricCompressionLevel());
    Assertions.assertEquals(spec, mapper.readValue(mapper.writeValueAsBytes(spec), IndexSpec.class));
    Assertions.assertEquals(Integer.valueOf(0), spec.getEffectiveSpec().getDimensionCompressionLevel());
    Assertions.assertEquals(Integer.valueOf(3), spec.getEffectiveSpec().getMetricCompressionLevel());
  }

  @Test
  public void testCompressionLevelValidation()
  {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> IndexSpec.builder().withDimensionCompressionLevel(1).build()
    );
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> IndexSpec.builder().withDimensionCompression(CompressionStrategy.LZ4)
                       .withDimensionCompressionLevel(18).build()
    );
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> IndexSpec.builder().withMetricCompression(CompressionStrategy.ZSTD)
                       .withMetricCompressionLevel(-1).build()
    );
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> IndexSpec.builder().withMetricCompression(CompressionStrategy.ZSTD)
                       .withMetricCompressionLevel(23).build()
    );
  }

  @Test
  public void testIgnoredCompressionLevelIsRemovedFromEffectiveSpec() throws Exception
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    final IndexSpec spec = IndexSpec.builder()
                                    .withMetricCompression(CompressionStrategy.UNCOMPRESSED)
                                    .withMetricCompressionLevel(77)
                                    .build();
    final IndexSpec effectiveSpec = spec.getEffectiveSpec();

    Assertions.assertNull(effectiveSpec.getMetricCompressionLevel());
    Assertions.assertFalse(mapper.writeValueAsString(effectiveSpec).contains("metricCompressionLevel"));
  }

  @Test
  public void testUnsetCompressionLevelsDoNotChangeJson() throws Exception
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    final String json = mapper.writeValueAsString(IndexSpec.builder().build());
    Assertions.assertFalse(json.contains("dimensionCompressionLevel"));
    Assertions.assertFalse(json.contains("metricCompressionLevel"));
    Assertions.assertFalse(json.contains("complexMetricCompressionLevel"));
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(IndexSpec.class)
                  .usingGetClass()
                  .withPrefabValues(
                      StringBitmapIndexType.class,
                      StringBitmapIndexType.DictionaryEncodedValueIndex.INSTANCE,
                      StringBitmapIndexType.NoIndex.INSTANCE
                  )
                  .verify();
  }
}
