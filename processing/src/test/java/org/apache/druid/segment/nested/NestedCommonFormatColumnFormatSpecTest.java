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

package org.apache.druid.segment.nested;

import com.fasterxml.jackson.core.JsonProcessingException;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.BitmapIndexEncodingStrategy;
import org.apache.druid.segment.column.StringEncodingStrategy;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.ConciseBitmapSerdeFactory;
import org.apache.druid.segment.data.FrontCodedIndexed;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

public class NestedCommonFormatColumnFormatSpecTest
{
  @Test
  public void testSerde() throws JsonProcessingException
  {
    NestedCommonFormatColumnFormatSpec spec =
        NestedCommonFormatColumnFormatSpec.builder()
                                          .setObjectFieldsDictionaryEncoding(
                                              new StringEncodingStrategy.FrontCoded(4, FrontCodedIndexed.V1)
                                          )
                                          .setObjectStorageCompression(CompressionStrategy.ZSTD)
                                          .setStringDictionaryEncoding(
                                              new StringEncodingStrategy.FrontCoded(16, FrontCodedIndexed.V1)
                                          )
                                          .setNumericFieldsBitmapIndexEncoding(BitmapIndexEncodingStrategy.NullValueIndex.INSTANCE)
                                          .build();
    Assert.assertEquals(
        spec,
        TestHelper.JSON_MAPPER.readValue(
            TestHelper.JSON_MAPPER.writeValueAsString(spec),
            NestedCommonFormatColumnFormatSpec.class
        )
    );
  }

  @Test
  public void testGetEffectiveSpecDefaults()
  {
    NestedCommonFormatColumnFormatSpec defaults = NestedCommonFormatColumnFormatSpec.getEffectiveFormatSpec(
        null,
        IndexSpec.getDefault().getEffectiveSpec()
    );

    Assert.assertEquals(
        StringEncodingStrategy.UTF8_STRATEGY,
        defaults.getObjectFieldsDictionaryEncoding()
    );
    Assert.assertEquals(
        ObjectStorageEncoding.SMILE,
        defaults.getObjectStorageEncoding()
    );
    Assert.assertEquals(
        CompressionStrategy.LZ4,
        defaults.getObjectStorageCompression()
    );
    Assert.assertEquals(
        IndexSpec.getDefault().getEffectiveSpec().getDimensionCompression(),
        defaults.getDictionaryEncodedColumnCompression()
    );
    Assert.assertEquals(
        IndexSpec.getDefault().getEffectiveSpec().getStringDictionaryEncoding(),
        defaults.getStringDictionaryEncoding()
    );
    Assert.assertEquals(
        IndexSpec.getDefault().getEffectiveSpec().getMetricCompression(),
        defaults.getLongColumnCompression()
    );
    Assert.assertEquals(
        IndexSpec.getDefault().getEffectiveSpec().getMetricCompression(),
        defaults.getDoubleColumnCompression()
    );
  }

  @Test
  public void testEffectiveSpecIndexSpecOverrides()
  {
    StringEncodingStrategy frontcoded = new StringEncodingStrategy.FrontCoded(4, FrontCodedIndexed.V1);
    NestedCommonFormatColumnFormatSpec defaults = NestedCommonFormatColumnFormatSpec.getEffectiveFormatSpec(
        null,
        IndexSpec.builder()
                 .withAutoColumnFormatSpec(
                     NestedCommonFormatColumnFormatSpec.builder()
                                                       .setObjectFieldsDictionaryEncoding(frontcoded)
                                                       .setObjectStorageEncoding(ObjectStorageEncoding.NONE)
                                                       .build()
                 )
                 .withMetricCompression(CompressionStrategy.LZF)
                 .build()
                 .getEffectiveSpec()
    );

    Assert.assertEquals(
        frontcoded,
        defaults.getObjectFieldsDictionaryEncoding()
    );
    Assert.assertEquals(
        ObjectStorageEncoding.NONE,
        defaults.getObjectStorageEncoding()
    );
    Assert.assertEquals(
        CompressionStrategy.LZ4,
        defaults.getObjectStorageCompression()
    );
    Assert.assertEquals(
        IndexSpec.getDefault().getEffectiveSpec().getDimensionCompression(),
        defaults.getDictionaryEncodedColumnCompression()
    );
    Assert.assertEquals(
        IndexSpec.getDefault().getEffectiveSpec().getStringDictionaryEncoding(),
        defaults.getStringDictionaryEncoding()
    );
    Assert.assertEquals(
        CompressionStrategy.LZF,
        defaults.getLongColumnCompression()
    );
    Assert.assertEquals(
        CompressionStrategy.LZF,
        defaults.getDoubleColumnCompression()
    );
  }

  @Test
  public void testGetEffectiveSpecMerge()
  {
    NestedCommonFormatColumnFormatSpec merged = NestedCommonFormatColumnFormatSpec.getEffectiveFormatSpec(
        NestedCommonFormatColumnFormatSpec.builder()
                                          .setObjectFieldsDictionaryEncoding(
                                              new StringEncodingStrategy.FrontCoded(4, FrontCodedIndexed.V1)
                                          )
                                          .setObjectStorageCompression(CompressionStrategy.ZSTD)
                                          .setStringDictionaryEncoding(
                                              new StringEncodingStrategy.FrontCoded(4, FrontCodedIndexed.V1)
                                          )
                                          .setDoubleColumnCompression(CompressionStrategy.ZSTD)
                                          .build(),
        IndexSpec.getDefault().getEffectiveSpec()
    );

    Assert.assertEquals(
        new StringEncodingStrategy.FrontCoded(4, FrontCodedIndexed.V1),
        merged.getObjectFieldsDictionaryEncoding()
    );
    Assert.assertEquals(
        new StringEncodingStrategy.FrontCoded(4, FrontCodedIndexed.V1),
        merged.getStringDictionaryEncoding()
    );
    Assert.assertEquals(
        ObjectStorageEncoding.SMILE,
        merged.getObjectStorageEncoding()
    );
    Assert.assertEquals(
        IndexSpec.getDefault().getEffectiveSpec().getDimensionCompression(),
        merged.getDictionaryEncodedColumnCompression()
    );
    Assert.assertEquals(
        CompressionStrategy.ZSTD,
        merged.getObjectStorageCompression()
    );
    Assert.assertEquals(
        IndexSpec.getDefault().getEffectiveSpec().getMetricCompression(),
        merged.getLongColumnCompression()
    );
    Assert.assertEquals(
        CompressionStrategy.ZSTD,
        merged.getDoubleColumnCompression()
    );
  }

  @Test
  public void testGetEffectiveSpecInvalid()
  {
    Throwable t = Assert.assertThrows(
        ISE.class,
        () -> NestedCommonFormatColumnFormatSpec.getEffectiveFormatSpec(
            NestedCommonFormatColumnFormatSpec.builder().setBitmapEncoding(new ConciseBitmapSerdeFactory()).build(),
            IndexSpec.builder()
                     .withBitmapSerdeFactory(RoaringBitmapSerdeFactory.getInstance())
                     .build()
                     .getEffectiveSpec()
        )
    );

    Assert.assertEquals(
        "bitmapEncoding[ConciseBitmapSerdeFactory{}] does not match indexSpec.bitmap[RoaringBitmapSerdeFactory{}]",
        t.getMessage()
    );
  }

  @Test
  public void testEqualsAndHashcode()
  {
    EqualsVerifier.forClass(NestedCommonFormatColumnFormatSpec.class).usingGetClass().verify();
  }
}
