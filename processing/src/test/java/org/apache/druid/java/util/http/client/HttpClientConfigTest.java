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


package org.apache.druid.java.util.http.client;

import com.google.common.collect.ImmutableList;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.http.client.HttpClientConfig.CompressionCodec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HttpClientConfigTest
{
  @Test
  public void testDefaultCompressionCodecs()
  {
    final HttpClientConfig config = HttpClientConfig.builder().build();
    Assertions.assertNull(config.getCompressionCodecs());
  }

  @Test
  public void testWithDuplicateCompressionCodecs()
  {
    final HttpClientConfig config = HttpClientConfig.builder()
                                                    .withCompressionCodecs(
                                                        ImmutableList.of(
                                                            CompressionCodec.GZIP,
                                                            CompressionCodec.ZSTD,
                                                            CompressionCodec.GZIP
                                                        )
                                                    )
                                                    .build();
    Assertions.assertEquals(
        ImmutableList.of(CompressionCodec.GZIP, CompressionCodec.ZSTD),
        config.getCompressionCodecs()
    );
  }

  @Test
  public void testNullAndEmptyCompressionCodecs()
  {
    Assertions.assertNull(HttpClientConfig.builder().withCompressionCodec(null).build().getCompressionCodecs());
    Assertions.assertNull(HttpClientConfig.builder().withCompressionCodecs(null).build().getCompressionCodecs());
    Assertions.assertEquals(
        ImmutableList.of(),
        HttpClientConfig.builder().withCompressionCodecs(ImmutableList.of()).build().getCompressionCodecs()
    );
  }

  @Test
  public void testCompressionCodecFromString()
  {
    Assertions.assertEquals(CompressionCodec.GZIP, CompressionCodec.fromString(" GZIP "));
    Assertions.assertEquals(CompressionCodec.ZSTD, CompressionCodec.fromString("zstd"));

    // Unsupported CompressionCodec
    Assertions.assertThrows(DruidException.class, () -> CompressionCodec.fromString("lz4"));
    Assertions.assertThrows(DruidException.class, () -> CompressionCodec.fromString(null));
  }
}
