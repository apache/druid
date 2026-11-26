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
import org.apache.druid.java.util.http.client.HttpClientConfig.CompressionCodec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HttpClientInitTest
{
  @Test
  public void testToAcceptEncoding()
  {
    Assertions.assertEquals("identity", HttpClientInit.toAcceptEncoding(ImmutableList.of()));
    Assertions.assertEquals("zstd", HttpClientInit.toAcceptEncoding(ImmutableList.of(CompressionCodec.ZSTD)));
    Assertions.assertEquals(
        "zstd, gzip;q=0.5",
        HttpClientInit.toAcceptEncoding(ImmutableList.of(CompressionCodec.ZSTD, CompressionCodec.GZIP))
    );
    Assertions.assertEquals(
        "zstd, gzip;q=0.666, identity;q=0.333",
        HttpClientInit.toAcceptEncoding(
            ImmutableList.of(CompressionCodec.ZSTD, CompressionCodec.GZIP, CompressionCodec.IDENTITY)
        )
    );
  }
}
