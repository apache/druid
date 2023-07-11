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

package org.apache.druid.segment.serde.cell;

import org.apache.druid.segment.data.CompressionStrategy;

import java.io.IOException;

/**
 * this interface is used so that both CellWriter[.Builder] and BlockCompressedPayload[.Builder] may use the
 * same test code. production code should not use this and use the classes directly
 */

public interface BytesWriterBuilder
{
  BytesWriter build() throws IOException;

  BytesWriterBuilder setCompressionStrategy(CompressionStrategy compressionStrategy);

  BytesWriterBuilder setByteBufferProvider(ByteBufferProvider byteBufferProvider);
}
