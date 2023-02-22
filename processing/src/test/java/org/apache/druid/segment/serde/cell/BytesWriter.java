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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * This interface is used so that both CellWriter[.Builder] and BlockCompressedPayloadWriter[.Builder] may use the
 * same test code.
 */
public interface BytesWriter extends Closeable
{
  void write(byte[] cellBytes) throws IOException;

  void write(ByteBuffer cellByteBuffer) throws IOException;

  @Override
  void close() throws IOException;

  void transferTo(WritableByteChannel channel) throws IOException;

  long getSerializedSize();
}
