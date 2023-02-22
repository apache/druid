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

import com.google.common.primitives.Ints;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

public class CellIndexReader implements Closeable
{
  private final BlockCompressedPayloadReader payloadReader;

  public CellIndexReader(BlockCompressedPayloadReader payloadReader)
  {
    this.payloadReader = payloadReader;
  }

  @Nonnull
  public PayloadEntrySpan getEntrySpan(int entryNumber)
  {
    int position = entryNumber * Long.BYTES;
    ByteBuffer payload = payloadReader.read(position, 2 * Long.BYTES);
    long payloadValue = payload.getLong();
    long nextPayloadValue = payload.getLong();

    return new PayloadEntrySpan(payloadValue, Ints.checkedCast(nextPayloadValue - payloadValue));
  }

  @Override
  public void close() throws IOException
  {
    payloadReader.close();
  }
}
