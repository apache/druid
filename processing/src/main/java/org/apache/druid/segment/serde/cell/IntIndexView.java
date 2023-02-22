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

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;

public class IntIndexView
{
  private final ByteBuffer byteBuffer;
  private final int numberOfEntries;

  public IntIndexView(ByteBuffer byteBuffer)
  {
    this.byteBuffer = byteBuffer;
    numberOfEntries = byteBuffer.remaining() / Integer.BYTES;
  }

  public EntrySpan getEntrySpan(int entryNumber)
  {
    Preconditions.checkArgument(
        entryNumber < numberOfEntries, "invalid entry number %s [%s]", entryNumber, numberOfEntries
    );
    int start = byteBuffer.getInt(byteBuffer.position() + entryNumber * Integer.BYTES);
    int nextStart = byteBuffer.getInt(byteBuffer.position() + ((entryNumber + 1) * Integer.BYTES));

    return new EntrySpan(start, nextStart - start);
  }

  public static class EntrySpan
  {
    private final int start;
    private final int size;

    public EntrySpan(int start, int size)
    {
      this.start = start;
      this.size = size;
    }

    public int getStart()
    {
      return start;
    }

    public int getSize()
    {
      return size;
    }
  }
}
