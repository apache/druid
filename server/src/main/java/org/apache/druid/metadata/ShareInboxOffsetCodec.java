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

package org.apache.druid.metadata;

import org.apache.druid.indexing.overlord.ShareInboxBatch;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public final class ShareInboxOffsetCodec
{
  private static final int VERSION = 1;

  private ShareInboxOffsetCodec()
  {
  }

  public static byte[] encode(Collection<Long> offsets, int pageSize)
  {
    validatePageSize(pageSize);
    final Map<Long, byte[]> pages = new TreeMap<>();
    for (long offset : offsets) {
      if (offset < 0) {
        throw new IllegalArgumentException("Share inbox offsets must not be negative");
      }
      final long pageStart = pageStart(offset, pageSize);
      final byte[] bitmap = pages.computeIfAbsent(pageStart, ignored -> emptyPage(pageSize));
      set(bitmap, Math.toIntExact(offset - pageStart));
    }

    try {
      final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      final DataOutputStream output = new DataOutputStream(bytes);
      output.writeInt(VERSION);
      output.writeInt(pageSize);
      output.writeInt(pages.size());
      for (Map.Entry<Long, byte[]> page : pages.entrySet()) {
        output.writeLong(page.getKey());
        output.writeInt(page.getValue().length);
        output.write(page.getValue());
      }
      output.flush();
      return bytes.toByteArray();
    }
    catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public static List<Long> decode(byte[] encoded)
  {
    try {
      final DataInputStream input = new DataInputStream(new ByteArrayInputStream(encoded));
      final int version = input.readInt();
      if (version != VERSION) {
        throw new IllegalArgumentException("Unsupported share inbox offset version " + version);
      }
      final int pageSize = input.readInt();
      final int pageCount = input.readInt();
      validatePageSize(pageSize);
      if (pageCount < 0) {
        throw new IllegalArgumentException("Invalid share inbox offset header");
      }
      final List<Long> offsets = new ArrayList<>();
      for (int page = 0; page < pageCount; page++) {
        final long pageStart = input.readLong();
        final int bitmapSize = input.readInt();
        if (bitmapSize != emptyPage(pageSize).length) {
          throw new IllegalArgumentException("Invalid share inbox offset bitmap size " + bitmapSize);
        }
        final byte[] bitmap = new byte[bitmapSize];
        input.readFully(bitmap);
        for (int bit = 0; bit < pageSize; bit++) {
          if (get(bitmap, bit)) {
            offsets.add(pageStart + bit);
          }
        }
      }
      if (input.read() != -1) {
        throw new IllegalArgumentException("Unexpected trailing share inbox offset data");
      }
      return offsets;
    }
    catch (IOException e) {
      throw new IllegalArgumentException("Invalid share inbox offset data", e);
    }
  }

  static byte[] emptyPage(int pageSize)
  {
    validatePageSize(pageSize);
    return new byte[(pageSize + Byte.SIZE - 1) / Byte.SIZE];
  }

  static long pageStart(long offset, int pageSize)
  {
    return Math.floorDiv(offset, pageSize) * (long) pageSize;
  }

  static boolean get(byte[] bitmap, int bit)
  {
    return (bitmap[bit / Byte.SIZE] & (1 << (bit % Byte.SIZE))) != 0;
  }

  static void set(byte[] bitmap, int bit)
  {
    bitmap[bit / Byte.SIZE] |= (byte) (1 << (bit % Byte.SIZE));
  }

  private static void validatePageSize(int pageSize)
  {
    if (pageSize <= 0 || pageSize > ShareInboxBatch.MAX_RECEIPT_PAGE_SIZE) {
      throw new IllegalArgumentException("Invalid share inbox offset page size " + pageSize);
    }
  }
}
