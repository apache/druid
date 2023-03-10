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

package org.apache.druid.msq.statistics;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.google.common.annotations.VisibleForTesting;
import org.apache.datasketches.ArrayOfItemsSerDe;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.java.util.common.StringUtils;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Comparator;

public class QuantilesSketchKeyCollectorFactory
    implements KeyCollectorFactory<QuantilesSketchKeyCollector, QuantilesSketchKeyCollectorSnapshot>
{
  // Maximum value of K possible.
  @VisibleForTesting
  static final int SKETCH_INITIAL_K = 1 << 15;

  private final Comparator<byte[]> comparator;

  private QuantilesSketchKeyCollectorFactory(final Comparator<byte[]> comparator)
  {
    this.comparator = comparator;
  }

  static QuantilesSketchKeyCollectorFactory create(final ClusterBy clusterBy)
  {
    return new QuantilesSketchKeyCollectorFactory(clusterBy.byteKeyComparator());
  }

  @Override
  public QuantilesSketchKeyCollector newKeyCollector()
  {
    return new QuantilesSketchKeyCollector(comparator, ItemsSketch.getInstance(SKETCH_INITIAL_K, comparator), 0);
  }

  @Override
  public JsonDeserializer<QuantilesSketchKeyCollectorSnapshot> snapshotDeserializer()
  {
    return new JsonDeserializer<QuantilesSketchKeyCollectorSnapshot>()
    {
      @Override
      public QuantilesSketchKeyCollectorSnapshot deserialize(JsonParser jp, DeserializationContext ctxt)
          throws IOException
      {
        return jp.readValueAs(QuantilesSketchKeyCollectorSnapshot.class);
      }
    };
  }

  @Override
  public QuantilesSketchKeyCollectorSnapshot toSnapshot(QuantilesSketchKeyCollector collector)
  {
    final String encodedSketch =
        StringUtils.encodeBase64String(collector.getSketch().toByteArray(ByteRowKeySerde.INSTANCE));
    return new QuantilesSketchKeyCollectorSnapshot(encodedSketch, collector.getAverageKeyLength());
  }

  @Override
  public QuantilesSketchKeyCollector fromSnapshot(QuantilesSketchKeyCollectorSnapshot snapshot)
  {
    final String encodedSketch = snapshot.getEncodedSketch();
    final byte[] bytes = StringUtils.decodeBase64String(encodedSketch);
    final ItemsSketch<byte[]> sketch =
        ItemsSketch.getInstance(Memory.wrap(bytes), comparator, ByteRowKeySerde.INSTANCE);
    return new QuantilesSketchKeyCollector(comparator, sketch, snapshot.getAverageKeyLength());
  }

  private static class ByteRowKeySerde extends ArrayOfItemsSerDe<byte[]>
  {
    private static final ByteRowKeySerde INSTANCE = new ByteRowKeySerde();

    private ByteRowKeySerde()
    {
    }

    @Override
    public byte[] serializeToByteArray(final byte[][] items)
    {
      int serializedSize = Integer.BYTES * items.length;

      for (final byte[] key : items) {
        serializedSize += key.length;
      }

      final byte[] serializedBytes = new byte[serializedSize];
      final WritableMemory writableMemory = WritableMemory.writableWrap(serializedBytes, ByteOrder.LITTLE_ENDIAN);
      long keyWritePosition = (long) Integer.BYTES * items.length;

      for (int i = 0; i < items.length; i++) {
        final byte[] keyBytes = items[i];

        writableMemory.putInt((long) Integer.BYTES * i, keyBytes.length);
        writableMemory.putByteArray(keyWritePosition, keyBytes, 0, keyBytes.length);

        keyWritePosition += keyBytes.length;
      }

      assert keyWritePosition == serializedSize;
      return serializedBytes;
    }

    @Override
    public byte[][] deserializeFromMemory(final Memory mem, final int numItems)
    {
      final byte[][] keys = new byte[numItems][];
      long keyPosition = (long) Integer.BYTES * numItems;

      for (int i = 0; i < numItems; i++) {
        final int keyLength = mem.getInt((long) Integer.BYTES * i);
        final byte[] keyBytes = new byte[keyLength];

        mem.getByteArray(keyPosition, keyBytes, 0, keyLength);
        keys[i] = keyBytes;

        keyPosition += keyLength;
      }

      return keys;
    }
  }
}
