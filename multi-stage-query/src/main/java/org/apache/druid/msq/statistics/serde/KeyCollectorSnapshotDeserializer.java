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

package org.apache.druid.msq.statistics.serde;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.collections.SerializablePair;
import org.apache.druid.frame.key.RowKey;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.statistics.DelegateOrMinKeyCollectorSnapshot;
import org.apache.druid.msq.statistics.DistinctKeySnapshot;
import org.apache.druid.msq.statistics.KeyCollectorSnapshot;
import org.apache.druid.msq.statistics.QuantilesSketchKeyCollectorSnapshot;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class KeyCollectorSnapshotDeserializer
{
  private static final ImmutableMap<Byte, Function<ByteBuffer, KeyCollectorSnapshot>> DESERIALIZERS =
      ImmutableMap.<Byte, Function<ByteBuffer, KeyCollectorSnapshot>>builder()
                  .put(
                      QuantilesSnapshotSerializer.TYPE,
                      QuantilesSnapshotDeserializer::deserialize
                  ).put(
                      DelegateOrMinSerializer.TYPE,
                      DelegateOrMinDeserializer::deserialize
                  ).put(
                      DistinctSnapshotSerializer.TYPE,
                      DistinctSnapshotDeserializer::deserialize
                  )
                  .build();

  public static KeyCollectorSnapshot deserialize(ByteBuffer byteBuffer)
  {
    int position = byteBuffer.position();
    byte type = byteBuffer.get(position);
    byteBuffer.position(position + 1);

    return DESERIALIZERS.get(type).apply(byteBuffer);
  }

  static class DelegateOrMinDeserializer
  {
    private static final int TYPE_OFFSET = 0;
    private static final int ARRAY_LENGTH_OFFSET = TYPE_OFFSET + Byte.BYTES;
    private static final int ARRAY_OFFSET = ARRAY_LENGTH_OFFSET + Integer.BYTES;

    public static KeyCollectorSnapshot deserialize(ByteBuffer byteBuffer)
    {
      int position = byteBuffer.position();
      byte type = byteBuffer.get(position + TYPE_OFFSET);
      int length = byteBuffer.getInt(position + ARRAY_LENGTH_OFFSET);
      final ByteBuffer duplicate = (ByteBuffer) byteBuffer.duplicate()
                                                          .order(byteBuffer.order())
                                                          .position(position + ARRAY_OFFSET)
                                                          .limit(position + ARRAY_OFFSET + length);
      if (type == DelegateOrMinSerializer.ROWKEY_SNAPSHOT) {
        byte[] rowKey = new byte[length];
        duplicate.get(rowKey, 0, length);
        return new DelegateOrMinKeyCollectorSnapshot<>(null, RowKey.wrap(rowKey));
      } else if (type == DelegateOrMinSerializer.SKETCH_SNAPSHOT) {
        return new DelegateOrMinKeyCollectorSnapshot<>(KeyCollectorSnapshotDeserializer.deserialize(duplicate), null);
      } else {
        throw new UnsupportedOperationException();
      }
    }
  }

  static class DistinctSnapshotDeserializer
  {
    private static final int SPACE_REDUCTION_FACTOR_OFFSET = 0;
    private static final int LIST_LENGTH_OFFSET = SPACE_REDUCTION_FACTOR_OFFSET + Integer.BYTES;
    private static final int LIST_OFFSET = LIST_LENGTH_OFFSET + Integer.BYTES;

    private static final int WEIGHT_OFFSET = 0;
    private static final int KEY_LENGTH_OFFSET = WEIGHT_OFFSET + Long.BYTES;
    private static final int KEY_OFFSET = KEY_LENGTH_OFFSET + Integer.BYTES;

    public static KeyCollectorSnapshot deserialize(ByteBuffer byteBuffer)
    {
      int position = byteBuffer.position();
      final int spaceReductionFactor = byteBuffer.getInt(position + SPACE_REDUCTION_FACTOR_OFFSET);
      final int listLength = byteBuffer.getInt(position + LIST_LENGTH_OFFSET);

      List<SerializablePair<RowKey, Long>> keys = new ArrayList<>();
      position = byteBuffer.position(position + LIST_OFFSET).position();
      for (int i = 0; i < listLength; i++) {
        long weight = byteBuffer.getLong(position + WEIGHT_OFFSET);
        int keyLength = byteBuffer.getInt(position + KEY_LENGTH_OFFSET);
        final ByteBuffer duplicate = (ByteBuffer) byteBuffer.duplicate()
                                                            .order(byteBuffer.order())
                                                            .position(position + KEY_OFFSET)
                                                            .limit(position + KEY_OFFSET + keyLength);

        byte[] key = new byte[keyLength];
        duplicate.get(key);
        keys.add(new SerializablePair<>(RowKey.wrap(key), weight));

        position = byteBuffer.position(position + KEY_OFFSET + keyLength).position();
      }

      return new DistinctKeySnapshot(keys, spaceReductionFactor);
    }
  }
  static class QuantilesSnapshotDeserializer
  {
    private static final int AVG_KEY_LENGTH_OFFSET = 0;
    private static final int SKETCH_LENGTH_OFFSET = AVG_KEY_LENGTH_OFFSET + Double.BYTES;
    private static final int SKETCH_OFFSET = SKETCH_LENGTH_OFFSET + Integer.BYTES;

    public static QuantilesSketchKeyCollectorSnapshot deserialize(ByteBuffer byteBuffer)
    {
      int position = byteBuffer.position();
      final double avgKeyLength = byteBuffer.getDouble(position + AVG_KEY_LENGTH_OFFSET);
      final int sketchLength = byteBuffer.getInt(position + SKETCH_LENGTH_OFFSET);
      final byte[] sketchBytes = new byte[sketchLength];
      byteBuffer.position(position + SKETCH_OFFSET);
      byteBuffer.get(sketchBytes);

      final String sketch = StringUtils.encodeBase64String(sketchBytes);
      return new QuantilesSketchKeyCollectorSnapshot(sketch, avgKeyLength);
    }
  }
}
