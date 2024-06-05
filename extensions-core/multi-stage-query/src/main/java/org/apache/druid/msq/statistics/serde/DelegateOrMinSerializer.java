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

import com.google.common.base.Preconditions;
import org.apache.druid.frame.key.RowKey;
import org.apache.druid.msq.statistics.DelegateOrMinKeyCollectorSnapshot;
import org.apache.druid.msq.statistics.KeyCollectorSnapshot;

import java.nio.ByteBuffer;

public class DelegateOrMinSerializer extends KeyCollectorSnapshotSerializer
{
  public static final byte TYPE = (byte) 0;
  public static final byte ROWKEY_SNAPSHOT = (byte) 0;
  public static final byte SKETCH_SNAPSHOT = (byte) 1;

  @Override
  protected byte getType()
  {
    return TYPE;
  }

  @Override
  protected byte[] serializeKeyCollector(KeyCollectorSnapshot snapshot)
  {
    final DelegateOrMinKeyCollectorSnapshot<?> delegateOrMinKeySnapshot = (DelegateOrMinKeyCollectorSnapshot<?>) snapshot;
    final RowKey minKey = delegateOrMinKeySnapshot.getMinKey();
    if (minKey != null) {
      // The sketch contains a minkey, and the sketch is null
      return ByteBuffer.allocate(1 + Integer.BYTES + minKey.array().length)
                       .put(ROWKEY_SNAPSHOT)
                       .putInt(minKey.array().length)
                       .put(minKey.array())
                       .array();
    } else {
      // The sketch contains a delegate sketch, and the minkey is null.
      final KeyCollectorSnapshot delegateSnapshot = Preconditions.checkNotNull((DelegateOrMinKeyCollectorSnapshot<?>) snapshot).getSnapshot();
      byte[] serializedSnapshot = delegateSnapshot.getSerializer().serialize(delegateSnapshot);
      return ByteBuffer.allocate(1 + Integer.BYTES + serializedSnapshot.length)
                       .put(SKETCH_SNAPSHOT)
                       .putInt(serializedSnapshot.length)
                       .put(serializedSnapshot)
                       .array();
    }
  }
}
