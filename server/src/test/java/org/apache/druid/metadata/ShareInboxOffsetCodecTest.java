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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

public class ShareInboxOffsetCodecTest
{
  @Test
  public void testRoundTripSparseOffsetsAcrossPages()
  {
    final List<Long> offsets = List.of(0L, 3L, 4_095L, 4_096L, 9_001L);

    Assertions.assertEquals(offsets, ShareInboxOffsetCodec.decode(ShareInboxOffsetCodec.encode(offsets, 4_096)));
  }

  @Test
  public void testDecodeRejectsOversizedPageBeforeAllocation()
  {
    final byte[] encoded = ByteBuffer.allocate(Integer.BYTES * 3)
                                     .putInt(1)
                                     .putInt(ShareInboxBatch.MAX_RECEIPT_PAGE_SIZE + 1)
                                     .putInt(0)
                                     .array();

    Assertions.assertThrows(IllegalArgumentException.class, () -> ShareInboxOffsetCodec.decode(encoded));
  }

  @Test
  public void testDecodeRejectsTrailingData()
  {
    final byte[] valid = ShareInboxOffsetCodec.encode(List.of(1L), 8);
    final byte[] withTrailingData = ByteBuffer.allocate(valid.length + 1).put(valid).put((byte) 1).array();

    Assertions.assertThrows(IllegalArgumentException.class, () -> ShareInboxOffsetCodec.decode(withTrailingData));
  }
}
