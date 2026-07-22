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

package org.apache.druid.segment.loading;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayInputStream;
import java.io.IOException;

class PermitLimitedSegmentRangeReaderTest
{
  private static StorageLoadingThreadPool oneThreadPool()
  {
    return StorageLoadingThreadPool.createFromConfig(
        new SegmentLoaderConfig()
        {
          @Override
          public int getVirtualStorageLoadThreads()
          {
            return 1;
          }
        }.setVirtualStorage(true)
    );
  }

  @Test
  @Timeout(30)
  void testPermitIsReleasedWhenTheReturnedStreamIsClosed() throws IOException
  {
    final StorageLoadingThreadPool pool = oneThreadPool();
    try {
      final SegmentRangeReader delegate = (filename, offset, length) -> new ByteArrayInputStream(new byte[0]);
      final PermitLimitedSegmentRangeReader reader = new PermitLimitedSegmentRangeReader(delegate, pool);

      // Acquires the single permit and releases it on close. The second read would hang under @Timeout if the permit
      // were held for longer than the stream's lifetime.
      reader.readRange("file", 0, 1).close();
      reader.readRange("file", 0, 1).close();
    }
    finally {
      pool.stop();
    }
  }

  @Test
  @Timeout(30)
  void testPermitIsReleasedWhenTheDelegateReadFails() throws IOException
  {
    final StorageLoadingThreadPool pool = oneThreadPool();
    try {
      final boolean[] fail = {true};
      final SegmentRangeReader delegate = (filename, offset, length) -> {
        if (fail[0]) {
          throw new IOException("boom");
        }
        return new ByteArrayInputStream(new byte[0]);
      };
      final PermitLimitedSegmentRangeReader reader = new PermitLimitedSegmentRangeReader(delegate, pool);

      // A failed read must release the permit rather than leak it, so a later read still acquires it.
      Assertions.assertThrows(IOException.class, () -> reader.readRange("file", 0, 1));
      fail[0] = false;
      reader.readRange("file", 0, 1).close();
    }
    finally {
      pool.stop();
    }
  }
}
