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

package org.apache.druid.storage.google;

import org.apache.druid.segment.loading.SegmentRangeReader;
import org.easymock.EasyMockSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class GoogleLoadSpecTest extends EasyMockSupport
{
  private static final String BUCKET = "test-bucket";
  private static final String RAW_PATH = "path/to/segment/";
  private static final String ZIP_PATH = "path/to/index.zip";

  private GoogleStorage storage;

  @BeforeEach
  public void setUp()
  {
    storage = createMock(GoogleStorage.class);
    // opening a range reader must not touch storage, so any call fails these tests
    replayAll();
  }

  @Test
  public void testOpenRangeReaderReturnsReaderWhenRangeableTrue()
  {
    final SegmentRangeReader reader = newLoadSpec(RAW_PATH, true).openRangeReader();
    assertNotNull(reader);
    assertInstanceOf(GoogleSegmentRangeReader.class, reader);
    verifyAll();
  }

  @Test
  public void testOpenRangeReaderReturnsNullForZipPathEvenWhenRangeableTrue()
  {
    // Defensive: a zip object can't be range-read; the zip check wins over the flag even if hand-crafted input claims
    // the layout is rangeable.
    assertNull(newLoadSpec(ZIP_PATH, true).openRangeReader());
    verifyAll();
  }

  @Test
  public void testOpenRangeReaderReturnsNullWhenRangeableFalse()
  {
    assertNull(newLoadSpec(RAW_PATH, false).openRangeReader());
    verifyAll();
  }

  @Test
  public void testOpenRangeReaderReturnsNullForLegacySegmentWithoutFlag()
  {
    // Legacy segment (pushed before this field existed) → null flag → full-download path.
    assertNull(newLoadSpec(RAW_PATH, null).openRangeReader());
    verifyAll();
  }

  private GoogleLoadSpec newLoadSpec(String path, @Nullable Boolean rangeable)
  {
    return new GoogleLoadSpec(
        BUCKET,
        path,
        rangeable,
        new GoogleDataSegmentPuller(storage, new GoogleInputDataConfig())
    );
  }
}
