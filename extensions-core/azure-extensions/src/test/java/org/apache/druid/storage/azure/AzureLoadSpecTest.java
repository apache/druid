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

package org.apache.druid.storage.azure;

import org.apache.druid.segment.loading.SegmentRangeReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
public class AzureLoadSpecTest
{
  private static final String CONTAINER = "test-container";
  private static final String RAW_BLOB_PATH = "path/to/segment/";
  private static final String ZIP_BLOB_PATH = "path/to/index.zip";

  @Mock
  private AzureByteSourceFactory byteSourceFactory;

  @Mock
  private AzureStorage azureStorage;

  @Test
  public void testOpenRangeReaderReturnsReaderWhenRangeableTrue()
  {
    final SegmentRangeReader reader = newLoadSpec(RAW_BLOB_PATH, true).openRangeReader();
    assertNotNull(reader);
    assertInstanceOf(AzureSegmentRangeReader.class, reader);
    verifyNoInteractions(azureStorage);
  }

  @Test
  public void testOpenRangeReaderReturnsNullForZipBlobPathEvenWhenRangeableTrue()
  {
    // Defensive: a zip blob can't be range-read; the zip check wins over the flag even if hand-crafted input claims
    // the layout is rangeable.
    assertNull(newLoadSpec(ZIP_BLOB_PATH, true).openRangeReader());
    verifyNoInteractions(azureStorage);
  }

  @Test
  public void testOpenRangeReaderReturnsNullWhenRangeableFalse()
  {
    assertNull(newLoadSpec(RAW_BLOB_PATH, false).openRangeReader());
    verifyNoInteractions(azureStorage);
  }

  @Test
  public void testOpenRangeReaderReturnsNullForLegacySegmentWithoutFlag()
  {
    // Legacy segment (pushed before this field existed) → null flag → full-download path.
    assertNull(newLoadSpec(RAW_BLOB_PATH, null).openRangeReader());
    verifyNoInteractions(azureStorage);
  }

  private AzureLoadSpec newLoadSpec(String blobPath, Boolean rangeable)
  {
    return new AzureLoadSpec(
        CONTAINER,
        blobPath,
        rangeable,
        new AzureDataSegmentPuller(byteSourceFactory, azureStorage, new AzureAccountConfig())
    );
  }
}
