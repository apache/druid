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

package org.apache.druid.storage.s3;

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
public class S3LoadSpecTest
{
  private static final String BUCKET = "test-bucket";
  private static final String RAW_KEY = "path/to/segment/";
  private static final String ZIP_KEY = "path/to/index.zip";

  @Mock
  private ServerSideEncryptingAmazonS3 s3Client;

  @Test
  public void testOpenRangeReaderReturnsReaderWhenRangeableTrue()
  {
    final S3LoadSpec spec = new S3LoadSpec(new S3DataSegmentPuller(s3Client), BUCKET, RAW_KEY, true);
    final SegmentRangeReader reader = spec.openRangeReader();
    assertNotNull(reader);
    assertInstanceOf(S3SegmentRangeReader.class, reader);
    verifyNoInteractions(s3Client);
  }

  @Test
  public void testOpenRangeReaderReturnsNullForZipKeyEvenWhenRangeableTrue()
  {
    // Defensive: a zip key can't be range-read; the zip check wins over the flag even if hand-crafted input claims
    // the layout is rangeable.
    final S3LoadSpec spec = new S3LoadSpec(new S3DataSegmentPuller(s3Client), BUCKET, ZIP_KEY, true);
    assertNull(spec.openRangeReader());
    verifyNoInteractions(s3Client);
  }

  @Test
  public void testOpenRangeReaderReturnsNullWhenRangeableFalse()
  {
    final S3LoadSpec spec = new S3LoadSpec(new S3DataSegmentPuller(s3Client), BUCKET, RAW_KEY, false);
    assertNull(spec.openRangeReader());
    verifyNoInteractions(s3Client);
  }

  @Test
  public void testOpenRangeReaderReturnsNullForLegacySegmentWithoutFlag()
  {
    // Legacy segment (pushed before this field existed) → null flag → full-download path.
    final S3LoadSpec spec = new S3LoadSpec(new S3DataSegmentPuller(s3Client), BUCKET, RAW_KEY, null);
    assertNull(spec.openRangeReader());
    verifyNoInteractions(s3Client);
  }
}
