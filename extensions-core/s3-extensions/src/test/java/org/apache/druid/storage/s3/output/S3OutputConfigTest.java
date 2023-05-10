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

package org.apache.druid.storage.s3.output;

import org.apache.druid.java.util.common.HumanReadableBytes;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

public class S3OutputConfigTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  private static String BUCKET = "BUCKET";
  private static String PREFIX = "PREFIX";
  private static int MAX_RETRY_COUNT = 0;

  @Test
  public void testTooLargeChunkSize() throws IOException
  {
    long chunkSize = S3OutputConfig.S3_MULTIPART_UPLOAD_MAX_PART_SIZE_BYTES + 1;

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "chunkSize[5368709121] should be >= "
    );
    new S3OutputConfig(
        BUCKET,
        PREFIX,
        temporaryFolder.newFolder(),
        HumanReadableBytes.valueOf(chunkSize),
        MAX_RETRY_COUNT,
        true
    );
  }
}
