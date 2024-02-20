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

import com.google.common.collect.ImmutableList;
import org.apache.druid.error.DruidException;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class S3ExportStorageProviderTest
{
  private final List<String> validPrefixes = ImmutableList.of(
      "s3://bucket-name/validPath1",
      "s3://bucket-name/validPath2"
  );

  @Test
  public void testValidatePaths()
  {
    S3ExportStorageProvider.validateS3Prefix(validPrefixes, "bucket-name", "validPath1/");
    S3ExportStorageProvider.validateS3Prefix(validPrefixes, "bucket-name", "validPath1");
    S3ExportStorageProvider.validateS3Prefix(validPrefixes, "bucket-name", "validPath1/validSubPath/");

    S3ExportStorageProvider.validateS3Prefix(ImmutableList.of("s3://bucket-name"), "bucket-name", "");
    S3ExportStorageProvider.validateS3Prefix(ImmutableList.of("s3://bucket-name"), "bucket-name", "validPath");
    S3ExportStorageProvider.validateS3Prefix(validPrefixes, "bucket-name", "validPath1/../validPath2/");

    Assert.assertThrows(
        DruidException.class,
        () -> S3ExportStorageProvider.validateS3Prefix(validPrefixes, "incorrect-bucket", "validPath1/")
    );
    Assert.assertThrows(
        DruidException.class,
        () -> S3ExportStorageProvider.validateS3Prefix(validPrefixes, "bucket-name", "invalidPath1")
    );
    Assert.assertThrows(
        DruidException.class,
        () -> S3ExportStorageProvider.validateS3Prefix(validPrefixes, "bucket-name", "validPath123")
    );
  }
}
