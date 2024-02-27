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

package org.apache.druid.data.input.s3;

import org.apache.druid.storage.s3.S3InputDataConfig;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class S3InputSourceFactoryTest
{
  @Test
  public void testAdapterGet()
  {
    ServerSideEncryptingAmazonS3.Builder serverSides3Builder =
        EasyMock.createMock(ServerSideEncryptingAmazonS3.Builder.class);
    ServerSideEncryptingAmazonS3 service = EasyMock.createMock(ServerSideEncryptingAmazonS3.class);
    S3InputDataConfig dataConfig = EasyMock.createMock(S3InputDataConfig.class);
    List<String> fileUris = Arrays.asList(
        "s3://foo/bar/file.csv",
        "s3://bar/foo/file2.csv",
        "s3://bar/foo/file3.txt"
    );

    S3InputSourceFactory s3Builder = new S3InputSourceFactory(
        service,
        serverSides3Builder,
        dataConfig,
        null,
        null,
        null,
        null,
        null
    );
    Assert.assertTrue(s3Builder.create(fileUris) instanceof S3InputSource);
  }
}
