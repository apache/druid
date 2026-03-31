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

import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.storage.s3.S3InputDataConfig;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class S3InputSourceFactoryVendedCredentialsTest
{
  private S3InputSourceFactory factory;
  private List<String> testPaths;

  @Before
  public void setUp()
  {
    ServerSideEncryptingAmazonS3.Builder builder =
        EasyMock.createMock(ServerSideEncryptingAmazonS3.Builder.class);
    ServerSideEncryptingAmazonS3 service = EasyMock.createMock(ServerSideEncryptingAmazonS3.class);
    S3InputDataConfig dataConfig = EasyMock.createMock(S3InputDataConfig.class);

    factory = new S3InputSourceFactory(
        service,
        builder,
        dataConfig,
        null,
        null,
        null,
        null,
        null
    );

    testPaths = Arrays.asList("s3://bucket/path/file1.parquet", "s3://bucket/path/file2.parquet");
  }

  @Test
  public void testCreateWithNullCredentials()
  {
    SplittableInputSource result = factory.create(testPaths, null);
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof S3InputSource);
  }

  @Test
  public void testCreateWithEmptyCredentials()
  {
    SplittableInputSource result = factory.create(testPaths, Collections.emptyMap());
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof S3InputSource);
  }

  @Test
  public void testCreateWithS3VendedCredentials()
  {
    Map<String, String> vendedCreds = Map.of(
        "s3.access-key-id", "AKIAIOSFODNN7EXAMPLE",
        "s3.secret-access-key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "s3.session-token", "FwoGZXIvYXdzEBYaDHqa0AP"
    );

    SplittableInputSource result = factory.create(testPaths, vendedCreds);
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof S3InputSource);
  }

  @Test
  public void testCreateWithS3CredentialsWithoutSessionToken()
  {
    Map<String, String> vendedCreds = Map.of(
        "s3.access-key-id", "AKIAIOSFODNN7EXAMPLE",
        "s3.secret-access-key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    );

    SplittableInputSource result = factory.create(testPaths, vendedCreds);
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof S3InputSource);
  }

  @Test
  public void testCreateWithNonS3CredentialsFallsBack()
  {
    Map<String, String> vendedCreds = Map.of(
        "gcs.oauth2.token", "ya29.a0AfH6SMBx"
    );

    SplittableInputSource result = factory.create(testPaths, vendedCreds);
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof S3InputSource);
  }

  @Test
  public void testCreateWithOnlyAccessKeyMissingSecretFallsBack()
  {
    Map<String, String> vendedCreds = new HashMap<>();
    vendedCreds.put("s3.access-key-id", "AKIAIOSFODNN7EXAMPLE");

    SplittableInputSource result = factory.create(testPaths, vendedCreds);
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof S3InputSource);
  }

  @Test
  public void testDefaultCreateDelegatesToVendedVersion()
  {
    SplittableInputSource result1 = factory.create(testPaths);
    SplittableInputSource result2 = factory.create(testPaths, null);
    Assert.assertTrue(result1 instanceof S3InputSource);
    Assert.assertTrue(result2 instanceof S3InputSource);
  }
}
