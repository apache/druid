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

package org.apache.druid.iceberg.input;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class VendedCredentialsTest
{
  @Test
  public void testExtractFromNull()
  {
    Assert.assertNull(VendedCredentials.extractFrom(null));
  }

  @Test
  public void testExtractFromEmpty()
  {
    Assert.assertNull(VendedCredentials.extractFrom(Collections.emptyMap()));
  }

  @Test
  public void testExtractFromUnrelatedKeys()
  {
    Map<String, String> props = Map.of("foo", "bar", "baz", "qux");
    Assert.assertNull(VendedCredentials.extractFrom(props));
  }

  @Test
  public void testExtractS3Credentials()
  {
    Map<String, String> props = Map.of(
        "s3.access-key-id", "AKIAIOSFODNN7EXAMPLE",
        "s3.secret-access-key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "s3.session-token", "FwoGZXIvYXdzEBYaDHqa0AP"
    );

    VendedCredentials creds = VendedCredentials.extractFrom(props);
    Assert.assertNotNull(creds);
    Assert.assertFalse(creds.isEmpty());

    VendedCredentials.S3Credentials s3 = VendedCredentials.S3Credentials.extract(creds.getRawCredentials());
    Assert.assertNotNull(s3);
    Assert.assertEquals("AKIAIOSFODNN7EXAMPLE", s3.getAccessKeyId());
    Assert.assertEquals("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", s3.getSecretAccessKey());
    Assert.assertEquals("FwoGZXIvYXdzEBYaDHqa0AP", s3.getSessionToken());
  }

  @Test
  public void testExtractS3CredentialsWithoutSessionToken()
  {
    Map<String, String> props = Map.of(
        "s3.access-key-id", "AKIAIOSFODNN7EXAMPLE",
        "s3.secret-access-key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    );

    VendedCredentials creds = VendedCredentials.extractFrom(props);
    Assert.assertNotNull(creds);

    VendedCredentials.S3Credentials s3 = VendedCredentials.S3Credentials.extract(creds.getRawCredentials());
    Assert.assertNotNull(s3);
    Assert.assertNull(s3.getSessionToken());
  }

  @Test
  public void testExtractS3CredentialsMissingSecretKey()
  {
    Map<String, String> props = Map.of("s3.access-key-id", "AKIAIOSFODNN7EXAMPLE");

    VendedCredentials creds = VendedCredentials.extractFrom(props);
    Assert.assertNotNull(creds);

    VendedCredentials.S3Credentials s3 = VendedCredentials.S3Credentials.extract(creds.getRawCredentials());
    Assert.assertNull(s3);
  }

  @Test
  public void testExtractGcsCredentials()
  {
    Map<String, String> props = Map.of(
        "gcs.oauth2.token", "ya29.a0AfH6SMBx",
        "gcs.oauth2.token-expires-at", "1680000000000"
    );

    VendedCredentials creds = VendedCredentials.extractFrom(props);
    Assert.assertNotNull(creds);

    VendedCredentials.GcsCredentials gcs = VendedCredentials.GcsCredentials.extract(creds.getRawCredentials());
    Assert.assertNotNull(gcs);
    Assert.assertEquals("ya29.a0AfH6SMBx", gcs.getOauth2Token());
    Assert.assertEquals("1680000000000", gcs.getExpiresAt());
  }

  @Test
  public void testExtractGcsCredentialsWithoutExpiry()
  {
    Map<String, String> props = Map.of("gcs.oauth2.token", "ya29.a0AfH6SMBx");

    VendedCredentials creds = VendedCredentials.extractFrom(props);
    Assert.assertNotNull(creds);

    VendedCredentials.GcsCredentials gcs = VendedCredentials.GcsCredentials.extract(creds.getRawCredentials());
    Assert.assertNotNull(gcs);
    Assert.assertNull(gcs.getExpiresAt());
  }

  @Test
  public void testExtractAzureCredentials()
  {
    Map<String, String> props = Map.of("adls.sas-token", "sv=2021-06-08&ss=bfqt&sig=xxx");

    VendedCredentials creds = VendedCredentials.extractFrom(props);
    Assert.assertNotNull(creds);

    VendedCredentials.AzureCredentials azure = VendedCredentials.AzureCredentials.extract(creds.getRawCredentials());
    Assert.assertNotNull(azure);
    Assert.assertEquals("sv=2021-06-08&ss=bfqt&sig=xxx", azure.getSasToken());
  }

  @Test
  public void testExtractAzureCredentialsMissing()
  {
    Map<String, String> props = Map.of("s3.access-key-id", "AKIAIOSFODNN7EXAMPLE");

    VendedCredentials creds = VendedCredentials.extractFrom(props);
    Assert.assertNotNull(creds);

    VendedCredentials.AzureCredentials azure = VendedCredentials.AzureCredentials.extract(creds.getRawCredentials());
    Assert.assertNull(azure);
  }

  @Test
  public void testIsEmpty()
  {
    VendedCredentials empty = new VendedCredentials(null);
    Assert.assertTrue(empty.isEmpty());

    VendedCredentials alsoEmpty = new VendedCredentials(new HashMap<>());
    Assert.assertTrue(alsoEmpty.isEmpty());

    VendedCredentials notEmpty = new VendedCredentials(Map.of("s3.access-key-id", "AKIA"));
    Assert.assertFalse(notEmpty.isEmpty());
  }

  @Test
  public void testExtractFromEmptyValues()
  {
    Map<String, String> props = Map.of("s3.access-key-id", "", "s3.secret-access-key", "");
    Assert.assertNull(VendedCredentials.extractFrom(props));
  }
}
