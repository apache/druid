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

import com.google.common.io.Files;
import org.apache.druid.common.aws.v2.AWSClientConfig;
import org.apache.druid.common.aws.v2.AWSCredentialsConfig;
import org.apache.druid.common.aws.v2.AWSEndpointConfig;
import org.apache.druid.common.aws.v2.AWSModule;
import org.apache.druid.common.aws.v2.AWSProxyConfig;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class TestAWSCredentialsProvider
{
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private final AWSModule awsModule = new AWSModule();
  private final S3StorageDruidModule s3Module = new S3StorageDruidModule();

  @Test
  public void testWithFixedAWSKeys()
  {
    AWSCredentialsConfig config = EasyMock.createMock(AWSCredentialsConfig.class);
    EasyMock.expect(config.getAccessKey()).andReturn(new DefaultPasswordProvider("accessKeySample")).atLeastOnce();
    EasyMock.expect(config.getSecretKey()).andReturn(new DefaultPasswordProvider("secretKeySample")).atLeastOnce();
    EasyMock.replay(config);

    AwsCredentialsProvider provider = awsModule.getAWSCredentialsProvider(config);
    AwsCredentials credentials = provider.resolveCredentials();
    Assert.assertEquals("accessKeySample", credentials.accessKeyId());
    Assert.assertEquals("secretKeySample", credentials.secretAccessKey());

    // try to create
    ServerSideEncryptingAmazonS3.Builder amazonS3ClientBuilder = s3Module.getServerSideEncryptingAmazonS3Builder(
        provider,
        new AWSProxyConfig(),
        new AWSEndpointConfig(),
        new AWSClientConfig(),
        new S3StorageConfig(new NoopServerSideEncryption(), null)
    );

    s3Module.getAmazonS3Client(
        amazonS3ClientBuilder
    );
  }

  @Test
  public void testWithFileSessionCredentials() throws IOException
  {
    AWSCredentialsConfig config = EasyMock.createMock(AWSCredentialsConfig.class);
    EasyMock.expect(config.getAccessKey()).andReturn(new DefaultPasswordProvider(""));
    EasyMock.expect(config.getSecretKey()).andReturn(new DefaultPasswordProvider(""));
    File file = folder.newFile();
    try (BufferedWriter out = Files.newWriter(file, StandardCharsets.UTF_8)) {
      out.write("sessionToken=sessionTokenSample\nsecretKey=secretKeySample\naccessKey=accessKeySample\n");
    }
    EasyMock.expect(config.getFileSessionCredentials()).andReturn(file.getAbsolutePath()).atLeastOnce();
    EasyMock.replay(config);

    AwsCredentialsProvider provider = awsModule.getAWSCredentialsProvider(config);
    AwsCredentials credentials = provider.resolveCredentials();
    Assert.assertTrue(credentials instanceof AwsSessionCredentials);
    AwsSessionCredentials sessionCredentials = (AwsSessionCredentials) credentials;
    Assert.assertEquals("accessKeySample", sessionCredentials.accessKeyId());
    Assert.assertEquals("secretKeySample", sessionCredentials.secretAccessKey());
    Assert.assertEquals("sessionTokenSample", sessionCredentials.sessionToken());

    // try to create
    ServerSideEncryptingAmazonS3.Builder amazonS3ClientBuilder = s3Module.getServerSideEncryptingAmazonS3Builder(
        provider,
        new AWSProxyConfig(),
        new AWSEndpointConfig(),
        new AWSClientConfig(),
        new S3StorageConfig(new NoopServerSideEncryption(), null)
    );

    s3Module.getAmazonS3Client(
        amazonS3ClientBuilder
    );
  }
}
