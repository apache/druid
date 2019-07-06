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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.google.common.io.Files;
import org.apache.druid.common.aws.AWSClientConfig;
import org.apache.druid.common.aws.AWSCredentialsConfig;
import org.apache.druid.common.aws.AWSEndpointConfig;
import org.apache.druid.common.aws.AWSModule;
import org.apache.druid.common.aws.AWSProxyConfig;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

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

    AWSCredentialsProvider provider = awsModule.getAWSCredentialsProvider(config);
    AWSCredentials credentials = provider.getCredentials();
    Assert.assertEquals("accessKeySample", credentials.getAWSAccessKeyId());
    Assert.assertEquals("secretKeySample", credentials.getAWSSecretKey());

    // try to create
    s3Module.getAmazonS3Client(
        provider,
        new AWSProxyConfig(),
        new AWSEndpointConfig(),
        new AWSClientConfig(),
        new S3StorageConfig(new NoopServerSideEncryption())
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

    AWSCredentialsProvider provider = awsModule.getAWSCredentialsProvider(config);
    AWSCredentials credentials = provider.getCredentials();
    Assert.assertTrue(credentials instanceof AWSSessionCredentials);
    AWSSessionCredentials sessionCredentials = (AWSSessionCredentials) credentials;
    Assert.assertEquals("accessKeySample", sessionCredentials.getAWSAccessKeyId());
    Assert.assertEquals("secretKeySample", sessionCredentials.getAWSSecretKey());
    Assert.assertEquals("sessionTokenSample", sessionCredentials.getSessionToken());

    // try to create
    s3Module.getAmazonS3Client(
        provider,
        new AWSProxyConfig(),
        new AWSEndpointConfig(),
        new AWSClientConfig(),
        new S3StorageConfig(new NoopServerSideEncryption())
    );
  }
}
