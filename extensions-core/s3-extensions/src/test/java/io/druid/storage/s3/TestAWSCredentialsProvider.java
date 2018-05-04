/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.storage.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.google.common.io.Files;
import io.druid.common.aws.AWSClientConfig;
import io.druid.common.aws.AWSCredentialsConfig;
import io.druid.common.aws.AWSEndpointConfig;
import io.druid.common.aws.AWSProxyConfig;
import io.druid.guice.AWSModule;
import io.druid.metadata.DefaultPasswordProvider;
import org.easymock.EasyMock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
    assertEquals(credentials.getAWSAccessKeyId(), "accessKeySample");
    assertEquals(credentials.getAWSSecretKey(), "secretKeySample");

    // try to create
    s3Module.getAmazonS3Client(provider, new AWSProxyConfig(), new AWSEndpointConfig(), new AWSClientConfig());
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
    assertTrue(credentials instanceof AWSSessionCredentials);
    AWSSessionCredentials sessionCredentials = (AWSSessionCredentials) credentials;
    assertEquals(sessionCredentials.getAWSAccessKeyId(), "accessKeySample");
    assertEquals(sessionCredentials.getAWSSecretKey(), "secretKeySample");
    assertEquals(sessionCredentials.getSessionToken(), "sessionTokenSample");

    // try to create
    s3Module.getAmazonS3Client(provider, new AWSProxyConfig(), new AWSEndpointConfig(), new AWSClientConfig());
  }
}
