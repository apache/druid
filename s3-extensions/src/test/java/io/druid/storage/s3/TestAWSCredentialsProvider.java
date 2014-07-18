/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.storage.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import org.easymock.EasyMock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestAWSCredentialsProvider {
  @Test
  public void testWithFixedAWSKeys() {
    S3StorageDruidModule module = new S3StorageDruidModule();

    AWSCredentialsConfig config = EasyMock.createMock(AWSCredentialsConfig.class);
    EasyMock.expect(config.getAccessKey()).andReturn("accessKeySample").atLeastOnce();
    EasyMock.expect(config.getSecretKey()).andReturn("secretKeySample").atLeastOnce();
    EasyMock.replay(config);

    AWSCredentialsProvider provider = module.getAWSCredentialsProvider(config);
    AWSCredentials credentials = provider.getCredentials();
    assertEquals(credentials.getAWSAccessKeyId(), "accessKeySample");
    assertEquals(credentials.getAWSSecretKey(), "secretKeySample");

    // try to create
    module.getRestS3Service(provider);
  }

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testWithFileSessionCredentials() throws IOException {
    S3StorageDruidModule module = new S3StorageDruidModule();

    AWSCredentialsConfig config = EasyMock.createMock(AWSCredentialsConfig.class);
    EasyMock.expect(config.getAccessKey()).andReturn("");
    EasyMock.expect(config.getSecretKey()).andReturn("");
    File file = folder.newFile();
    PrintWriter out = new PrintWriter(file.getAbsolutePath());
    out.println("sessionToken=sessionTokenSample\nsecretKey=secretKeySample\naccessKey=accessKeySample");
    out.close();
    EasyMock.expect(config.getFileSessionCredentials()).andReturn(file.getAbsolutePath()).atLeastOnce();
    EasyMock.replay(config);

    AWSCredentialsProvider provider = module.getAWSCredentialsProvider(config);
    AWSCredentials credentials = provider.getCredentials();
    assertTrue(credentials instanceof AWSSessionCredentials);
    AWSSessionCredentials sessionCredentials = (AWSSessionCredentials) credentials;
    assertEquals(sessionCredentials.getAWSAccessKeyId(), "accessKeySample");
    assertEquals(sessionCredentials.getAWSSecretKey(), "secretKeySample");
    assertEquals(sessionCredentials.getSessionToken(), "sessionTokenSample");

    // try to create
    module.getRestS3Service(provider);
  }
}
