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

import com.amazonaws.auth.AWSSessionCredentials;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import static org.junit.Assert.assertEquals;

public class TestFileSessionCredentialsProvider {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void test() throws IOException {
    File file = folder.newFile();
    PrintWriter out = new PrintWriter(file.getAbsolutePath());
    out.println("sessionToken=sessionTokenSample\nsecretKey=secretKeySample\naccessKey=accessKeySample");
    out.close();

    FileSessionCredentialsProvider provider = new FileSessionCredentialsProvider(file.getAbsolutePath());
    AWSSessionCredentials sessionCredentials = (AWSSessionCredentials) provider.getCredentials();
    assertEquals(sessionCredentials.getSessionToken(), "sessionTokenSample");
    assertEquals(sessionCredentials.getAWSAccessKeyId(), "accessKeySample");
    assertEquals(sessionCredentials.getAWSSecretKey(), "secretKeySample");
  }
}
