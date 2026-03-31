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

package org.apache.druid.server;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class BuildInfoTest
{
  @Test
  public void testGetBuildRevisionReturnsEmptyStringOutsideJar()
  {
    // During mvn test the class loads from the filesystem, not a JAR, so this must return "".
    Assert.assertEquals("", BuildInfo.getBuildRevision());
  }

  @Test
  public void testReadRevisionFromManifestWithRevisionPresent() throws IOException
  {
    String manifest = "Manifest-Version: 1.0\nBuild-Revision: abc123\n\n";
    InputStream is = new ByteArrayInputStream(manifest.getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals("abc123", BuildInfo.readRevisionFromManifest(is));
  }

  @Test
  public void testReadRevisionFromManifestWithRevisionAbsent() throws IOException
  {
    String manifest = "Manifest-Version: 1.0\n\n";
    InputStream is = new ByteArrayInputStream(manifest.getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals("", BuildInfo.readRevisionFromManifest(is));
  }
}
