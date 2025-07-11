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

package org.apache.druid.testing.embedded;

import org.apache.druid.java.util.common.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.NginxContainer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestcontainerResourceTest
{
  @Test
  @Timeout(60)
  public void testNginx() throws Exception
  {
    final NginxTestcontainerResource resource =
        new NginxTestcontainerResource(MountableFile.forClasspathResource("/druid-ascii-art.html"));

    resource.start();
    assertTrue(resource.isRunning());

    // Fetch from nginx
    final String url = StringUtils.format("http://%s:%d/", resource.getHost(), resource.getPort());
    final String pageContent = fetchUrl(url);

    // Verify page contains "Apache Druid" text and some asterisks
    assertTrue(pageContent.contains("Apache Druid"));
    assertTrue(pageContent.contains("************"));

    resource.stop();
    assertFalse(resource.isRunning());
  }

  private String fetchUrl(String urlString) throws IOException
  {
    final URL url = new URL(urlString);
    final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("GET");

    try {
      final int responseCode = connection.getResponseCode();
      assertEquals(200, responseCode, "HTTP response status");

      return StringUtils.fromUtf8(connection.getInputStream().readAllBytes());
    }
    finally {
      connection.disconnect();
    }
  }

  /**
   * An {@link NginxContainer} wrapped as an {@link EmbeddedResource}.
   */
  private static class NginxTestcontainerResource extends TestcontainerResource<NginxContainer<?>>
  {
    private final MountableFile indexHtml;

    public NginxTestcontainerResource(final MountableFile indexHtml)
    {
      this.indexHtml = indexHtml;
    }

    @Override
    protected NginxContainer<?> createContainer()
    {
      return new NginxContainer<>("nginx:alpine")
          .withCopyFileToContainer(
              indexHtml,
              "/usr/share/nginx/html/index.html"
          );
    }

    public int getPort()
    {
      return getMappedPort(80);
    }
  }
}
