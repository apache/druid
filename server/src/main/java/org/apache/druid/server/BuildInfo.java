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

import org.apache.druid.java.util.common.logger.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.jar.Manifest;

/**
 * Utility class for reading build metadata from the JAR manifest.
 */
public class BuildInfo
{
  private static final Logger log = new Logger(BuildInfo.class);

  private BuildInfo()
  {
    // Utility class; do not instantiate.
  }

  /**
   * Reads the {@code Build-Revision} attribute from the {@code META-INF/MANIFEST.MF} of the JAR
   * that contains this class. Returns an empty string when running outside a packaged JAR
   * (e.g., during {@code mvn test}).
   */
  public static String getBuildRevision()
  {
    try {
      URL classUrl = BuildInfo.class.getResource(BuildInfo.class.getSimpleName() + ".class");
      if (classUrl != null && "jar".equals(classUrl.getProtocol())) {
        String classPath = classUrl.toString();
        String manifestPath = classPath.substring(0, classPath.lastIndexOf('!') + 1) + "/META-INF/MANIFEST.MF";
        try (InputStream is = new URL(manifestPath).openStream()) {
          String revision = new Manifest(is).getMainAttributes().getValue("Build-Revision");
          return revision != null ? revision : "";
        }
      }
    }
    catch (IOException e) {
      log.warn(e, "Failed to read Build-Revision from JAR manifest");
    }
    return "";
  }
}
