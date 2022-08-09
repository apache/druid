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

package org.apache.druid.indexing.common.task;

import org.apache.druid.guice.ExtensionsConfig;
import org.apache.druid.java.util.common.ISE;
import org.eclipse.aether.artifact.DefaultArtifact;

import java.io.File;
import java.util.List;

public class Initialization
{
  /**
   * Find all the Hadoop dependencies that should be loaded by Druid.
   *
   * @param hadoopDependencyCoordinates e.g.["org.apache.hadoop:hadoop-client:2.3.0"]
   * @param extensionsConfig            ExtensionsConfig configured by druid.extensions.xxx
   *
   * @return an array of Hadoop dependency files that will be loaded by the Druid process.
   */
  public static File[] getHadoopDependencyFilesToLoad(
      List<String> hadoopDependencyCoordinates,
      ExtensionsConfig extensionsConfig
  )
  {
    final File rootHadoopDependenciesDir = new File(extensionsConfig.getHadoopDependenciesDir());
    if (rootHadoopDependenciesDir.exists() && !rootHadoopDependenciesDir.isDirectory()) {
      throw new ISE("Root Hadoop dependencies directory [%s] is not a directory!?", rootHadoopDependenciesDir);
    }
    final File[] hadoopDependenciesToLoad = new File[hadoopDependencyCoordinates.size()];
    int i = 0;
    for (final String coordinate : hadoopDependencyCoordinates) {
      final DefaultArtifact artifact = new DefaultArtifact(coordinate);
      final File hadoopDependencyDir = new File(rootHadoopDependenciesDir, artifact.getArtifactId());
      final File versionDir = new File(hadoopDependencyDir, artifact.getVersion());
      // find the hadoop dependency with the version specified in coordinate
      if (!hadoopDependencyDir.isDirectory() || !versionDir.isDirectory()) {
        throw new ISE("Hadoop dependency [%s] didn't exist!?", versionDir.getAbsolutePath());
      }
      hadoopDependenciesToLoad[i++] = versionDir;
    }
    return hadoopDependenciesToLoad;
  }
}
