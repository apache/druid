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

import com.google.common.collect.ImmutableList;
import org.apache.druid.guice.ExtensionsConfig;
import org.apache.druid.java.util.common.ISE;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

public class InitializationTest
{
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test(expected = ISE.class)
  public void testGetHadoopDependencyFilesToLoad_wrong_type_root_hadoop_depenencies_dir() throws IOException
  {
    final File rootHadoopDependenciesDir = temporaryFolder.newFile();
    final ExtensionsConfig config = new ExtensionsConfig()
    {
      @Override
      public String getHadoopDependenciesDir()
      {
        return rootHadoopDependenciesDir.getAbsolutePath();
      }
    };
    Initialization.getHadoopDependencyFilesToLoad(ImmutableList.of(), config);
  }

  @Test(expected = ISE.class)
  public void testGetHadoopDependencyFilesToLoad_non_exist_version_dir() throws IOException
  {
    final File rootHadoopDependenciesDir = temporaryFolder.newFolder();
    final ExtensionsConfig config = new ExtensionsConfig()
    {
      @Override
      public String getHadoopDependenciesDir()
      {
        return rootHadoopDependenciesDir.getAbsolutePath();
      }
    };
    final File hadoopClient = new File(rootHadoopDependenciesDir, "hadoop-client");
    hadoopClient.mkdir();
    Initialization.getHadoopDependencyFilesToLoad(ImmutableList.of("org.apache.hadoop:hadoop-client:2.3.0"), config);
  }


  @Test
  public void testGetHadoopDependencyFilesToLoad_with_hadoop_coordinates() throws IOException
  {
    final File rootHadoopDependenciesDir = temporaryFolder.newFolder();
    final ExtensionsConfig config = new ExtensionsConfig()
    {
      @Override
      public String getHadoopDependenciesDir()
      {
        return rootHadoopDependenciesDir.getAbsolutePath();
      }
    };
    final File hadoopClient = new File(rootHadoopDependenciesDir, "hadoop-client");
    final File versionDir = new File(hadoopClient, "2.3.0");
    hadoopClient.mkdir();
    versionDir.mkdir();
    final File[] expectedFileList = new File[]{versionDir};
    final File[] actualFileList = Initialization.getHadoopDependencyFilesToLoad(
        ImmutableList.of(
            "org.apache.hadoop:hadoop-client:2.3.0"
        ), config
    );
    Assert.assertArrayEquals(expectedFileList, actualFileList);
  }
}
