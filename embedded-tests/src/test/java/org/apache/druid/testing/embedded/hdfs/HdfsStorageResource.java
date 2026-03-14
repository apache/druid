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

package org.apache.druid.testing.embedded.hdfs;

import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.storage.hdfs.HdfsStorageDruidModule;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedResource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

/**
 * An {@link EmbeddedResource} that starts an in-process HDFS cluster using {@link MiniDFSCluster}.
 * No Docker container is needed; MiniDFSCluster runs entirely within the test JVM.
 *
 * <p>When {@code configureAsDeepStorage} is {@code true} (the default), this resource also
 * configures the embedded Druid cluster to use HDFS for segment deep storage and task logs.
 * When {@code false}, it only provides HDFS connectivity (useful when HDFS is the input source
 * but a different system is the deep storage).
 */
public class HdfsStorageResource implements EmbeddedResource
{
  private final boolean configureAsDeepStorage;
  private MiniDFSCluster miniDFSCluster;

  /**
   * Creates a resource that configures HDFS as both deep storage and input source.
   */
  public HdfsStorageResource()
  {
    this(true);
  }

  /**
   * Creates a resource with explicit control over whether HDFS is configured as deep storage.
   *
   * @param configureAsDeepStorage if {@code true}, sets {@code druid.storage.type=hdfs} and
   *                               configures task logs on HDFS; if {@code false}, only provides
   *                               HDFS connectivity for use as an input source
   */
  public HdfsStorageResource(boolean configureAsDeepStorage)
  {
    this.configureAsDeepStorage = configureAsDeepStorage;
  }

  @Override
  public void start()
  {
    try {
      final File tempDir = FileUtils.createTempDir("mini-dfs-");
      tempDir.deleteOnExit();

      final Configuration conf = new Configuration();
      // Direct MiniDFSCluster to use our temp directory so we control cleanup.
      conf.set("hadoop.tmp.dir", tempDir.getAbsolutePath());
      conf.set("dfs.replication", "1");
      // Disable permissions checks to simplify test setup
      conf.set("dfs.permissions.enabled", "false");

      miniDFSCluster = new MiniDFSCluster.Builder(conf)
          .nameNodePort(0) // use a random available port
          .build();
      miniDFSCluster.waitClusterUp();
    }
    catch (IOException e) {
      throw new RuntimeException("Failed to start MiniDFSCluster", e);
    }
  }

  @Override
  public void stop()
  {
    if (miniDFSCluster != null) {
      miniDFSCluster.shutdown(true);
      miniDFSCluster = null;
    }
  }

  @Override
  public void onStarted(EmbeddedDruidCluster cluster)
  {
    cluster.addExtension(HdfsStorageDruidModule.class);

    // Set fs.defaultFS so that the Hadoop client and Druid's HdfsStorageDruidModule
    // can connect to this MiniDFSCluster.
    cluster.addCommonProperty("hadoop.fs.defaultFS", getHdfsUrl());

    if (configureAsDeepStorage) {
      cluster.addCommonProperty("druid.storage.type", "hdfs");
      cluster.addCommonProperty("druid.storage.storageDirectory", "/druid/segments");
      cluster.addCommonProperty("druid.indexer.logs.type", "hdfs");
      cluster.addCommonProperty("druid.indexer.logs.directory", "/druid/indexing-logs");
    }
  }

  /**
   * Returns the HDFS NameNode URL, e.g. {@code hdfs://localhost:12345}.
   */
  public String getHdfsUrl()
  {
    ensureRunning();
    return "hdfs://localhost:" + miniDFSCluster.getNameNodePort();
  }

  /**
   * Returns the {@link FileSystem} connected to this MiniDFSCluster, for use in test setup and
   * teardown (uploading data, deleting folders, etc.).
   */
  public FileSystem getFileSystem() throws IOException
  {
    ensureRunning();
    return miniDFSCluster.getFileSystem();
  }

  private void ensureRunning()
  {
    if (miniDFSCluster == null || !miniDFSCluster.isClusterUp()) {
      throw new IllegalStateException("MiniDFSCluster is not running");
    }
  }
}
