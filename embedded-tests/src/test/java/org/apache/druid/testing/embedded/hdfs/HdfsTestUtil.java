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

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.List;

/**
 * Test utility for managing files on HDFS during embedded cluster tests.
 */
public class HdfsTestUtil
{
  private static final Logger LOG = new Logger(HdfsTestUtil.class);

  private final FileSystem fileSystem;
  private final String basePath;

  /**
   * @param fileSystem the HDFS {@link FileSystem} (typically from {@link HdfsStorageResource#getFileSystem()})
   * @param basePath   the HDFS base path under which test files are stored (e.g. {@code /data/json})
   */
  public HdfsTestUtil(FileSystem fileSystem, String basePath)
  {
    this.fileSystem = fileSystem;
    this.basePath = basePath;
  }

  /**
   * Uploads resource-relative files to HDFS under the configured base path.
   *
   * @param localFiles resource-relative paths to upload (e.g. {@code "data/json/tiny_wiki_1.json"})
   */
  public void uploadDataFilesToHdfs(List<String> localFiles) throws Exception
  {
    fileSystem.mkdirs(new Path(basePath));
    for (String localFile : localFiles) {
      final String fileName = localFile.substring(localFile.lastIndexOf('/') + 1);
      final Path hdfsPath = new Path(basePath + "/" + fileName);
      final Path localPath = new Path(Resources.getFileForResource(localFile).getAbsolutePath());
      fileSystem.copyFromLocalFile(localPath, hdfsPath);
      LOG.info("Uploaded [%s] to HDFS at [%s]", localFile, hdfsPath);
    }
  }

  /**
   * Deletes the given bare file names from the configured base path.
   *
   * @param fileNames bare file names (without path prefix) to delete
   */
  public void deleteFilesFromHdfs(List<String> fileNames)
  {
    for (String fileName : fileNames) {
      try {
        fileSystem.delete(new Path(basePath + "/" + fileName), false);
      }
      catch (Exception e) {
        LOG.warn(e, "Unable to delete file [%s] from HDFS", fileName);
      }
    }
  }

}
