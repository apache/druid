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

package org.apache.druid.indexer;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Used by ResetCluster to delete the Hadoop Working Path.
 */
@SuppressWarnings("unused")
public class HadoopWorkingDirCleaner
{
  private static final Logger log = new Logger(HadoopWorkingDirCleaner.class);

  public static void runTask(String[] args) throws Exception
  {
    String workingPath = args[0];
    log.info("Deleting indexing hadoop working path [%s].", workingPath);
    Path p = new Path(workingPath);
    try (FileSystem fs = p.getFileSystem(new Configuration())) {
      fs.delete(p, true);
    }
  }
}
