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

package org.apache.druid.storage.hdfs;


import com.google.inject.Inject;
import org.apache.druid.guice.Hdfs;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

@ManageLifecycle
public class HdfsStorageAvailabilityChecker
{
  private final Configuration hadoopConf;

  @Inject
  public HdfsStorageAvailabilityChecker(@Hdfs Configuration hadoopConf)
  {
    this.hadoopConf = hadoopConf;
  }

  @LifecycleStart
  public void checkHdfsAvailability()
  {
    try {
      // If cache is enabled, need to check the FileSystem object by access hdfs because FileSystem object will be used later, like in push stage.
      // If FileSystem is invalid, the peon task should stop immediately.
      boolean disableCache = hadoopConf.getBoolean("fs.hdfs.impl.disable.cache", false);
      if (!disableCache) {
        FileSystem fs = FileSystem.get(hadoopConf);
        fs.exists(new Path("/"));
      }
    }
    catch (IOException ex) {
      throw new ISE(ex, "Failed to access hdfs.");
    }
  }

  @LifecycleStop
  public void stop()
  {
    //noop
  }
}
