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

import org.apache.druid.indexer.lock.DistributedLock;
import org.apache.druid.indexer.lock.ZookeeperDistributedLock;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.dict.AppendTrieDictionary;
import org.apache.kylin.dict.global.AppendTrieDictionaryBuilder;

import java.io.IOException;


/**
 * Modified from the GlobalDictionaryBuilder in https://github.com/apache/kylin
 */
public class GlobalDictionaryBuilder
{
  private AppendTrieDictionaryBuilder builder;

  private DistributedLock lock;
  private String sourceColumn;
  private long counter;

  private static final Logger log = new Logger(GlobalDictionaryBuilder.class);

  public void init(String dataSource, String fieldName, String hdfsDir, String zkHosts, String zkBase)
  {
    sourceColumn = dataSource + "_" + fieldName;
    lock = new ZookeeperDistributedLock.Factory(zkHosts, zkBase).lockForCurrentThread();
    lock.lock(getLockPath(sourceColumn), Long.MAX_VALUE);

    int maxEntriesPerSlice = KylinConfig.getInstanceFromEnv().getAppendDictEntrySize();
    if (hdfsDir == null) {
      //build in Kylin job server
      hdfsDir = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory();
    }

    String baseDir = hdfsDir + "/resources/GlobalDict/dict/" +
                     dataSource + "/" + fieldName + "/";

    try {
      this.builder = new AppendTrieDictionaryBuilder(baseDir, maxEntriesPerSlice, true);
    }
    catch (Throwable e) {
      lock.unlock(getLockPath(sourceColumn));
      throw new RuntimeException(StringUtils.format("Failed to create global dictionary on %s ", sourceColumn), e);
    }
  }

  public boolean addValue(String value)
  {
    if (++counter % 1_000_000 == 0) {
      if (lock.lock(getLockPath(sourceColumn))) {
        log.info("processed %s values for %s", counter, sourceColumn);
      } else {
        throw new RuntimeException("Failed to create global dictionary on " + sourceColumn + " This client doesn't keep the lock");
      }
    }

    if (value == null) {
      return false;
    }

    try {
      builder.addValue(value);
    }
    catch (Throwable e) {
      lock.unlock(getLockPath(sourceColumn));
      throw new RuntimeException(StringUtils.format("Failed to create global dictionary on %s ", sourceColumn), e);
    }

    return true;
  }

  public AppendTrieDictionary<String> build() throws IOException
  {
    try {
      if (lock.lock(getLockPath(sourceColumn))) {
        return builder.build(0);
      } else {
        throw new RuntimeException("Failed to create global dictionary on " + sourceColumn + " This client doesn't keep the lock");
      }
    }
    finally {
      lock.unlock(getLockPath(sourceColumn));
    }
  }

  private String getLockPath(String pathName)
  {
    return "/dict/" + pathName;
  }
}
