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

package org.apache.druid.metadata;

import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.SQLLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * record the execution time of SQL to log file
 */
public class SQLProfileLogger implements SQLLog
{
  static Logger logger = LoggerFactory.getLogger(SQLProfileLogger.class);

  @Override
  public void logBeginTransaction(Handle h)
  {
  }

  @Override
  public void logCommitTransaction(long time, Handle h)
  {
  }

  @Override
  public void logRollbackTransaction(long time, Handle h)
  {
  }

  @Override
  public void logObtainHandle(long time, Handle h)
  {
  }

  @Override
  public void logReleaseHandle(Handle h)
  {
  }

  @Override
  public void logSQL(long time, String sql)
  {
    logger.info("statement:[{}] took {} millis", sql, time);
  }

  @Override
  public void logPreparedBatch(long time, String sql, int count)
  {
  }

  @Override
  public BatchLogger logBatch()
  {
    return new BatchLogger()
    {
      private StringBuilder builder = new StringBuilder("batch:[");
      private boolean added = false;

      @Override
      public void add(String sql)
      {
        added = true;
        builder.append("[").append(sql).append("], ");
      }

      @Override
      public void log(long time)
      {
        if (added) {
          builder.delete(builder.length() - 2, builder.length());
        }
        builder.append("]");
        logger.info("{} took {} millis", builder, time);
      }
    };
  }

  @Override
  public void logCheckpointTransaction(Handle h, String name)
  {
  }

  @Override
  public void logReleaseCheckpointTransaction(Handle h, String name)
  {
  }

  @Override
  public void logRollbackToCheckpoint(long time, Handle h, String checkpointName)
  {
  }
}
