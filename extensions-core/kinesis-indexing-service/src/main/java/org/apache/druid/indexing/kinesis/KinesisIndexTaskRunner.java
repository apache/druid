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

package org.apache.druid.indexing.kinesis;

import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.stats.RowIngestionMeters;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.realtime.appenderator.Appenderator;

import javax.ws.rs.core.Response;
import java.util.Map;

/**
 * Kinesis indexing task runner supporting incremental segments publishing
 */
public class KinesisIndexTaskRunner implements SeekableStreamIndexTaskRunner<String, String>
{
  private static final EmittingLogger log = new EmittingLogger(KinesisIndexTask.class);



  @Override
  public Appenderator getAppenderator()
  {
    return null;
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox)
  {
    return null;
  }

  @Override
  public void stopGracefully()
  {

  }

  @Override
  public RowIngestionMeters getRowIngestionMeters()
  {
    return null;
  }

  @Override
  public SeekableStreamIndexTask.Status getStatus()
  {
    return null;
  }

  @Override
  public Map<String, String> getCurrentOffsets()
  {
    return null;
  }

  @Override
  public Map<String, String> getEndOffsets()
  {
    return null;
  }

  @Override
  public Response setEndOffsets(Map<String, String> offsets, boolean finish) throws InterruptedException
  {
    return null;
  }

  @Override
  public Response pause() throws InterruptedException
  {
    return null;
  }

  @Override
  public void resume() throws InterruptedException
  {

  }
}
