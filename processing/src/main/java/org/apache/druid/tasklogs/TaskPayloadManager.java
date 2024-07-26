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

package org.apache.druid.tasklogs;

import com.google.common.base.Optional;
import org.apache.commons.lang.NotImplementedException;
import org.apache.druid.guice.annotations.ExtensionPoint;
import org.apache.druid.java.util.common.StringUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * Something that knows how to push a task payload before it is run to somewhere
 * a ingestion worker will be able to stream the task payload from when trying to run the task.
 */
@ExtensionPoint
public interface TaskPayloadManager
{
  /**
   * Save payload so it can be retrieved later.
   *
   * @return inputStream for this taskPayload, if available
   */
  default void pushTaskPayload(String taskid, File taskPayloadFile) throws IOException
  {
    throw new NotImplementedException(StringUtils.format("this druid.indexer.logs.type [%s] does not support managing task payloads yet. You will have to switch to using environment variables", getClass()));
  }

  /**
   * Stream payload for a task.
   *
   * @return inputStream for this taskPayload, if available
   */
  default Optional<InputStream> streamTaskPayload(String taskid) throws IOException
  {
    throw new NotImplementedException(StringUtils.format("this druid.indexer.logs.type [%s] does not support managing task payloads yet. You will have to switch to using environment variables", getClass()));
  }
}
