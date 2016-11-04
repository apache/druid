/*
* Licensed to Metamarkets Group Inc. (Metamarkets) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. Metamarkets licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package io.druid.tasklogs;

import com.google.common.base.Optional;
import com.google.common.io.ByteSource;

import java.io.IOException;

/**
 * Something that knows how to stream logs for tasks.
 */
public interface TaskLogStreamer
{
  /**
   * Stream log for a task.
   *
   * @param offset If zero, stream the entire log. If positive, attempt to read from this position onwards. If
   *               negative, attempt to read this many bytes from the end of the file (like <tt>tail -n</tt>).
   *
   * @return input supplier for this log, if available from this provider
   */
  public Optional<ByteSource> streamTaskLog(String taskid, long offset) throws IOException;
}
