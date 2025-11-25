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

package org.apache.druid.server.metrics;

/**
 * A TaskHolder implementation for all servers that are not CliPeon.
 *
 * <p>This holder does not provide task information; calls to its methods will
 * throw {@link UnsupportedOperationException}. It serves as a safe default for
 * non-task server processes. Callers should check the implementation type and
 * avoid invoking task-specific methods when using a {@link NoopTaskHolder}.</p>
 */
public class NoopTaskHolder implements TaskHolder
{
  @Override
  public String getDataSource()
  {
    throw new UnsupportedOperationException("getDataSource() should not be called for non-CliPeon");
  }

  @Override
  public String getTaskId()
  {
    throw new UnsupportedOperationException("getTaskId() should not be called for non-CliPeon");
  }
}
