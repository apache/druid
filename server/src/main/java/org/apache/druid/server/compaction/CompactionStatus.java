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

package org.apache.druid.server.compaction;

import org.apache.druid.java.util.common.StringUtils;


/**
 * Represents the status of compaction for a given {@link CompactionCandidate}.
 */
public class CompactionStatus
{
  static final CompactionStatus COMPLETE = new CompactionStatus(State.COMPLETE, null);

  public enum State
  {
    COMPLETE, PENDING, RUNNING, SKIPPED
  }

  private final State state;
  private final String reason;

  private CompactionStatus(State state, String reason)
  {
    this.state = state;
    this.reason = reason;
  }

  public boolean isComplete()
  {
    return state == State.COMPLETE;
  }

  public boolean isSkipped()
  {
    return state == State.SKIPPED;
  }

  public String getReason()
  {
    return reason;
  }

  public State getState()
  {
    return state;
  }

  @Override
  public String toString()
  {
    return "CompactionStatus{" +
           "state=" + state +
           ", reason=" + reason +
           '}';
  }

  public static CompactionStatus pending(String reasonFormat, Object... args)
  {
    return new CompactionStatus(State.PENDING, StringUtils.format(reasonFormat, args));
  }

  public static CompactionStatus skipped(String reasonFormat, Object... args)
  {
    return new CompactionStatus(State.SKIPPED, StringUtils.format(reasonFormat, args));
  }

  public static CompactionStatus running(String message)
  {
    return new CompactionStatus(State.RUNNING, message);
  }
}
