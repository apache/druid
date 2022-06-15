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

package org.apache.druid.metadata.catalog;

import org.apache.druid.java.util.common.ISE;

/**
 * Manages catalog data. Used in Coordinator, which will be in either
 * an leader or standby state. The Coordinator calls the {@link #start()}
 * method when it becomes the leader, and calls {@link #stop()} when
 * it loses leadership, or shuts down.
 */
public interface CatalogManager
{
  enum TableState
  {
    ACTIVE("A"),
    DELETING("D");

    private final String code;

    private TableState(String code)
    {
      this.code = code;
    }

    public String code()
    {
      return code;
    }

    public static TableState fromCode(String code)
    {
      for (TableState state : values()) {
        if (state.code.equals(code)) {
          return state;
        }
      }
      throw new ISE("Unknown TableState code: " + code);
    }
  }

  /**
   * Thrown with an "optimistic lock" fails: the version of a
   * catalog object being updated is not the same as that of
   * the expected version.
   */
  class OutOfDateException extends Exception
  {
    public OutOfDateException(String msg)
    {
      super(msg);
    }
  }

  class NotFoundException extends Exception
  {
    public NotFoundException(String msg)
    {
      super(msg);
    }
  }

  /**
   * Indicates an attempt to insert a duplicate key into a table.
   * This could indicate a logic error, or a race condition. It is
   * generally not retryable: it us unrealistic to expect the other
   * thread to helpfully delete the record it just added.
   */
  class DuplicateKeyException extends Exception
  {
    public DuplicateKeyException(String msg, Exception e)
    {
      super(msg, e);
    }
  }

  void start();

  void stop();

  TableDefnManager tables();
}
