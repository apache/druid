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

package org.apache.druid.indexing.worker.executor;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.ISE;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import java.io.File;
import java.io.InputStream;

/**
 */
public class ExecutorLifecycleConfig
{
  @JsonProperty
  @NotNull
  private File taskFile = null;

  @JsonProperty
  @NotNull
  private File statusFile = null;

  @JsonProperty
  @Pattern(regexp = "\\{stdin}")
  private String parentStreamName = "stdin";
  @JsonProperty
  private boolean parentStreamDefined = true;

  /**
   * Should parent stream be monitored.
   */
  public boolean isParentStreamDefined()
  {
    return parentStreamDefined;
  }

  public File getTaskFile()
  {
    return taskFile;
  }

  public ExecutorLifecycleConfig setTaskFile(File taskFile)
  {
    this.taskFile = taskFile;
    return this;
  }

  public File getStatusFile()
  {
    return statusFile;
  }

  public ExecutorLifecycleConfig setStatusFile(File statusFile)
  {
    this.statusFile = statusFile;
    return this;
  }

  public InputStream getParentStream()
  {
    if ("stdin".equals(parentStreamName)) {
      return System.in;
    } else {
      throw new ISE("Unknown stream name[%s]", parentStreamName);
    }
  }
}
