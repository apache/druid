/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.indexing.worker.executor;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.common.ISE;

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
  @Pattern(regexp = "\\{stdin\\}")
  private String parentStreamName = "stdin";

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

  public String getParentStreamName()
  {
    return parentStreamName;
  }

  public ExecutorLifecycleConfig setParentStreamName(String parentStreamName)
  {
    this.parentStreamName = parentStreamName;
    return this;
  }

  public InputStream getParentStream()
  {
   if ("stdin".equals(parentStreamName)) {
     return System.in;
   }
   else {
     throw new ISE("Unknown stream name[%s]", parentStreamName);
   }
  }
}
