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

package org.apache.druid.k8s.overlord.common;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PeonCommandContext
{

  private final List<String> comamnd;
  private final List<String> javaOpts;
  private final File taskDir;
  private final boolean enableTls;
  private final int cpuMicroCore;

  public PeonCommandContext(List<String> comamnd, List<String> javaOpts, File taskDir, int cpuMicroCore)
  {
    this(comamnd, javaOpts, taskDir, cpuMicroCore, false);
  }

  public PeonCommandContext(
      List<String> comamnd,
      List<String> javaOpts,
      File taskDir,
      int cpuMicroCore,
      boolean enableTls
  )
  {
    this.comamnd = comamnd;
    this.javaOpts = javaOpts;
    this.taskDir = taskDir;
    this.cpuMicroCore = cpuMicroCore;
    this.enableTls = enableTls;
  }

  public List<String> getComamnd()
  {
    return comamnd;
  }

  public List<String> getJavaOpts()
  {
    // we don't know if they put everything in as one string, or split.
    List<String> result = new ArrayList<>();
    for (String javaOpt : javaOpts) {
      String[] value = javaOpt.split("\\s+");
      result.addAll(Arrays.asList(value));
    }
    return result;
  }

  public File getTaskDir()
  {
    return taskDir;
  }

  public int getCpuMicroCore()
  {
    return cpuMicroCore;
  }

  public boolean isEnableTls()
  {
    return enableTls;
  }
}
