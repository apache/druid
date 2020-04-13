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

package org.apache.druid.indexing.overlord.setup;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.ImmutableWorkerInfo;
import org.apache.druid.indexing.overlord.config.WorkerTaskRunnerConfig;
import org.apache.druid.js.JavaScriptConfig;

import javax.script.Compilable;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public class JavaScriptWorkerSelectStrategy implements WorkerSelectStrategy
{
  public interface SelectorFunction
  {
    String apply(WorkerTaskRunnerConfig config, ImmutableMap<String, ImmutableWorkerInfo> zkWorkers, Task task);
  }

  private final String function;

  // This variable is lazily initialized to avoid unnecessary JavaScript compilation during JSON serde
  private SelectorFunction fnSelector;

  @JsonCreator
  public JavaScriptWorkerSelectStrategy(
      @JsonProperty("function") String fn,
      @JacksonInject JavaScriptConfig config
  )
  {
    Preconditions.checkNotNull(fn, "function must not be null");
    Preconditions.checkState(config.isEnabled(), "JavaScript is disabled");

    this.function = fn;
  }

  private SelectorFunction compileSelectorFunction()
  {
    final ScriptEngine engine = new ScriptEngineManager().getEngineByName("javascript");
    try {
      ((Compilable) engine).compile("var apply = " + function).eval();
      return ((Invocable) engine).getInterface(SelectorFunction.class);
    }
    catch (ScriptException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ImmutableWorkerInfo findWorkerForTask(
      WorkerTaskRunnerConfig config,
      ImmutableMap<String, ImmutableWorkerInfo> zkWorkers,
      Task task
  )
  {
    fnSelector = fnSelector == null ? compileSelectorFunction() : fnSelector;
    String worker = fnSelector.apply(config, zkWorkers, task);
    return worker == null ? null : zkWorkers.get(worker);
  }

  @JsonProperty
  public String getFunction()
  {
    return function;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    JavaScriptWorkerSelectStrategy that = (JavaScriptWorkerSelectStrategy) o;

    if (function != null ? !function.equals(that.function) : that.function != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return function.hashCode();
  }

  @Override
  public String toString()
  {
    return "JavaScriptWorkerSelectStrategy{" +
           "function='" + function + '\'' +
           '}';
  }
}
