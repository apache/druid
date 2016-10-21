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

package io.druid.indexing.overlord.setup;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;

import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.ImmutableWorkerInfo;
import io.druid.indexing.overlord.config.WorkerTaskRunnerConfig;
import io.druid.java.util.common.ISE;
import io.druid.js.JavaScriptConfig;

import javax.script.Compilable;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public class JavaScriptWorkerSelectStrategy implements WorkerSelectStrategy
{
  public static interface SelectorFunction
  {
    public String apply(WorkerTaskRunnerConfig config, ImmutableMap<String, ImmutableWorkerInfo> zkWorkers, Task task);
  }

  private final SelectorFunction fnSelector;
  private final String function;

  @JsonCreator
  public JavaScriptWorkerSelectStrategy(
      @JsonProperty("function") String fn,
      @JacksonInject JavaScriptConfig config
  )
  {
    Preconditions.checkNotNull(fn, "function must not be null");

    if (config.isDisabled()) {
      throw new ISE("JavaScript is disabled");
    }

    final ScriptEngine engine = new ScriptEngineManager().getEngineByName("javascript");
    try {
      ((Compilable) engine).compile("var apply = " + fn).eval();
    }
    catch (ScriptException e) {
      throw Throwables.propagate(e);
    }
    this.function = fn;
    this.fnSelector = ((Invocable) engine).getInterface(SelectorFunction.class);
  }

  @Override
  public Optional<ImmutableWorkerInfo> findWorkerForTask(
      WorkerTaskRunnerConfig config, ImmutableMap<String, ImmutableWorkerInfo> zkWorkers, Task task
  )
  {
    String worker = fnSelector.apply(config, zkWorkers, task);
    return Optional.fromNullable(worker == null ? null : zkWorkers.get(worker));
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
