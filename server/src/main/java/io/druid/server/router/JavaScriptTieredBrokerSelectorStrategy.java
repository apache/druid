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

package io.druid.server.router;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import io.druid.java.util.common.ISE;
import io.druid.js.JavaScriptConfig;
import io.druid.query.Query;

import javax.script.Compilable;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public class JavaScriptTieredBrokerSelectorStrategy implements TieredBrokerSelectorStrategy
{
  public static interface SelectorFunction
  {
    public String apply(TieredBrokerConfig config, Query query);
  }

  private final SelectorFunction fnSelector;
  private final String function;

  @JsonCreator
  public JavaScriptTieredBrokerSelectorStrategy(
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
      ((Compilable)engine).compile("var apply = " + fn).eval();
    } catch(ScriptException e) {
      Throwables.propagate(e);
    }
    this.function = fn;
    this.fnSelector = ((Invocable)engine).getInterface(SelectorFunction.class);
  }

  @Override
  public Optional<String> getBrokerServiceName(
      TieredBrokerConfig config, Query query
  )
  {
    return Optional.fromNullable(fnSelector.apply(config, query));
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

    JavaScriptTieredBrokerSelectorStrategy that = (JavaScriptTieredBrokerSelectorStrategy) o;

    if (!function.equals(that.function)) {
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
    return "JavascriptTieredBrokerSelectorStrategy{" +
           "function='" + function + '\'' +
           '}';
  }
}
