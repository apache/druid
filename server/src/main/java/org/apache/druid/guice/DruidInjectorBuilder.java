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

package org.apache.druid.guice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.LoadScope;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Druid-enabled injector builder which supports {@link DruidModule}s, module classes
 * created from the base injector, and filtering based on properties and {@link LoadScope}
 * annotations.
 * <p>
 * Can be used in clients and tests, in which case no module filtering is done.
 * Presumably, the test or client has already selected the modules that it needs.
 * <p>
 * Druid injector builders can be chained with an earlier builder providing a set of
 * modules which a later builder overrides. Again, this is typically used only in the
 * server, not in clients or tests.
 */
public class DruidInjectorBuilder
{
  private static final Logger log = new Logger(DruidInjectorBuilder.class);

  private final List<Module> modules = new ArrayList<>();
  protected final Injector baseInjector;
  private final ObjectMapper jsonMapper;
  private final ObjectMapper smileMapper;
  private final Set<NodeRole> nodeRoles;
  private final ModulesConfig modulesConfig;
  private boolean ignoreLoadScopes;

  public DruidInjectorBuilder(final Injector baseInjector)
  {
    this(baseInjector, Collections.emptySet());
  }

  public DruidInjectorBuilder(final Injector baseInjector, final Set<NodeRole> nodeRoles)
  {
    this.baseInjector = baseInjector;
    this.nodeRoles = nodeRoles;
    this.modulesConfig = baseInjector.getInstance(ModulesConfig.class);
    this.jsonMapper = baseInjector.getInstance(Key.get(ObjectMapper.class, Json.class));
    this.smileMapper = baseInjector.getInstance(Key.get(ObjectMapper.class, Smile.class));
  }

  public DruidInjectorBuilder(final DruidInjectorBuilder from)
  {
    this.baseInjector = from.baseInjector;
    this.nodeRoles = from.nodeRoles;
    this.modulesConfig = from.modulesConfig;
    this.jsonMapper = from.jsonMapper;
    this.smileMapper = from.smileMapper;
    this.ignoreLoadScopes = from.ignoreLoadScopes;
  }

  /**
   * Ignore load scope annotations on modules. Primarily for testing where a unit
   * test is not any Druid node, and may wish to load a module that is annotated
   * with a load scope.
   */
  public DruidInjectorBuilder ignoreLoadScopes()
  {
    this.ignoreLoadScopes = true;
    return this;
  }

  /**
   * Add an arbitrary set of modules.
   */
  public DruidInjectorBuilder add(Object...input)
  {
    for (Object o : input) {
      addInput(o);
    }
    return this;
  }

  public DruidInjectorBuilder addModules(Module...inputs)
  {
    for (Object o : inputs) {
      addInput(o);
    }
    return this;
  }

  public DruidInjectorBuilder addAll(Iterable<? extends Object> inputs)
  {
    for (Object o : inputs) {
      addInput(o);
    }
    return this;
  }

  /**
   * Add an arbitrary {@link Module}, {@link DruidModule} instance,
   * or a subclass of these classes. If a class is provided, it is instantiated
   * using the base injector to allow dependency injection. If a module
   * instance is provided, its members are injected. Note that such
   * modules have visibility <i>only</i> to objects defined in the base
   * injector, but not to objects defined in the injector being built.
   */
  public DruidInjectorBuilder addInput(Object input)
  {
    if (input instanceof Module) {
      return addModule((Module) input);
    } else if (input instanceof Class) {
      return addClass((Class<?>) input);
    } else {
      throw new ISE("Unknown module type [%s]", input.getClass());
    }
  }

  public DruidInjectorBuilder addModule(Module module)
  {
    if (!acceptModule(module.getClass())) {
      return this;
    }
    baseInjector.injectMembers(module);
    if (module instanceof DruidModule) {
      registerJacksonModules((DruidModule) module);
    }
    modules.add(module);
    return this;
  }

  public DruidInjectorBuilder addClass(Class<?> input)
  {
    if (!acceptModule(input)) {
      return this;
    }
    if (DruidModule.class.isAssignableFrom(input)) {
      @SuppressWarnings("unchecked")
      DruidModule module = baseInjector.getInstance((Class<? extends DruidModule>) input);
      registerJacksonModules(module);
      modules.add(module);
    } else if (Module.class.isAssignableFrom(input)) {
      @SuppressWarnings("unchecked")
      Module module = baseInjector.getInstance((Class<? extends Module>) input);
      modules.add(module);
    } else {
      throw new ISE("Class [%s] does not implement %s", input, Module.class);
    }
    return this;
  }

  /**
   * Filter module classes based on the (optional) module exclude list and
   * (optional) set of known node roles.
   */
  private boolean acceptModule(Class<?> moduleClass)
  {
    // Modules config is optional: it won't be present in tests or clients.
    String moduleClassName = moduleClass.getName();
    if (moduleClassName != null && modulesConfig.getExcludeList().contains(moduleClassName)) {
      log.info("Not loading module %s because it is present in excludeList", moduleClassName);
      return false;
    }

    // Tests don't have node roles, and so want to load the given modules
    // regardless of the node roles provided.
    if (ignoreLoadScopes) {
      return true;
    }
    LoadScope loadScope = moduleClass.getAnnotation(LoadScope.class);
    if (loadScope == null) {
      // always load if annotation is not specified
      return true;
    }
    Set<NodeRole> rolesPredicate = Arrays.stream(loadScope.roles())
                                         .map(NodeRole::fromJsonName)
                                         .collect(Collectors.toSet());
    return rolesPredicate.stream().anyMatch(nodeRoles::contains);
  }

  private void registerJacksonModules(DruidModule module)
  {
    for (com.fasterxml.jackson.databind.Module jacksonModule : module.getJacksonModules()) {
      jsonMapper.registerModule(jacksonModule);
      smileMapper.registerModule(jacksonModule);
    }
  }

  public List<Module> modules()
  {
    return modules;
  }

  public Injector build()
  {
    return Guice.createInjector(modules);
  }

  public Injector baseInjector()
  {
    return baseInjector;
  }
}
