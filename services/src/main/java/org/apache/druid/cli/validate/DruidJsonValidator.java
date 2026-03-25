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

package org.apache.druid.cli.validate;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.Injector;
import com.google.inject.name.Names;
import io.netty.util.SuppressForbidden;
import org.apache.commons.io.output.NullWriter;
import org.apache.druid.cli.GuiceRunnable;
import org.apache.druid.guice.DruidProcessingModule;
import org.apache.druid.guice.ExtensionsLoader;
import org.apache.druid.guice.IndexingServiceInputSourceModule;
import org.apache.druid.guice.LocalDataStorageDruidModule;
import org.apache.druid.guice.QueryRunnerFactoryModule;
import org.apache.druid.guice.QueryableModule;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.Query;

import java.io.File;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 */
@Command(
    name = "validator",
    description = "Validates that a given Druid JSON object is correctly formatted"
)
@SuppressForbidden(reason = "System#out")
public class DruidJsonValidator extends GuiceRunnable
{
  private static final Logger LOG = new Logger(DruidJsonValidator.class);
  private Writer logWriter = new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8));

  @Option(name = "-f", title = "file", description = "file to validate")
  @Required
  public String jsonFile;

  @Option(name = "-t", title = "type", description = "the type of schema to validate")
  @Required
  public String type;

  @Option(name = "-r", title = "resource", description = "optional resources required for validation")
  public String resource;

  @Option(name = "--log", title = "toLogger", description = "redirects any outputs to logger")
  public boolean toLogger;

  public DruidJsonValidator()
  {
    super(LOG);
  }

  @Override
  protected List<? extends com.google.inject.Module> getModules()
  {
    return ImmutableList.of(
        // It's unknown if those modules are required in DruidJsonValidator.
        // Maybe some of those modules could be removed.
        // See https://github.com/apache/druid/pull/4429#discussion_r123603498
        new DruidProcessingModule(),
        new QueryableModule(),
        new QueryRunnerFactoryModule(),
        binder -> {
          binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/validator");
          binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
          binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);
        }
    );
  }

  @Override
  public void run()
  {
    File file = new File(jsonFile);
    if (!file.exists()) {
      LOG.info("File[%s] does not exist.%n", file);
    }

    final Injector injector = makeInjector();
    final ObjectMapper jsonMapper = injector.getInstance(ObjectMapper.class);
    ExtensionsLoader extnLoader = injector.getInstance(ExtensionsLoader.class);

    registerModules(
        jsonMapper,
        Iterables.concat(
            extnLoader.getModules(),
            Arrays.asList(
                new IndexingServiceInputSourceModule(),
                new LocalDataStorageDruidModule()
            )
        )
    );

    final ClassLoader loader;
    if (Thread.currentThread().getContextClassLoader() != null) {
      loader = Thread.currentThread().getContextClassLoader();
    } else {
      loader = DruidJsonValidator.class.getClassLoader();
    }

    if (toLogger) {
      logWriter = new NullWriter()
      {
        private final Logger logger = new Logger(DruidJsonValidator.class);

        @Override
        public void write(char[] cbuf, int off, int len)
        {
          logger.info(new String(cbuf, off, len));
        }
      };
    }

    try {
      if ("query".equalsIgnoreCase(type)) {
        jsonMapper.readValue(file, Query.class);
      } else if ("task".equalsIgnoreCase(type)) {
        jsonMapper.readValue(file, Task.class);
      } else {
        throw new UOE("Unknown type[%s]", type);
      }
    }
    catch (Exception e) {
      LOG.error(e, "INVALID JSON!");
      Throwables.propagateIfPossible(e);
      throw new RuntimeException(e);
    }
  }

  private void registerModules(ObjectMapper jsonMapper, Iterable<DruidModule> fromExtensions)
  {
    for (DruidModule druidModule : fromExtensions) {
      for (Module module : druidModule.getJacksonModules()) {
        jsonMapper.registerModule(module);
      }
    }
  }

  @VisibleForTesting
  void setLogWriter(Writer writer)
  {
    this.logWriter = writer;
  }

}
