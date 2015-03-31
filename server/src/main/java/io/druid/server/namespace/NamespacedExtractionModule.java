/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.server.namespace;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.metamx.common.IAE;
import com.metamx.common.RetryUtils;
import com.metamx.common.StringUtils;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.Json;
import io.druid.initialization.DruidModule;
import io.druid.query.extraction.namespace.ExtractionNamespace;
import io.druid.query.extraction.namespace.ExtractionNamespaceFunctionFactory;
import io.druid.query.extraction.namespace.ExtractionNamespaceUpdate;
import io.druid.query.extraction.namespace.JDBCExtractionNamespace;
import io.druid.query.extraction.namespace.URIExtractionNamespace;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.server.namespace.cache.NamespaceExtractionCacheManager;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class NamespacedExtractionModule implements DruidModule
{
  private static final Logger log = new Logger(NamespacedExtractionModule.class);
  private static final String TYPE_PREFIX = "druid.query.extraction.namespace.cache";
  private static final String ENABLE_NAMESPACES = "druid.query.extraction.namespace";
  private final ConcurrentMap<String, Function<String, String>> fnCache = new ConcurrentHashMap<>();

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.<Module>of(
        new Module()
        {
          @Override
          public String getModuleName()
          {
            return "DruidNamespacedExtractionModule";
          }

          @Override
          public Version version()
          {
            return Version.unknownVersion();
          }

          @Override
          public void setupModule(SetupContext context)
          {
            context.registerSubtypes(ExtractionNamespaceUpdate.class);
            context.registerSubtypes(ExtractionNamespace.class);
          }
        }
    );
  }

  public static class NamespacedExtractionModuleConfig
  {
    @JsonCreator
    public NamespacedExtractionModuleConfig(
        @JsonProperty("enabled") Boolean enabled
    )
    {
      this.enabled = enabled == null ? false : enabled;
    }

    @JsonProperty
    final Boolean enabled;
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, TYPE_PREFIX, NamespaceExtractionCacheManager.class);
    JsonConfigProvider.bind(binder, ENABLE_NAMESPACES, NamespacedExtractionModuleConfig.class);
    binder
        .bind(ExtractionNamespaceFunctionFactory.class)
        .annotatedWith(Names.named(JDBCExtractionNamespace.class.getCanonicalName()))
        .to(JDBCExtractionNamespaceFunctionFactory.class)
        .in(LazySingleton.class);
    binder
        .bind(ExtractionNamespaceFunctionFactory.class)
        .annotatedWith(Names.named(URIExtractionNamespace.class.getCanonicalName()))
        .to(URIExtractionNamespaceFunctionFactory.class)
        .in(LazySingleton.class);

    binder.bind(NamespacedKeeper.class).in(ManageLifecycle.class);
    LifecycleModule.register(binder, NamespacedKeeper.class);
  }

  private static final Function<String, String> NOOP_FUNCTION = new Function<String, String>()
  {
    @Nullable
    @Override
    public String apply(@Nullable String input)
    {
      return Strings.isNullOrEmpty(input) ? null : input;
    }
  };

  @Provides
  @Named("io.druid.server.namespace.NamespacedExtractionModule")
  public ConcurrentMap<String, Function<String, String>> getFnCache()
  {
    return fnCache;
  }

  @Provides
  @Named("dimExtractionNamespace")
  @LazySingleton
  public Function<String, Function<String, String>> getFunctionMaker(
      @Named("io.druid.server.namespace.NamespacedExtractionModule")
      final ConcurrentMap<String, Function<String, String>> fnCache,
      final NamespacedExtractionModuleConfig config
  )
  {
    return new Function<String, Function<String, String>>()
    {
      @Nullable
      @Override
      public Function<String, String> apply(final String namespace)
      {
        if (config.enabled == null || !config.enabled.booleanValue()) {
          return NOOP_FUNCTION;
        }
        Function<String, String> fn = fnCache.get(namespace);
        if (fn == null) {
          throw new IAE("Namespace [%s] not found", namespace);
        }
        return fn;
      }
    };
  }

  @ManageLifecycle
  public static class NamespacedKeeper
  {
    private final CuratorFramework curator;
    private final ZkPathsConfig zkPathsConfig;
    private final ListeningExecutorService listeningExecutorService = MoreExecutors.listeningDecorator(
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("NamespaceKeeper-%d")
                .setPriority(Thread.MIN_PRIORITY)
                .build()
        )
    );
    private final Injector injector;
    private final NamespaceExtractionCacheManager namespaceExtractionCacheManager;
    private final ObjectMapper jsonMapper;
    private final ConcurrentMap<String, Function<String, String>> fnCache;
    private final Boolean enableNamespaces;

    @Inject
    public NamespacedKeeper(
        CuratorFramework curator,
        ZkPathsConfig zkPathsConfig,
        @Json ObjectMapper jsonMapper,
        Injector injector,
        NamespaceExtractionCacheManager namespaceExtractionCacheManager,
        @Named("io.druid.server.namespace.NamespacedExtractionModule")
        ConcurrentMap<String, Function<String, String>> fnCache,
        NamespacedExtractionModuleConfig config
    )
    {
      this.curator = curator;
      this.zkPathsConfig = zkPathsConfig;
      this.jsonMapper = jsonMapper;
      this.injector = injector;
      this.namespaceExtractionCacheManager = namespaceExtractionCacheManager;
      this.fnCache = fnCache;
      this.enableNamespaces = config == null ? false : config.enabled;
    }

    @LifecycleStart
    public void start()
    {
      if (!enableNamespaces) {
        log.info("Namespaces disabled. Skipping start()");
        return;
      }
      try {
        curator.newNamespaceAwareEnsurePath(zkPathsConfig.getNamespacePath()).ensure(curator.getZookeeperClient());
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
      final PathChildrenCache pathChildrenCache = new PathChildrenCache(
          curator,
          zkPathsConfig.getNamespacePath(),
          true,
          true,
          MoreExecutors.sameThreadExecutor()
      );

      log.info("Registering extraction namespace listener on path [%s]", zkPathsConfig.getNamespacePath());

      pathChildrenCache.getListenable().addListener(
          new PathChildrenCacheListener()
          {
            @Override
            public void childEvent(
                CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent
            ) throws Exception
            {
              if (!ImmutableSet.of(
                  PathChildrenCacheEvent.Type.CHILD_UPDATED,
                  PathChildrenCacheEvent.Type.CHILD_ADDED,
                  PathChildrenCacheEvent.Type.CHILD_REMOVED
              ).contains(pathChildrenCacheEvent.getType())) {
                // Don't care if not above
                return;
              }
              final byte[] data = pathChildrenCacheEvent.getData().getData();
              final String sData = StringUtils.fromUtf8(data);
              final ExtractionNamespaceUpdate update = jsonMapper.readValue(sData, ExtractionNamespaceUpdate.class);
              final String ns = update.getNamespace().getNamespace();
              final PathChildrenCacheEvent.Type eventType = pathChildrenCacheEvent.getType();
              log.debug("Processing event type [%s]", eventType);

              if (eventType.equals(PathChildrenCacheEvent.Type.CHILD_REMOVED) ||
                  eventType.equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)) {
                log.debug("Removing namespace [%s]", ns);
                namespaceExtractionCacheManager.delete(ns);
                fnCache.remove(ns);
              }

              if (eventType.equals(PathChildrenCacheEvent.Type.CHILD_ADDED) ||
                  eventType.equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)) {
                log.debug("Adding namespace [%s] : %s", ns, update.toString());
                final ExtractionNamespace namespace = update.getNamespace();
                final ExtractionNamespaceFunctionFactory factory = injector.getInstance(
                    Key.get(
                        ExtractionNamespaceFunctionFactory.class,
                        Names.named(
                            namespace
                                .getClass()
                                .getCanonicalName()
                        )
                    )
                );

                final Runnable cachePopulator = factory.getCachePopulator(namespace);
                final Function<String, String> fn = factory.build(namespace);
                final Object prior;
                if (update.getUpdateMs() > 0) {
                  log.info(
                      "Adding repeating update for namespace [%s] at an interval of [%d] ms via [%s]",
                      namespace.getNamespace(),
                      update.getUpdateMs(),
                      update.toString()
                  );
                  Futures.addCallback(
                      namespaceExtractionCacheManager.scheduleRepeat(
                          namespace.getNamespace(), cachePopulator, update.getUpdateMs(),
                          TimeUnit.MILLISECONDS
                      ), new FutureCallback<Object>()
                      {
                        @Override
                        public void onSuccess(@Nullable Object result)
                        {
                          // NoOp
                        }

                        @Override
                        public void onFailure(Throwable t)
                        {
                          log.error(t, "Failed to load namespace [%s]", namespace.toString());
                        }
                      },
                      MoreExecutors.sameThreadExecutor()
                  );
                  prior = fnCache.put(ns, fn);
                } else {
                  log.info(
                      "Scheduling one update of namespace [%s] via [%s]",
                      namespace.getNamespace(),
                      update.toString()
                  );
                  namespaceExtractionCacheManager.scheduleOnce(ns, cachePopulator);
                  prior = fnCache.put(ns, fn);
                }
                if (prior != null) {
                  log.warn("Function for namespace [%s] was overridden", namespace.getNamespace());
                }
              }
            }
          },
          listeningExecutorService
      );
      try {
        pathChildrenCache.start();
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    @LifecycleStop
    public void stop()
    {
      if (!enableNamespaces) {
        log.info("Namespaces disabled. Skipping stop()");
        return;
      }
      curator.close();
    }

    public Collection<String> listNamespaces()
    {
      final ArrayList<Exception> innerExceptions = new ArrayList<>();
      Set<String> retval = null;
      try {
         retval = ImmutableSet.copyOf(
            Collections2.filter(
                Collections2.transform(
                    curator.getChildren().forPath(zkPathsConfig.getNamespacePath()),
                    new Function<String, String>()
                    {
                      @Nullable
                      @Override
                      public String apply(@NotNull String input)
                      {
                        try {
                          return jsonMapper.readValue(
                              curator.getData()
                                     .forPath(
                                         ZKPaths.makePath(
                                             zkPathsConfig.getNamespacePath(),
                                             input
                                         )
                                     ), ExtractionNamespaceUpdate.class
                          ).getNamespace().getNamespace();
                        } catch(Exception e){
                          innerExceptions.add(e);
                          return null;
                        }
                      }
                    }
                ), Predicates.notNull()
            )
        );
      }
      catch (Exception e)
      {
        innerExceptions.add(e);
      }
      if(!innerExceptions.isEmpty()){
        if(innerExceptions.size() == 1){
          throw Throwables.propagate(innerExceptions.get(0));
        }
        final IAE iae = new IAE("Error in reading data from zookeeper");
        for(Exception inEx : innerExceptions){
          iae.addSuppressed(inEx);
        }
        throw iae;
      }
      return retval;
    }


    public ExtractionNamespaceUpdate newUpdate(final ExtractionNamespaceUpdate update){
      try {
        RetryUtils.retry(
            new Callable<Void>()
            {
              @Override
              public Void call() throws Exception
              {
                final String path = ZKPaths.makePath(
                    zkPathsConfig.getNamespacePath(),
                    update.getNamespace().getNamespace()
                );
                if (null != curator.checkExists().forPath(path)) {
                  throw new IAE("Namespace [%s] already exists", update.getNamespace().getNamespace());
                }
                curator.inTransaction().create().forPath(path, jsonMapper.writeValueAsBytes(update)).and().commit();
                return null;
              }
            },
            new Predicate<Throwable>()
            {
              @Override
              public boolean apply(@Nullable Throwable input)
              {
                return input instanceof IOException;
              }
            },
            10
        );
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
      return update;
    }

    public void deleteNamespace(final String namespace){
      try{
        RetryUtils.retry(
            new Callable<Void>()
            {
              @Override
              public Void call() throws Exception
              {
                final String path = ZKPaths.makePath(zkPathsConfig.getNamespacePath(), namespace);
                if (null == curator.checkExists().forPath(path)) {
                  throw new IAE("Namespace [%s] does not exists", namespace);
                }
                curator.inTransaction().delete().forPath(path).and().commit();
                return null;
              }
            },
            new Predicate<Throwable>()
            {
              @Override
              public boolean apply(@Nullable Throwable input)
              {
                return input instanceof IOException;
              }
            },
            10
        );
      }catch(Exception e){
        throw Throwables.propagate(e);
      }
    }
  }
}
