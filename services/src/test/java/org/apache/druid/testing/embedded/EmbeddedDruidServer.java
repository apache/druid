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

package org.apache.druid.testing.embedded;

import com.google.inject.Binder;
import com.google.inject.Injector;
import org.apache.druid.cli.ServerRunnable;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.metrics.LatchableEmitter;
import org.apache.druid.utils.RuntimeInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An embedded Druid server used in embedded tests.
 * This class and most of its methods are kept package protected as they are used
 * only by the specific server implementations in the same package.
 */
public abstract class EmbeddedDruidServer<T extends EmbeddedDruidServer<T>> implements EmbeddedResource
{
  private static final Logger log = new Logger(EmbeddedDruidServer.class);
  protected static final long MEM_100_MB = HumanReadableBytes.parse("100M");
  protected static final long MEM_2_MB = HumanReadableBytes.parse("2M");

  /**
   * A static incremental ID is used instead of a random number to ensure that
   * tests are more deterministic and easier to debug.
   */
  private static final AtomicInteger SERVER_ID = new AtomicInteger(0);

  private final String name;
  private final AtomicReference<EmbeddedServerLifecycle> lifecycle = new AtomicReference<>();

  private long serverMemory = MEM_100_MB;
  private long serverDirectMemory = MEM_2_MB;
  private final Map<String, String> serverProperties = new HashMap<>();
  private final List<BeforeStart> beforeStartHooks = new ArrayList<>();
  private final ServerReferenceHolder referenceHolder = new ServerReferenceHolder();

  protected EmbeddedDruidServer()
  {
    this.name = StringUtils.format(
        "%s-%d",
        this.getClass().getSimpleName(),
        SERVER_ID.incrementAndGet()
    );
    beforeStartHooks.add(
        (cluster, self) -> {
          // Add properties for temporary directories used by the servers
          final String logsDirectory = cluster.getTestFolder().getOrCreateFolder("indexer-logs").getAbsolutePath();
          final String taskDirectory = cluster.getTestFolder().newFolder().getAbsolutePath();
          final String storageDirectory = cluster.getTestFolder().getOrCreateFolder("deep-store").getAbsolutePath();
          log.info(
              "Server[%s] using directories: task directory[%s], logs directory[%s], storage directory[%s].",
              self.getName(),
              taskDirectory,
              logsDirectory,
              storageDirectory
          );

          self.addProperty("druid.extensions.loadList", "[]");
          self.addProperty("druid.host", cluster.getEmbeddedHostname().toString());
          self.addProperty("druid.indexer.task.baseDir", taskDirectory);
          self.addProperty("druid.indexer.logs.directory", logsDirectory);
          self.addProperty("druid.storage.storageDirectory", storageDirectory);

          // Add properties for RuntimeInfoModule
          self.addProperty(RuntimeInfoModule.SERVER_MEMORY_PROPERTY, String.valueOf(serverMemory));
          self.addProperty(RuntimeInfoModule.SERVER_DIRECT_MEMORY_PROPERTY, String.valueOf(serverDirectMemory));
        }
    );
  }

  @Override
  public void start() throws Exception
  {
    final EmbeddedServerLifecycle lifecycle = this.lifecycle.get();
    if (lifecycle == null) {
      throw new ISE("Server[%s] can be run only after it has been added to a cluster.", name);
    } else {
      lifecycle.start();
    }
  }

  @Override
  public void stop() throws Exception
  {
    final EmbeddedServerLifecycle lifecycle = this.lifecycle.get();
    if (lifecycle == null) {
      throw new ISE("Server[%s] can be run only after it has been added to a cluster.", name);
    } else {
      lifecycle.stop();
    }
  }

  @Override
  public void beforeStart(EmbeddedDruidCluster cluster)
  {
    initServerLifecycle(cluster.getCommonProperties());
    for (BeforeStart hook : beforeStartHooks) {
      hook.run(cluster, this);
    }
  }

  /**
   * @return Name of this server = type + 2-digit ID.
   */
  public final String getName()
  {
    return name;
  }

  /**
   * Adds a property to this server. These properties correspond to the
   * {@code runtime.properties} file in a real Druid cluster, and override the
   * common properties specified via {@link EmbeddedDruidCluster#addCommonProperty}.
   */
  @SuppressWarnings("unchecked")
  public final T addProperty(String key, String value)
  {
    serverProperties.put(key, value);
    return (T) this;
  }

  /**
   * Adds a {@link BeforeStart} to run as part of {@link #beforeStart(EmbeddedDruidCluster)}
   */
  @SuppressWarnings({"unchecked"})
  public final T addBeforeStartHook(BeforeStart hook)
  {
    beforeStartHooks.add(hook);
    return (T) this;
  }

  /**
   * Sets the amount of heap memory visible to the server through {@link RuntimeInfo}.
   */
  @SuppressWarnings("unchecked")
  public final T setServerMemory(long serverMemory)
  {
    this.serverMemory = serverMemory;
    return (T) this;
  }

  /**
   * Sets the amount of direct (off-heap) memory visible to the server through {@link RuntimeInfo}.
   */
  @SuppressWarnings("unchecked")
  public final T setServerDirectMemory(long serverDirectMemory)
  {
    this.serverDirectMemory = serverDirectMemory;
    return (T) this;
  }

  /**
   * Called from {@link EmbeddedDruidCluster#addServer(EmbeddedDruidServer)} to
   * tie the lifecycle of this server to the cluster.
   */
  private void initServerLifecycle(Properties commonProperties)
  {
    if (lifecycle.get() == null) {
      lifecycle.set(new EmbeddedServerLifecycle(this, commonProperties));
    }
  }

  /**
   * Creates a {@link ServerRunnable} corresponding to a specific Druid service.
   * Implementations of this class MUST NOT return a {@link ServerRunnable} that
   * overrides any default Druid behaviour so that the embedded cluster closely
   * replicates a real cluster. If an override is needed, it must be done using
   * extensions and Druid properties, which are visible to the unit test so that
   * there is no hidden config.
   *
   * @see EmbeddedDruidCluster#addExtension
   * @see EmbeddedDruidCluster#addCommonProperty
   * @see EmbeddedDruidServer#addProperty
   */
  protected abstract ServerRunnable createRunnable(
      LifecycleInitHandler handler
  );

  /**
   * Properties to be used in the {@code StartupInjectorBuilder} while launching
   * this server. This must be called only after all the resources required by
   * the Druid server have been initialized.
   */
  final Properties getStartupProperties()
  {
    final Properties serverProperties = new Properties();
    serverProperties.putAll(this.serverProperties);
    return serverProperties;
  }

  /**
   * Binds the {@link ServerReferenceHolder} for this server.
   * All implementations of {@link EmbeddedDruidServer} must use this binding in
   * {@link ServerRunnable#getModules()}.
   */
  protected final void bindReferenceHolder(Binder binder)
  {
    binder.bind(ServerReferenceHolder.class).toInstance(referenceHolder);
  }

  /**
   * Provides access to the various dependencies bound by Guice on this server.
   * The bindings should be used for read-only purposes and should not mutate
   * the state of this server or the cluster, so that the embedded cluster can
   * mirror the behaviour of a real production cluster.
   */
  public final ServerReferencesProvider bindings()
  {
    return referenceHolder;
  }

  /**
   * {@link LatchableEmitter} used by this server, if bound.
   */
  public final LatchableEmitter latchableEmitter()
  {
    return referenceHolder.latchableEmitter();
  }

  @Override
  public String toString()
  {
    return "EmbeddedDruidServer{" +
           "name='" + name + '\'' +
           '}';
  }

  /**
   * Handler used to register the lifecycle of an embedded server.
   */
  protected interface LifecycleInitHandler
  {
    /**
     * Registers the lifecycle of this server so that it can be stopped later.
     * All implementations of {@link EmbeddedDruidServer} must call this method
     * from {@link ServerRunnable#initLifecycle(Injector)}.
     */
    void onLifecycleInit(Lifecycle lifecycle);
  }

  @FunctionalInterface
  public interface BeforeStart
  {
    /**
     * Allows a {@link EmbeddedDruidServer} to perform additional initialization before starting
     *
     * @param cluster - the {@link EmbeddedDruidCluster} the {@link EmbeddedDruidServer} is part of
     * @param self    - the {@link EmbeddedDruidServer} to perform initialization on
     */
    void run(EmbeddedDruidCluster cluster, EmbeddedDruidServer<?> self);
  }
}
