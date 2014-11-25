/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
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
package io.druid.cli;

import com.google.api.client.repackaged.com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import io.airlift.command.Command;
import io.druid.guice.GuiceInjectors;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import io.druid.guice.LifecycleModule;
import io.druid.guice.ManageLifecycle;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import javax.annotation.Nullable;

/**
 *
 */

@Command(
    name = "all",
    description = "Runs a microcluster of a historical, broker, overlord, and coordinator thread"
)
public class CliMicrocluster extends ServerRunnable
{
  private static final Logger log = new Logger(CliMicrocluster.class);

  @Inject
  final Injector baseInjector = null;

  public CliMicrocluster()
  {
    super(log);
  }

  @ManageLifecycle
  public static class Microcluster
  {
    final ListeningExecutorService executorService = MoreExecutors.listeningDecorator(
        Executors.newFixedThreadPool(
            10,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("microcluster-%d")
                .build()
        )
    );
    final ConcurrentHashMap<String, Process> cmdProcesses = new ConcurrentHashMap<>();
    final ConcurrentHashMap<String, ListenableFuture<?>> cmdFutures = new ConcurrentHashMap<>();
    static final List<List<String>> commands = ImmutableList.<List<String>>of(
        ImmutableList.<String>of("server", "historical"),
        ImmutableList.<String>of("server", "broker"),
        ImmutableList.<String>of("server", "coordinator"),
        ImmutableList.<String>of("server", "overlord"),
        ImmutableList.<String>of("server", "middleManager")
    );
    final Thread dbThread = new Thread(
        new Runnable()
        {
          @Override
          public void run()
          {
            io.druid.cli.Main.main(new String[]{"microcluster","DB"});
          }
        }
    );

    final Thread zkThread = new Thread(
        new Runnable()
        {
          @Override
          public void run()
          {
            io.druid.cli.Main.main(new String[]{"microcluster","ZK"});
          }
        }
    );
    @LifecycleStart
    public void start()
    {
      final Path tmpDBPath;
      try {
        tmpDBPath = Files.createTempDirectory("microclusterMetaDB");
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
      File tmpDB = tmpDBPath.toFile();
      tmpDB.deleteOnExit();
      final String connectionURI = String.format(
          "jdbc:derby://127.0.0.1:%d%s/metaDB;create=true",
          CliMicroclusterDB.dbPort,
          tmpDB.getAbsolutePath()
      );
      System.getProperties().setProperty("druid.metadata.storage.connector.connectURI", connectionURI);

      Properties properties = new Properties();
      try (InputStream inStream = this.getClass().getResourceAsStream("/microcluster.runtime.properties")) {
        properties.load(inStream);
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
      // Override with environment properties
      System.getProperties().putAll(properties);

      try {
        Class<?> driver = Class.forName("org.apache.derby.jdbc.ClientDriver");
      }
      catch (ClassNotFoundException e) {
        throw Throwables.propagate(e);
      }

      try {
        final Path tempPath = Files.createTempDirectory("microserver-localstorage");
        final File tempDir = tempPath.toFile();
        tempDir.deleteOnExit();
        System.getProperties().setProperty("druid.storage.storage.storageDirectory", tempDir.getAbsolutePath());

      }
      catch (IOException e) {
        e.printStackTrace();
      }

      try {
        final Path tempPath = Files.createTempDirectory("microserver-historical");
        final File tempDir = tempPath.toFile();
        tempDir.deleteOnExit();
        System.getProperties()
              .setProperty(
                  "druid.segmentCache.locations",
                  "[{\"path\": \"" + tempDir.getAbsolutePath() + "\", \"maxSize\": " + String.format(
                      "%d",
                      tempDir.getFreeSpace()
                  ) + " }]"
              );
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }

      // Start up general cluster resources first
      try {
        dbThread.setName("microcluster-db");
        dbThread.start();
        synchronized (dbThread) {
          dbThread.wait();
        }
        zkThread.setName("microcluster-zk");
        zkThread.start();
        synchronized (zkThread) {
          zkThread.wait();
        }
      }
      catch (InterruptedException e) {
        throw Throwables.propagate(e);
      }
      for (final List<String> cmd : commands) {
        log.info("Submitting cmd %s", cmd);
        final String cmdKey = cmd.get(1);
        ListenableFuture future = executorService.submit(
            new Runnable()
            {
              @Override
              public void run()
              {
                log.info("Starting cmd %s", cmd);
                List<String> sb = new LinkedList<String>();
                sb.add("java");
                sb.add("-server");
                sb.add("-Duser.timezone=UTC");
                sb.add("-Dfile.encoding=UTF-8");
                for (Map.Entry<Object, Object> entry : System.getProperties().entrySet()) {
                  if (entry.getKey().equals("java.class.path")) {
                    sb.add("-cp");
                    sb.add(entry.getValue().toString().replace("\"", "\\\""));
                  } else if (((String) entry.getKey()).startsWith("druid.")) {
                    sb.add(String.format("-D%s=%s", entry.getKey(), entry.getValue().toString()));
                  }
                }
                Thread.currentThread().setName("microcluster-" + cmdKey);
                sb.add("io.druid.cli.Main");
                sb.addAll(cmd);
                ProcessBuilder processBuilder = new ProcessBuilder();
                log.info("Launching command %s with line: %s", cmd, Joiner.on(' ').join(sb));
                processBuilder.inheritIO().command(sb);
                try {
                  Process process = processBuilder.start();
                  cmdProcesses.put(cmdKey, process);
                  // waitFor is running in executor thread
                  int result = process.waitFor();
                  // 143 = java SIGTERM
                  if (0 != result && result != 143) {
                    throw new RuntimeException(
                        String.format(
                            "Process failed for command %s with code %d",
                            cmd,
                            result
                        )
                    );
                  }
                }
                catch (IOException | InterruptedException e) {
                  throw Throwables.propagate(e);
                }
              }
            }
        );
        cmdFutures.put(cmdKey, future);
        future.addListener(
            new Runnable()
            {
              @Override
              public void run()
              {
                printAttentionLine(String.format("Finished %s", cmd));
              }
            }, MoreExecutors.sameThreadExecutor()
        );
      }
    }

    @LifecycleStop
    public void stop()
    {
      log.info("Destroying %d processes", cmdProcesses.size());
      for(List<String> cmd : commands){
        final String cmdKey = cmd.get(1);
        Process process = cmdProcesses.get(cmdKey);
        log.info("Destroying %s", cmdKey);
        process.destroy();
        try {
          cmdFutures.get(cmdKey).get();
        }
        catch (InterruptedException e1) {
          // Interrupted, just continue with shutdown
        }
        catch (ExecutionException e1) {
          log.error(e1, "Error in execution of subtask");

        }
      }
      // Don't stop the zk or db thread... let it die with the JVM
      executorService.shutdownNow();
    }
  }

  @Override
  public List<Module> getModules()
  {
    return ImmutableList.<Module>of(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/microcluster");
            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);// Don't actually need a port
            binder.bind(Microcluster.class).in(ManageLifecycle.class);
            LifecycleModule.register(binder, Microcluster.class);

          }
        }
    );
  }

  private static final void printAttentionLine(String string)
  {
    log.info(
        "\n***********************************************\n**\n** %s\n**\n***********************************************",
        string
    );
  }
}
