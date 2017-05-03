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

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Injector;
import com.metamx.common.logger.Logger;
import io.airlift.command.Command;
import io.druid.guice.GuiceInjectors;
import io.druid.server.microcluster.MicroclusterModule;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

/**
 *
 */

@Command(
    name = "all",
    description = "Runs a microcluster of a historical, broker, and coordinator thread"
)
public class CliMicrocluster extends ServerRunnable
{

  private static final Logger log = new Logger(CliMicrocluster.class);

  public CliMicrocluster()
  {
    super(log);
  }

  @Override
  protected List<Object> getModules()
  {
    return ImmutableList.<Object>of(new MicroclusterModule());
  }

  @Override
  public void run()
  {
    Properties properties = new Properties();
    try (InputStream inStream = this.getClass().getResourceAsStream("/microcluster.runtime.properties")){
      properties.load(inStream);
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
    System.getProperties().putAll(properties);

    // Start zookeeper
    try {
      File tempDir = File.createTempFile("microserver","zk");
      tempDir.delete();
      tempDir.mkdir();
      tempDir.deleteOnExit();
      final ZooKeeperServer zkServer = new ZooKeeperServer(tempDir, tempDir, 2000);
      final ServerCnxnFactory serverCnxnFactory = ServerCnxnFactory.createFactory(0, 1000);
      final int zkPort = serverCnxnFactory.getLocalPort();
      serverCnxnFactory.startup(zkServer);
      System.getProperties().setProperty("druid.zk.service.host", "localhost");
      System.getProperties().setProperty("druid.zk.service.port", String.format("%d", zkPort));
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
    catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }

    try {
      final Path tempPath = Files.createTempDirectory("microserver-localstorage");
      final File tempDir = tempPath.toFile();
      tempDir.deleteOnExit();
      System.getProperties().setProperty("druid.storage.storage.storageDirectory",tempDir.getAbsolutePath());

    }
    catch (IOException e) {
      e.printStackTrace();
    }

    Runnable historicMain = new Runnable()
    {
      @Override
      public void run()
      {
        // Special properties
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
        final Injector injector = GuiceInjectors.makeStartupInjector();
        Runnable historical = new io.druid.cli.CliHistorical();
        injector.injectMembers(historical);
        historical.run();
      }
    };
    Runnable brokerMain = new Runnable()
    {
      @Override
      public void run()
      {
        final Injector injector = GuiceInjectors.makeStartupInjector();
        Runnable broker = new io.druid.cli.CliBroker();
        injector.injectMembers(broker);
        broker.run();
      }
    };
    Runnable coordinatorMain = new Runnable()
    {
      @Override
      public void run()
      {
        final Injector injector = GuiceInjectors.makeStartupInjector();
        Runnable coordinator = new io.druid.cli.CliCoordinator();
        injector.injectMembers(coordinator);
        coordinator.run();
      }
    };
    ThreadFactoryBuilder threadFactoryBuilder = new ThreadFactoryBuilder().setDaemon(true)
                                                                          .setPriority(Thread.NORM_PRIORITY);
    ThreadPoolExecutor executor = new ThreadPoolExecutor(
        3, 3, 0l, TimeUnit.MINUTES,
        new ArrayBlockingQueue<Runnable>(3),
        threadFactoryBuilder.build()
    );

    ArrayList<Future<?>> futures = new ArrayList<>();
    futures.add(executor.submit(historicMain));
    futures.add(executor.submit(brokerMain));
    futures.add(executor.submit(coordinatorMain));
    executor.shutdown();
    for (Future<?> future : futures) {
      try {
        future.get();
      }
      catch (InterruptedException | ExecutionException e) {
        log.error(e, "Error executing task");
      }
    }
  }
}
