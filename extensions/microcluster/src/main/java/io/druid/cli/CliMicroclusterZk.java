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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.name.Names;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import io.airlift.command.Command;
import io.druid.guice.LifecycleModule;
import io.druid.guice.ManageLifecycle;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import java.io.File;
import java.io.IOException;
import java.util.List;


@Command(
    name = "ZK",
    description = "Runs a microcluster zookeeper"
)
public class CliMicroclusterZk extends ServerRunnable
{
  private static final Logger log = new Logger(CliMicroclusterZk.class);
  public static final int zkPort = 2181;

  public CliMicroclusterZk()
  {
    super(log);
  }

  @ManageLifecycle
  public static class ZkServer
  {
    ZooKeeperServer zkServer;
    ServerCnxnFactory serverCnxnFactory;

    @LifecycleStart
    public void start()
    {

      final Thread currentThread = Thread.currentThread();
      try {
        File tempDir = File.createTempFile("microserver", "zk");
        tempDir.delete();
        tempDir.mkdir();
        tempDir.deleteOnExit();
        zkServer = new ZooKeeperServer(tempDir, tempDir, 2000);
        serverCnxnFactory = ServerCnxnFactory.createFactory(zkPort, 1000);
        serverCnxnFactory.startup(zkServer);
        System.getProperties().setProperty("druid.zk.service.host", String.format("localhost:%d", zkPort));
        log.info("Started zookeeper on localhost:%d", zkPort);
        synchronized (currentThread){
          currentThread.notifyAll();
        }
      }
      catch (java.net.BindException e) {
        log.warn("Port %d is already in use, politely skipping local ZK setup");
        stop();
        synchronized (currentThread){
          currentThread.notifyAll();
        }
      }
      catch (IOException | InterruptedException e) {
        log.error(e, "Error starting Zk");
        System.exit(-1);
      }
    }

    @LifecycleStop
    public void stop()
    {
      // Do not actually stop the zk service, let it die with the JVM
    }
  }

  @Override
  protected List<Module> getModules()
  {
    return ImmutableList.<Module>of(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/microcluster/zk");
            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(zkPort);
            binder.bind(ZkServer.class).in(ManageLifecycle.class);
            LifecycleModule.register(binder, ZkServer.class);
          }
        }
    );
  }
}
