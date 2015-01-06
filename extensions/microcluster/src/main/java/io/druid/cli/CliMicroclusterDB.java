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

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.net.InetAddresses;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.name.Names;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import io.airlift.command.Command;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.ManageLifecycle;
import org.apache.derby.drda.NetworkServerControl;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
@Command(
    name = "DB",
    description = "Runs a microcluster DB"
)
public class CliMicroclusterDB extends ServerRunnable
{
  private static final Logger log = new Logger(CliMicroclusterDB.class);
  public static final int dbPort = 1527;

  public CliMicroclusterDB()
  {
    super(log);
  }

  @ManageLifecycle
  public static class DerbyDB
  {
    NetworkServerControl server;

    @LifecycleStart
    public void start()
    {
      try {
        server = new NetworkServerControl
            (InetAddresses.forString("127.0.0.1"), dbPort);
        server.start(
            new PrintWriter(
                new OutputStream()
                {
                  ArrayList<Byte> buff = new ArrayList<>(1024);

                  @Override
                  public void write(int b) throws IOException
                  {
                    if (System.lineSeparator().getBytes()[0] == (byte) b) {
                      flush();
                    } else {
                      buff.add((byte) b);
                    }
                  }

                  @Override
                  public void flush()
                  {
                    final byte[] bytes = new byte[buff.size()];
                    for (int i = 0; i < bytes.length; ++i) {
                      bytes[i] = buff.get(i);
                    }
                    try {
                      log.info("DB: %s", new String(bytes, "UTF-8"));
                    }
                    catch (UnsupportedEncodingException e) {
                      throw Throwables.propagate(e);
                    }
                    buff.clear();
                    buff.ensureCapacity(1024);
                  }
                }
            )
        );
        // Give the server some time to attach to the port
        Thread.sleep(1000);
        server.ping();
        log.info(System.lineSeparator() + "Started DB: " + server.getRuntimeInfo());
      }
      catch (Exception e) {
        log.error(e, "Error in setting up DB");
        System.exit(-1);
      }
      final Thread currentThread = Thread.currentThread();
      synchronized (currentThread) {
        currentThread.notifyAll();
      }
    }

    @LifecycleStop
    public void stop()
    {
      // Let the server die with the JVM
    }
  }

  @Override
  protected List<Object> getModules()
  {
    return ImmutableList.<Object>of(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/microcluster/db");
            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(dbPort);
            binder.bind(DerbyDB.class).in(ManageLifecycle.class);
            LifecycleModule.register(binder, DerbyDB.class);
          }
        }
    );
  }
}
