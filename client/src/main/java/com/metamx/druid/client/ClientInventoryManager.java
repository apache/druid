/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.client;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metamx.common.Pair;
import com.metamx.common.logger.Logger;
import com.metamx.phonebook.PhoneBook;
import com.metamx.phonebook.PhoneBookPeon;

/**
 */
public class ClientInventoryManager extends InventoryManager<DruidServer>
{
  private static final Logger log = new Logger(ClientInventoryManager.class);

  private final Object lock = new Object();

  private final Executor exec;

  private final MutableServerView serverView;

  public ClientInventoryManager(
      final InventoryManagerConfig config,
      final PhoneBook yp,
      final MutableServerView serverView
  )
  {
    super(log, config, yp);

    this.serverView = serverView;

    this.exec = Executors.newFixedThreadPool(
        1, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("CIV-Execution-%d").build()
    );

    setStrategy(
        new InventoryManagementStrategy<DruidServer>()
        {
          @Override
          public Class<DruidServer> getContainerClass()
          {
            return DruidServer.class;
          }

          @Override
          public Pair<String, PhoneBookPeon<?>> makeSubListener(final DruidServer server)
          {
            ClientInventoryManager.this.serverView.addServer(server);

            return Pair.<String, PhoneBookPeon<?>>of(
                server.getName(),
                new PhoneBookPeon<DataSegment>()
                {
                  @Override
                  public Class<DataSegment> getObjectClazz()
                  {
                    return DataSegment.class;
                  }

                  @Override
                  public void newEntry(String segmentId, DataSegment segment)
                  {
                    exec.execute(new AddSegmentRunnable(server, segment));
                    server.addDataSegment(segmentId, segment);
                  }

                  @Override
                  public void entryRemoved(String segmentId)
                  {
                    exec.execute(new RemoveSegmentRunnable(server, segmentId));
                    server.removeDataSegment(segmentId);
                  }
                }
            );
          }

          @Override
          public void objectRemoved(DruidServer server)
          {
            ClientInventoryManager.this.serverView.removeServer(server);
          }

          @Override
          public boolean doesSerde()
          {
            return false;
          }

          @Override
          public DruidServer deserialize(String name, Map<String, String> properties)
          {
            throw new UnsupportedOperationException();
          }
        }
    );
  }

  @Override
  protected void doStop()
  {
    synchronized (lock) {
      serverView.clear();
    }
  }

  private class RemoveSegmentRunnable implements Runnable
  {
    private final DruidServer server;
    private final String segmentId;

    public RemoveSegmentRunnable(DruidServer server, String segmentId)
    {
      this.server = server;
      this.segmentId = segmentId;
    }

    @Override
    public void run()
    {
      serverView.serverRemovedSegment(server, segmentId);
    }
  }

  private class AddSegmentRunnable implements Runnable
  {
    private final DruidServer server;
    private final DataSegment segment;

    public AddSegmentRunnable(DruidServer server, DataSegment segment)
    {
      this.server = server;
      this.segment = segment;
    }

    @Override
    public void run()
    {
      serverView.serverAddedSegment(server, segment);
    }
  }
}
