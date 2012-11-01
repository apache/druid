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

import com.metamx.common.Pair;
import com.metamx.common.logger.Logger;
import com.metamx.phonebook.PhoneBook;
import com.metamx.phonebook.PhoneBookPeon;

/**
 */
public class SegmentInventoryManager extends InventoryManager<DruidDataSource>
{
  private static final Logger log = new Logger(SegmentInventoryManager.class);

  public SegmentInventoryManager(
      SegmentInventoryManagerConfig config,
      PhoneBook zkPhoneBook
  )
  {
    super(
        log,
        new InventoryManagerConfig(
            config.getBasePath(),
            config.getBasePath()
        ),
        zkPhoneBook,
        new SegmentInventoryManagementStrategy()
    );
  }

  private static class SegmentInventoryManagementStrategy implements InventoryManagementStrategy<DruidDataSource>
  {
    @Override
    public Class<DruidDataSource> getContainerClass()
    {
      return DruidDataSource.class;
    }

    @Override
    public Pair<String, PhoneBookPeon<?>> makeSubListener(final DruidDataSource baseObject)
    {
      return new Pair<String, PhoneBookPeon<?>>(
          baseObject.getName(),
          new PhoneBookPeon<DataSegment>()
          {
            @Override
            public Class<DataSegment> getObjectClazz()
            {
              return DataSegment.class;
            }

            @Override
            public void newEntry(String name, DataSegment segment)
            {
              log.info("Adding dataSegment[%s].", segment);
              baseObject.addSegment(name, segment);
            }

            @Override
            public void entryRemoved(String name)
            {
              log.info("Partition[%s] deleted.", name);
              baseObject.removePartition(name);
            }
          }
      );
    }

    @Override
    public void objectRemoved(DruidDataSource baseObject)
    {
    }

    @Override
    public boolean doesSerde()
    {
      return true;
    }

    @Override
    public DruidDataSource deserialize(String name, Map<String, String> properties)
    {
      return new DruidDataSource(name, properties);
    }
  }
}
