package com.metamx.druid.client;

import com.metamx.common.Pair;
import com.metamx.common.logger.Logger;
import com.metamx.phonebook.PhoneBook;
import com.metamx.phonebook.PhoneBookPeon;

import java.util.Map;

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
