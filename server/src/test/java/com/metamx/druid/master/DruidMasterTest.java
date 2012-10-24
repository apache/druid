package com.metamx.druid.master;

import com.metamx.common.concurrent.ScheduledExecutorFactory;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidServer;
import com.metamx.druid.client.ServerInventoryManager;
import com.metamx.druid.client.ZKPhoneBook;
import com.metamx.druid.coordination.legacy.TheSizeAdjuster;
import com.metamx.druid.db.DatabaseSegmentManager;
import com.metamx.druid.metrics.NoopServiceEmitter;
import com.metamx.phonebook.PhoneBook;
import org.easymock.EasyMock;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

/**
 */
public class DruidMasterTest
{
  private static final Logger log = new Logger(DruidMasterTest.class);

  private DruidMaster master;
  private PhoneBook yp;
  private DatabaseSegmentManager databaseSegmentManager;
  private ServerInventoryManager serverInventoryManager;
  private TheSizeAdjuster theSizeAdjuster;
  private ScheduledExecutorFactory scheduledExecutorFactory;
  private DruidServer druidServer;
  private DataSegment segment;
  private Map<String, LoadQueuePeon> loadManagementPeons;
  private LoadQueuePeon loadQueuePeon;

  @Before
  public void setUp() throws Exception
  {
    druidServer = EasyMock.createMock(DruidServer.class);
    segment = EasyMock.createNiceMock(DataSegment.class);
    loadQueuePeon = EasyMock.createNiceMock(LoadQueuePeon.class);
    loadManagementPeons = EasyMock.createMock(Map.class);
    serverInventoryManager = EasyMock.createMock(ServerInventoryManager.class);

    yp = EasyMock.createNiceMock(ZKPhoneBook.class);
    EasyMock.replay(yp);

    databaseSegmentManager = EasyMock.createNiceMock(DatabaseSegmentManager.class);
    EasyMock.replay(databaseSegmentManager);

    theSizeAdjuster = EasyMock.createNiceMock(TheSizeAdjuster.class);
    EasyMock.replay(theSizeAdjuster);

    scheduledExecutorFactory = EasyMock.createNiceMock(ScheduledExecutorFactory.class);
    EasyMock.replay(scheduledExecutorFactory);

    master = new DruidMaster(
        new DruidMasterConfig()
        {
          @Override
          public String getHost()
          {
            return null;
          }

          @Override
          public String getBasePath()
          {
            return null;
          }

          @Override
          public String getLoadQueuePath()
          {
            return null;
          }

          @Override
          public String getServedSegmentsLocation()
          {
            return null;
          }

          @Override
          public Duration getMasterStartDelay()
          {
            return null;
          }

          @Override
          public Duration getMasterPeriod()
          {
            return null;
          }

          @Override
          public Duration getMasterSegmentMergerPeriod()
          {
            return null;
          }

          @Override
          public long getMillisToWaitBeforeDeleting()
          {
            return super.getMillisToWaitBeforeDeleting();
          }

          @Override
          public String getMergerServiceName()
          {
            return "";
          }

          @Override
          public long getMergeThreshold()
          {
            return super.getMergeThreshold();
          }
        },
        null,
        null,
        null,
        databaseSegmentManager,
        serverInventoryManager,
        theSizeAdjuster,
        yp,
        new NoopServiceEmitter(),
        scheduledExecutorFactory,
        loadManagementPeons,
        null
    );
  }

  @After
  public void tearDown() throws Exception
  {
    EasyMock.verify(druidServer);
    EasyMock.verify(loadManagementPeons);
    EasyMock.verify(loadQueuePeon);
    EasyMock.verify(serverInventoryManager);
  }

  @Test
  public void testMoveSegment() throws Exception
  {
    EasyMock.expect(druidServer.getSegment("dummySegment")).andReturn(segment);
    EasyMock.expect(druidServer.getMaxSize()).andReturn(new Long(5));
    EasyMock.expect(druidServer.getCurrSize()).andReturn(new Long(1)).atLeastOnce();
    EasyMock.expect(druidServer.getName()).andReturn("blah");
    EasyMock.replay(druidServer);

    EasyMock.expect(loadManagementPeons.get("to")).andReturn(loadQueuePeon);
    EasyMock.expect(loadManagementPeons.get("from")).andReturn(loadQueuePeon);
    EasyMock.replay(loadManagementPeons);

    EasyMock.expect(loadQueuePeon.getLoadQueueSize()).andReturn(new Long(1));
    EasyMock.replay(loadQueuePeon);

    EasyMock.expect(serverInventoryManager.getInventoryValue("from")).andReturn(druidServer);
    EasyMock.expect(serverInventoryManager.getInventoryValue("to")).andReturn(druidServer);
    EasyMock.replay(serverInventoryManager);

    master.moveSegment("from", "to", "dummySegment", null);
  }
}
