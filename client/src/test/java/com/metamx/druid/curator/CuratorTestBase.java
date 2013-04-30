package com.metamx.druid.curator;

import com.google.common.io.Closeables;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.curator.test.Timing;

/**
 */
public class CuratorTestBase
{
  protected TestingServer server;
  protected Timing timing;
  protected CuratorFramework curator;

  protected void setupServerAndCurator() throws Exception
  {
    server = new TestingServer();
    timing = new Timing();
    curator = CuratorFrameworkFactory
        .builder()
        .connectString(server.getConnectString())
        .sessionTimeoutMs(timing.session())
        .connectionTimeoutMs(timing.connection())
        .retryPolicy(new RetryOneTime(1))
        .compressionProvider(new PotentiallyGzippedCompressionProvider(true))
        .build();

  }

  protected void tearDownServerAndCurator()
  {
    Closeables.closeQuietly(curator);
    Closeables.closeQuietly(server);
  }
}
