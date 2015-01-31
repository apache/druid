/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.curator;

import com.metamx.common.guava.CloseQuietly;
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
    CloseQuietly.close(curator);
    CloseQuietly.close(server);
  }
}
