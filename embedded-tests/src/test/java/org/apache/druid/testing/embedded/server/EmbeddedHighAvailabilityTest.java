/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.testing.embedded.server;

import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedDruidServer;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

/**
 * Embedded cluster test to verify leadership changes in Coordinator and Overlord.
 * Makes assertions similar to {@code ITHighAvailabilityTest}.
 */
public class EmbeddedHighAvailabilityTest extends EmbeddedClusterTestBase
{
  private final EmbeddedOverlord overlord1 = new EmbeddedOverlord();
  private final EmbeddedOverlord overlord2 = new EmbeddedOverlord();
  private final EmbeddedCoordinator coordinator1 = new EmbeddedCoordinator();
  private final EmbeddedCoordinator coordinator2 = new EmbeddedCoordinator();

  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedRouter router = new EmbeddedRouter();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    overlord1.addProperty("druid.plaintextPort", "7090");
    coordinator1.addProperty("druid.plaintextPort", "7081");

    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addServer(coordinator1)
                               .addServer(coordinator2)
                               .addServer(overlord1)
                               .addServer(overlord2)
                               .addServer(indexer)
                               .addServer(broker)
                               .addServer(router);
  }

  @Test
  public void testLeadershipChanges()
  {
    // Ingest some data so that we can query sys tables later
    final String taskId = dataSource + "_" + IdUtils.getRandomId();
    final String taskPayload = StringUtils.format(
        Resources.INDEX_TASK_PAYLOAD_WITH_INLINE_DATA,
        StringUtils.replace(Resources.CSV_DATA_10_DAYS, "\n", "\\n"),
        dataSource
    );
    cluster.callApi().onLeaderOverlord(
        o -> o.runTask(taskId, EmbeddedClusterApis.createTaskFromPayload(taskId, taskPayload))
    );
    cluster.callApi().waitForTaskToSucceed(taskId, overlord1);

    // Run sys queries, switch leaders, repeat
    ServerPair<EmbeddedOverlord> overlordPair = createServerPair(overlord1, overlord2);
    ServerPair<EmbeddedCoordinator> coordinatorPair = createServerPair(coordinator1, coordinator2);
    for (int i = 0; i < 3; ++i) {
      Assertions.assertEquals(
          "1",
          cluster.runSql("SELECT COUNT(*) FROM sys.tasks WHERE datasource='%s'", dataSource)
      );
      Assertions.assertEquals(
          "10",
          cluster.runSql("SELECT COUNT(*) FROM sys.segments WHERE datasource='%s'", dataSource)
      );
      Assertions.assertEquals(
          "7",
          cluster.runSql("SELECT COUNT(*) FROM sys.servers")
      );

      overlordPair = switchAndVerifyLeader(overlordPair);
      coordinatorPair = switchAndVerifyLeader(coordinatorPair);
    }
  }

  /**
   * Restarts the current leader in the server pair to force the other server to
   * gain leadership. Returns the updated server pair.
   */
  private <S extends EmbeddedDruidServer> ServerPair<S> switchAndVerifyLeader(ServerPair<S> serverPair)
  {
    try {
      // Restart the current leader
      serverPair.leader.stop();
      serverPair.leader.start();

      // Verify that leadership has switched
      final ServerPair<S> updatedPair = new ServerPair<>(serverPair.notLeader, serverPair.leader);
      verifyLeader(updatedPair);

      return updatedPair;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private <S extends EmbeddedDruidServer> ServerPair<S> createServerPair(S serverA, S serverB)
  {
    final boolean aIsLeader;
    if (serverA instanceof EmbeddedOverlord) {
      aIsLeader = serverA.overlordLeaderSelector().isLeader();
    } else {
      aIsLeader = serverA.coordinatorLeaderSelector().isLeader();
    }

    return aIsLeader ? new ServerPair<>(serverA, serverB) : new ServerPair<>(serverB, serverA);
  }

  private <S extends EmbeddedDruidServer> void verifyLeader(ServerPair<S> serverPair)
  {
    if (serverPair.isCoordinator) {
      verifyLeader(serverPair, EmbeddedDruidServer::coordinatorLeaderSelector);
    } else {
      verifyLeader(serverPair, EmbeddedDruidServer::overlordLeaderSelector);
    }
  }

  /**
   * Verifies that exactly one of the servers in the pair is a leader and that
   * other servers know it to be the leader.
   */
  private <S extends EmbeddedDruidServer> void verifyLeader(
      ServerPair<S> serverPair,
      Function<EmbeddedDruidServer, DruidLeaderSelector> getLeaderSelector
  )
  {
    final String leaderUri = serverPair.leader.selfNode().getUriToUse().toString();

    // Verify that the leader knows that it is leader
    Assertions.assertTrue(getLeaderSelector.apply(serverPair.leader).isLeader());
    Assertions.assertEquals(
        leaderUri,
        getLeaderSelector.apply(serverPair.leader).getCurrentLeader()
    );

    // Verify that the other node knows that it is not leader
    Assertions.assertFalse(getLeaderSelector.apply(serverPair.notLeader).isLeader());
    Assertions.assertEquals(
        leaderUri,
        getLeaderSelector.apply(serverPair.notLeader).getCurrentLeader()
    );

    // Verify that other nodes also know which node is leader
    Assertions.assertEquals(
        leaderUri,
        getLeaderSelector.apply(broker).getCurrentLeader()
    );
    Assertions.assertEquals(
        leaderUri,
        getLeaderSelector.apply(indexer).getCurrentLeader()
    );

    // Verify leadership status in the sys.servers table
    final String serverType = serverPair.isCoordinator ? "coordinator" : "overlord";
    Assertions.assertEquals(
        StringUtils.format(
            "%s,0\n%s,1",
            serverPair.notLeader.selfNode().getPlaintextPort(),
            serverPair.leader.selfNode().getPlaintextPort()
        ),
        cluster.runSql(
            "SELECT plaintext_port, is_leader FROM sys.servers WHERE server_type='%s' ORDER BY is_leader",
            serverType
        )
    );
  }

  /**
   * A pair of highly available Coordinator or Overlord nodes where one is leader.
   */
  private static class ServerPair<S extends EmbeddedDruidServer>
  {
    private final S leader;
    private final S notLeader;
    private final boolean isCoordinator;

    ServerPair(S leader, S notLeader)
    {
      this.leader = leader;
      this.notLeader = notLeader;

      if (leader instanceof EmbeddedCoordinator && notLeader instanceof EmbeddedCoordinator) {
        this.isCoordinator = true;
      } else if (leader instanceof EmbeddedOverlord && notLeader instanceof EmbeddedOverlord) {
        this.isCoordinator = false;
      } else {
        throw new ISE("Servers in server pair must either both be Coordinators or both Overlords.");
      }
    }
  }
}
