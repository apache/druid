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

package org.apache.druid.testing.embedded.schema;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.server.StackTraceCollector;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class SystemStackTraceTableTest extends EmbeddedClusterTestBase
{
  private static final String BROKER_PORT = "9082";
  private static final String BROKER_SERVICE = "test/broker";
  private static final String OVERLORD_PORT = "9090";
  private static final String OVERLORD_SERVICE = "test/overlord";
  private static final String COORDINATOR_PORT = "9081";
  private static final String COORDINATOR_SERVICE = "test/coordinator";

  private final EmbeddedBroker broker = new EmbeddedBroker()
      .addProperty("druid.service", BROKER_SERVICE)
      .addProperty("druid.plaintextPort", BROKER_PORT);

  private final EmbeddedOverlord overlord = new EmbeddedOverlord()
      .addProperty("druid.service", OVERLORD_SERVICE)
      .addProperty("druid.plaintextPort", OVERLORD_PORT);

  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator()
      .addProperty("druid.service", COORDINATOR_SERVICE)
      .addProperty("druid.plaintextPort", COORDINATOR_PORT);

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster
        .withZookeeper()
        .addServer(coordinator)
        .addServer(overlord)
        .addServer(broker);
  }

  @Test
  public void test_stackTraceEndpoint()
  {
    final StackTraceCollector.ThreadStackTraceResponse response = cluster.callApi().serviceClient().onAnyBroker(
        mapper -> new RequestBuilder(HttpMethod.GET, "/status/stack"),
        new TypeReference<>(){}
    );

    Assertions.assertNotNull(response.getCollectedAt());
    Assertions.assertFalse(response.getThreads().isEmpty());
    Assertions.assertTrue(
        response.getThreads().stream().allMatch(thread -> !thread.getThreadName().isEmpty())
    );
    Assertions.assertTrue(
        response.getThreads().stream().anyMatch(thread -> !thread.getStackTrace().isEmpty())
    );
    Assertions.assertTrue(
        response.getThreads().stream().allMatch(thread -> countStackFrames(thread.getStackTrace()) <= 100)
    );
  }

  @Test
  public void test_stackTraceEndpointWithMaxStackTraceFrameDepth()
  {
    final StackTraceCollector.ThreadStackTraceResponse response = cluster.callApi().serviceClient().onAnyBroker(
        mapper -> new RequestBuilder(HttpMethod.GET, "/status/stack?maxStackTraceFrameDepth=10"),
        new TypeReference<>(){}
    );

    Assertions.assertTrue(
        response.getThreads().stream().allMatch(thread -> countStackFrames(thread.getStackTrace()) <= 10)
    );
  }

  @Test
  public void test_stackTraceEndpointRejectsInvalidMaxStackTraceFrameDepth()
  {
    for (final String invalidDepth : new String[]{"9", "1001", "10.5"}) {
      final RuntimeException exception = Assertions.assertThrows(
          RuntimeException.class,
          () -> cluster.callApi().serviceClient().onAnyBroker(
              mapper -> new RequestBuilder(
                  HttpMethod.GET,
                  StringUtils.format("/status/stack?maxStackTraceFrameDepth=%s", invalidDepth)
              ),
              new TypeReference<StackTraceCollector.ThreadStackTraceResponse>(){}
          )
      );
      Assertions.assertTrue(exception.getMessage().contains("400 Bad Request"), exception.getMessage());
      Assertions.assertTrue(
          exception.getMessage().contains(StackTraceCollector.MAX_STACK_TRACE_FRAME_DEPTH_KEY),
          exception.getMessage()
      );
    }
  }

  @Test
  public void test_stackTraceTable()
  {
    final String brokerHost = StringUtils.format("localhost:%s", BROKER_PORT);
    final String result = cluster.runSql(
        "SELECT server, service_name, node_roles, collected_at, thread_id, "
        + "thread_state, daemon, priority, cpu_time_ns, user_cpu_time_ns, is_deadlocked, error_message "
        + "FROM sys.stack_trace WHERE server = '%s'",
        brokerHost
    );

    Assertions.assertFalse(result.isEmpty(), "The stack trace table should return broker threads");
    for (final String row : result.split("\\n")) {
      final String[] columns = row.split(",", -1);
      Assertions.assertEquals(brokerHost, columns[0]);
      Assertions.assertEquals(BROKER_SERVICE, columns[1]);
      Assertions.assertEquals("broker", columns[2]);
      Assertions.assertFalse(columns[3].isEmpty());
      assertLong(columns[4]);
      Assertions.assertFalse(columns[6].isEmpty());
      Assertions.assertTrue(columns[6].equals("0") || columns[6].equals("1"), row);
      assertLong(columns[7]);
      if (!columns[8].isEmpty()) {
        assertLong(columns[8]);
      }
      if (!columns[9].isEmpty()) {
        assertLong(columns[9]);
      }
      Assertions.assertTrue(columns[10].equals("0") || columns[10].equals("1"), row);
      Assertions.assertTrue(columns[11].isEmpty());
    }

    Assertions.assertFalse(
        cluster.runSql("SELECT thread_name FROM sys.stack_trace WHERE server = '%s' LIMIT 1", brokerHost).isEmpty()
    );

    Assertions.assertFalse(
        cluster.runSql(
            "SELECT server FROM sys.stack_trace WHERE server = '%s' AND node_roles = 'broker' LIMIT 1",
            brokerHost
        ).isEmpty()
    );

    Assertions.assertFalse(
        cluster.runSql("SELECT stack FROM sys.stack_trace WHERE server = '%s' LIMIT 1", brokerHost).isEmpty()
    );
  }

  @Test
  public void test_stackTraceTableWithMaxStackTraceFrameDepth()
  {
    final String brokerHost = StringUtils.format("localhost:%s", BROKER_PORT);
    final String result = cluster.callApi().onAnyBroker(
        broker -> broker.submitSqlQuery(
            new ClientSqlQuery(
                StringUtils.format(
                    "SELECT stack FROM sys.stack_trace WHERE server = '%s' LIMIT 1",
                    brokerHost
                ),
                ResultFormat.CSV.name(),
                false,
                false,
                false,
                Map.of(StackTraceCollector.MAX_STACK_TRACE_FRAME_DEPTH_KEY, 10.9),
                null
            )
        )
    ).trim();

    Assertions.assertFalse(result.isEmpty());
    Assertions.assertTrue(countStackFrames(result) <= 10);
  }

  @Test
  public void test_stackTraceTable_requiresServerFilter()
  {
    final RuntimeException exception = Assertions.assertThrows(
        RuntimeException.class,
        () -> cluster.runSql("SELECT COUNT(*) FROM sys.stack_trace")
    );
    Assertions.assertTrue(exception.getMessage().contains("400 Bad Request"), exception.getMessage());
    Assertions.assertTrue(exception.getMessage().contains("requires a filter on the server column"));
  }

  @Test
  public void test_stackTraceTable_inFilter()
  {
    final String brokerHost = StringUtils.format("localhost:%s", BROKER_PORT);
    final String coordinatorHost = StringUtils.format("localhost:%s", COORDINATOR_PORT);
    final String result = cluster.runSql(
        "SELECT DISTINCT server FROM sys.stack_trace WHERE server IN ('%s', '%s')",
        brokerHost,
        coordinatorHost
    );

    Assertions.assertTrue(result.contains(brokerHost));
    Assertions.assertTrue(result.contains(coordinatorHost));
    Assertions.assertFalse(result.contains(StringUtils.format("localhost:%s", OVERLORD_PORT)));
  }

  private static long countStackFrames(final String stackTrace)
  {
    return stackTrace.lines().filter(line -> line.startsWith("\tat ")).count();
  }

  private static void assertLong(final String value)
  {
    try {
      Long.parseLong(value);
    }
    catch (NumberFormatException e) {
      Assertions.fail("Expected a long value but got[" + value + "]", e);
    }
  }
}
