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

package org.apache.druid.testing.embedded.msq;

import com.sun.net.httpserver.HttpServer;
import org.apache.druid.data.input.parquet.ParquetExtensionsModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.http.SqlTaskStatus;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Ingests a Parquet file served over HTTP through an MSQ task, confirming end to end success.
 *
 * <p>Reading from a remote source such as HTTP or S3 exercises this fetch-to-disk path; a {@code local} input source
 * does not, because its file is already on disk and is read in place.
 */
public class EmbeddedMSQParquetIngestionTest extends EmbeddedClusterTestBase
{
  private static final String PARQUET_RESOURCE = "data/parquet/wikipedia_index_data1.parquet";
  private static final String PARQUET_PATH = "/wikipedia_index_data1.parquet";

  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer()
      .setServerMemory(300_000_000L)
      .addProperty("druid.worker.capacity", "2");
  private final EmbeddedBroker broker = new EmbeddedBroker().setServerMemory(200_000_000);

  private EmbeddedMSQApis msqApis;
  private HttpServer parquetServer;
  private String parquetUri;

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .useLatchableEmitter()
        .addExtension(ParquetExtensionsModule.class)
        .addServer(overlord)
        .addServer(coordinator)
        .addServer(indexer)
        .addServer(broker)
        .addServer(new EmbeddedHistorical());
  }

  @BeforeAll
  public void initTestClient() throws IOException
  {
    msqApis = new EmbeddedMSQApis(cluster, overlord);

    final Path parquet = Resources.getFileForResource(PARQUET_RESOURCE).toPath();
    final byte[] parquetBytes = Files.readAllBytes(parquet);

    final int port;
    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    }
    parquetServer = HttpServer.create(new InetSocketAddress("localhost", port), 0);
    parquetServer.createContext(
        PARQUET_PATH,
        httpExchange -> {
          httpExchange.sendResponseHeaders(200, parquetBytes.length);
          try (OutputStream os = httpExchange.getResponseBody()) {
            os.write(parquetBytes);
          }
        }
    );
    parquetServer.start();
    parquetUri = StringUtils.format(
        "http://%s:%d%s",
        parquetServer.getAddress().getHostName(),
        parquetServer.getAddress().getPort(),
        PARQUET_PATH
    );
  }

  @AfterAll
  public void tearDownServer()
  {
    if (parquetServer != null) {
      parquetServer.stop(0);
    }
  }

  @Test
  public void testReplaceFromExternalParquet()
  {
    final String sql = StringUtils.format(
        "REPLACE INTO %s OVERWRITE ALL\n"
        + "SELECT\n"
        + "  TIME_PARSE(\"timestamp\") AS __time,\n"
        + "  \"page\",\n"
        + "  \"added\",\n"
        + "  \"delta\",\n"
        + "  \"deleted\",\n"
        + "  \"namespace\"\n"
        + "FROM TABLE(\n"
        + "  EXTERN(\n"
        + "    '{\"type\":\"http\",\"uris\":[\"%s\"]}',\n"
        + "    '{\"type\":\"parquet\"}'\n"
        + "  )\n"
        + ") EXTEND (\n"
        + "  \"timestamp\" VARCHAR,\n"
        + "  \"page\" VARCHAR,\n"
        + "  \"added\" BIGINT,\n"
        + "  \"delta\" BIGINT,\n"
        + "  \"deleted\" BIGINT,\n"
        + "  \"namespace\" VARCHAR\n"
        + ")\n"
        + "PARTITIONED BY DAY",
        dataSource,
        parquetUri
    );

    final SqlTaskStatus taskStatus = msqApis.submitTaskSql(sql);
    cluster.callApi().waitForTaskToSucceed(taskStatus.getTaskId(), overlord.latchableEmitter());
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    cluster.callApi().verifySqlQuery(
        "SELECT __time, page, added, delta, deleted, namespace FROM %s ORDER BY __time",
        dataSource,
        "2013-08-31T01:02:33.000Z,Gypsy Danger,57,-143,200,article\n"
        + "2013-08-31T03:32:45.000Z,Striker Eureka,459,330,129,wikipedia\n"
        + "2013-08-31T07:11:21.000Z,Cherno Alpha,123,111,12,article"
    );
  }
}
