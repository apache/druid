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

import org.apache.druid.data.input.s3.S3InputSourceDruidModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.testing.embedded.minio.MinIOStorageResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/**
 * Tests MSQ task-based SELECT queries that read external data via {@code EXTERN} from S3 (backed by MinIO).
 */
public class S3ExternQueryTest extends EmbeddedClusterTestBase
{
  /**
   * Base key under the bucket where the external input files are uploaded.
   */
  private static final String DATA_PATH = "extern-input";
  private static final String PLAIN_FILE = "data1.json";
  private static final String GZ_FILE = "data2.json.gz";

  private static final String PLAIN_FILE_CONTENT = """
      {"timestamp":"2020-01-01T00:00:00Z","page":"A","added":10}
      {"timestamp":"2020-01-01T01:00:00Z","page":"B","added":20}
      """;

  private static final String GZ_FILE_CONTENT = """
      {"timestamp":"2020-01-02T00:00:00Z","page":"C","added":30}
      {"timestamp":"2020-01-02T01:00:00Z","page":"D","added":40}
      {"timestamp":"2020-01-02T02:00:00Z","page":"E","added":50}
      """;

  /**
   * Total number of bytes uploaded across {@link #PLAIN_FILE} and {@link #GZ_FILE}. Populated by
   * {@link #uploadExternalFiles()} and used to verify the VSF {@code loadBytes} counter.
   */
  private long totalUploadedBytes;

  private final MinIOStorageResource storageResource = new MinIOStorageResource();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer()
      .setServerMemory(300_000_000L)
      .addProperty("druid.worker.capacity", "2");
  private final EmbeddedBroker broker = new EmbeddedBroker().setServerMemory(200_000_000);

  private EmbeddedMSQApis msqApis;

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .useLatchableEmitter()
        .addResource(storageResource)
        .addExtension(S3InputSourceDruidModule.class)
        .addServer(overlord)
        .addServer(coordinator)
        .addServer(indexer)
        .addServer(broker);
  }

  @BeforeAll
  public void setupCluster() throws IOException
  {
    msqApis = new EmbeddedMSQApis(cluster, overlord);
    uploadExternalFiles();
  }

  @Test
  public void test_extern_backgroundFetchEnabled()
  {
    runQueryAndVerify(true);
  }

  @Test
  public void test_extern_backgroundFetchDisabled()
  {
    runQueryAndVerify(false);
  }

  private void runQueryAndVerify(final boolean backgroundFetchExternalFiles)
  {
    final String inputSourceJson = StringUtils.format(
        "{\"type\":\"s3\",\"uris\":[\"s3://%s/%s/%s\",\"s3://%s/%s/%s\"]}",
        storageResource.getBucket(), DATA_PATH, PLAIN_FILE,
        storageResource.getBucket(), DATA_PATH, GZ_FILE
    );

    final String sql = StringUtils.format(
        """
            SET backgroundFetchExternalFiles = %s;
            SELECT page, added
            FROM TABLE(
              EXTERN(
                '%s',
                '{"type":"json"}'
              )
            ) EXTEND ("page" VARCHAR, "added" BIGINT)
            ORDER BY page
            """,
        backgroundFetchExternalFiles,
        inputSourceJson
    );

    final MSQTaskReportPayload report = msqApis.runTaskSqlAndGetReport(sql);

    BaseCalciteQueryTest.assertResultsEquals(
        sql,
        List.of(
            new Object[]{"A", 10},
            new Object[]{"B", 20},
            new Object[]{"C", 30},
            new Object[]{"D", 40},
            new Object[]{"E", 50}
        ),
        report.getResults().getResults()
    );

    // Verify input counters.
    final EmbeddedMSQApis.ChannelSums channelSums = msqApis.getInputChannelSums(report, 0);
    Assertions.assertEquals(2, channelSums.files(), "files");
    Assertions.assertEquals(2, channelSums.totalFiles(), "totalFiles");
    Assertions.assertEquals(5, channelSums.rows(), "rows");
    Assertions.assertEquals(PLAIN_FILE_CONTENT.length() + GZ_FILE_CONTENT.length(), channelSums.bytes(), "bytes");
    Assertions.assertEquals(0, channelSums.queries(), "queries");
    Assertions.assertEquals(0, channelSums.queries(), "totalQueries");

    // Verify load counters.
    if (backgroundFetchExternalFiles) {
      Assertions.assertEquals(totalUploadedBytes, channelSums.loadBytes(), "VSF bytes loaded");
      Assertions.assertEquals(2, channelSums.loadFiles(), "VSF files loaded");
      Assertions.assertTrue(channelSums.loadTime() >= 0, "VSF load time");
      Assertions.assertEquals(0, channelSums.loadWait(), "VSF load wait time");
    } else {
      Assertions.assertEquals(0, channelSums.loadBytes(), "VSF bytes loaded");
      Assertions.assertEquals(0, channelSums.loadFiles(), "VSF files loaded");
      Assertions.assertEquals(0, channelSums.loadTime(), "VSF load time");
      Assertions.assertEquals(0, channelSums.loadWait(), "VSF load wait time");
    }
  }

  /**
   * Uploads {@link #PLAIN_FILE} (plain JSON) and {@link #GZ_FILE} (gzipped JSON) to the MinIO bucket.
   */
  private void uploadExternalFiles() throws IOException
  {
    final S3Client s3Client = storageResource.getS3Client();

    final byte[] plainBytes = PLAIN_FILE_CONTENT.getBytes(StandardCharsets.UTF_8);
    final byte[] gzBytes = gzip(GZ_FILE_CONTENT);
    totalUploadedBytes += plainBytes.length + gzBytes.length;

    s3Client.putObject(
        PutObjectRequest.builder()
                        .bucket(storageResource.getBucket())
                        .key(DATA_PATH + "/" + PLAIN_FILE)
                        .build(),
        RequestBody.fromBytes(plainBytes)
    );

    s3Client.putObject(
        PutObjectRequest.builder()
                        .bucket(storageResource.getBucket())
                        .key(DATA_PATH + "/" + GZ_FILE)
                        .build(),
        RequestBody.fromBytes(gzBytes)
    );
  }

  private static byte[] gzip(final String content) throws IOException
  {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (final GZIPOutputStream gzipStream = new GZIPOutputStream(baos)) {
      gzipStream.write(StringUtils.toUtf8(content));
    }
    return baos.toByteArray();
  }
}
