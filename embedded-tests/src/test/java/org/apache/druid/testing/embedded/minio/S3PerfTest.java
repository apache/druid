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

package org.apache.druid.testing.embedded.minio;

import org.apache.druid.common.aws.AWSModule;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.TaskBuilder;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.storage.s3.S3StorageDruidModule;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * S3 segment-upload performance tests.
 *
 * <p>Each {@link Nested} inner class runs a full embedded cluster with a different
 * storage backend / HTTP client combination:
 * <ul>
 *   <li>MinIO container — CRT (default), Netty NIO, synchronous PutObject</li>
 *   <li>Real AWS S3 (opt-in via {@value BUCKET_PROPERTY}) — same three variants</li>
 * </ul>
 *
 * <p>Real-S3 tests are skipped unless {@code -Ds3.test.bucket=<bucket>} is set.
 * Data is written under {@code perf-test/<run-timestamp>/} and must be cleaned up manually.
 */
@Tag("perf")
public class S3PerfTest
{
  private static final Logger log = new Logger(S3PerfTest.class);

  private static final int TASK_COUNT = 5;
  private static final int ROWS_PER_TASK = 200_000;
  private static final long MS_PER_DAY = 86_400_000L;

  private static final String BUCKET_PROPERTY = "s3.test.bucket";
  private static final String REGION_PROPERTY = "s3.test.region";
  private static final String DEFAULT_REGION = "us-east-1";

  abstract static class PerfTestBase extends EmbeddedClusterTestBase
  {
    private final List<File> tempFiles = new ArrayList<>();

    final EmbeddedOverlord overlord = new EmbeddedOverlord();
    final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
    final EmbeddedIndexer indexer = new EmbeddedIndexer().addProperty("druid.worker.capacity", "10");
    final EmbeddedHistorical historical = new EmbeddedHistorical();
    final EmbeddedBroker broker = new EmbeddedBroker();

    EmbeddedDruidCluster newBaseCluster()
    {
      return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                                 .useLatchableEmitter()
                                 .useDefaultTimeoutForLatchableEmitter(300);
    }

    EmbeddedDruidCluster addServers(EmbeddedDruidCluster cluster)
    {
      return cluster.addServer(overlord)
                    .addServer(coordinator)
                    .addServer(indexer)
                    .addServer(historical)
                    .addServer(broker)
                    .addServer(new EmbeddedRouter());
    }

    @BeforeAll
    void generateInputFiles(@TempDir Path tempDir) throws IOException
    {
      final DateTime baseDay = DateTimes.of("2025-01-01");
      for (int i = 0; i < TASK_COUNT; i++) {
        final File f = generateLargeCsvFile(tempDir, baseDay.plusDays(i), ROWS_PER_TASK);
        tempFiles.add(f);
        log.info("Generated %s (%d MB)", f.getName(), f.length() / (1024 * 1024));
      }
    }

    @Test
    @Timeout(value = 20, unit = TimeUnit.MINUTES)
    public void test_concurrentUploads()
    {
      final List<String> taskIds = new ArrayList<>();
      for (int i = 0; i < TASK_COUNT; i++) {
        taskIds.add(EmbeddedClusterApis.newTaskId(dataSource));
      }

      log.info("Starting %d concurrent tasks (%d rows each)", TASK_COUNT, ROWS_PER_TASK);
      final long startNanos = System.nanoTime();

      for (int i = 0; i < taskIds.size(); i++) {
        final String taskId = taskIds.get(i);
        final File csvFile = tempFiles.get(i);
        final IndexTask task = TaskBuilder
            .ofTypeIndex()
            .isoTimestampColumn("time")
            .csvInputFormatWithColumns("time", "item", "value", "description")
            .localInputSourceWithFiles(csvFile)
            .segmentGranularity("DAY")
            .dimensions()
            .dataSource(dataSource)
            .withId(taskId);
        cluster.callApi().onLeaderOverlord(o -> o.runTask(taskId, task));
      }

      for (String taskId : taskIds) {
        cluster.callApi().waitForTaskToSucceed(taskId, overlord);
      }

      final long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
      log.info(
          "[%s] tasks=[%d] rowsPerTask=[%d] fileSizeMb=[%d] elapsed=[%d ms]",
          getClass().getSimpleName(), TASK_COUNT, ROWS_PER_TASK, tempFiles.get(0).length() / (1024 * 1024), elapsedMs
      );
    }
  }

  abstract static class MinIOPerfBase extends PerfTestBase
  {
    abstract void configureCluster(EmbeddedDruidCluster cluster);

    @Override
    public EmbeddedDruidCluster createCluster()
    {
      final EmbeddedDruidCluster cluster = addServers(newBaseCluster().addResource(new MinIOStorageResource()));
      configureCluster(cluster);
      return cluster;
    }
  }

  /** MinIO + Amazon CRT async HTTP client (default). */
  @Nested
  static class MinIO_Crt extends MinIOPerfBase
  {
    @Override
    void configureCluster(EmbeddedDruidCluster cluster)
    {
    }
  }

  /** MinIO + Netty NIO async HTTP client. */
  @Nested
  static class MinIO_Netty extends MinIOPerfBase
  {
    @Override
    void configureCluster(EmbeddedDruidCluster cluster)
    {
      cluster.addCommonProperty("druid.storage.transfer.asyncHttpClientType", "netty");
    }
  }

  /** MinIO + synchronous PutObject (transfer manager disabled). */
  @Nested
  static class MinIO_Sync extends MinIOPerfBase
  {
    @Override
    void configureCluster(EmbeddedDruidCluster cluster)
    {
      cluster.addCommonProperty("druid.storage.transfer.useTransferManager", "false");
    }
  }

  abstract static class RealS3PerfBase extends PerfTestBase
  {
    abstract void configureCluster(EmbeddedDruidCluster cluster);

    @Override
    public EmbeddedDruidCluster createCluster()
    {
      final String bucket = System.getProperty(BUCKET_PROPERTY);
      Assumptions.assumeTrue(
          bucket != null,
          "Skipping real S3 perf test: set -D" + BUCKET_PROPERTY + "=<bucket> to enable"
      );

      final String region = System.getProperty(REGION_PROPERTY, DEFAULT_REGION);
      final String baseKey = "perf-test/" + System.currentTimeMillis();

      final EmbeddedDruidCluster cluster = newBaseCluster()
          .addExtension(S3StorageDruidModule.class)
          .addExtension(AWSModule.class);

      cluster.addCommonProperty("druid.storage.type", "s3");
      cluster.addCommonProperty("druid.storage.bucket", bucket);
      cluster.addCommonProperty("druid.storage.baseKey", baseKey);
      cluster.addCommonProperty("druid.indexer.logs.type", "s3");
      cluster.addCommonProperty("druid.indexer.logs.s3Bucket", bucket);
      cluster.addCommonProperty("druid.indexer.logs.s3Prefix", baseKey + "/logs");
      cluster.addCommonProperty("druid.s3.endpoint.signingRegion", region);

      configureCluster(cluster);
      addServers(cluster);
      return cluster;
    }
  }

  /** Real S3 + Amazon CRT async HTTP client (default). */
  @Nested
  static class RealS3_Crt extends RealS3PerfBase
  {
    @Override
    void configureCluster(EmbeddedDruidCluster cluster)
    {
    }
  }

  /** Real S3 + Netty NIO async HTTP client. */
  @Nested
  static class RealS3_Netty extends RealS3PerfBase
  {
    @Override
    void configureCluster(EmbeddedDruidCluster cluster)
    {
      cluster.addCommonProperty("druid.storage.transfer.asyncHttpClientType", "netty");
    }
  }

  /** Real S3 + synchronous PutObject (transfer manager disabled). */
  @Nested
  static class RealS3_Sync extends RealS3PerfBase
  {
    @Override
    void configureCluster(EmbeddedDruidCluster cluster)
    {
      cluster.addCommonProperty("druid.storage.transfer.useTransferManager", "false");
    }
  }

  private static File generateLargeCsvFile(Path dir, DateTime day, int rowCount) throws IOException
  {
    final File file = dir.resolve("perf-" + day.toString("yyyy-MM-dd") + ".csv").toFile();
    final Random rng = new Random(day.getMillis());
    try (BufferedWriter w = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8)) {
      for (int i = 0; i < rowCount; i++) {
        w.write(rowTimestamp(day, i, rowCount));
        w.write(',');
        w.write("item_");
        w.write(Integer.toString(i));
        w.write(',');
        w.write(Long.toString(randomValue(rng)));
        w.write(',');
        w.write(randomDescription(rng));
        w.write('\n');
      }
    }
    return file;
  }

  private static String rowTimestamp(DateTime day, int row, int rowCount)
  {
    return day.plusMillis((int) (row * MS_PER_DAY / rowCount)).toString();
  }

  private static long randomValue(Random rng)
  {
    return (rng.nextLong() >>> 1) % 1_000_000L;
  }

  /** 13 × 16 hex chars = 208 chars of random data, resistant to compression. */
  private static String randomDescription(Random rng)
  {
    final StringBuilder sb = new StringBuilder(208);
    for (int j = 0; j < 13; j++) {
      sb.append(String.format(Locale.ROOT, "%016x", rng.nextLong()));
    }
    return sb.toString();
  }
}
