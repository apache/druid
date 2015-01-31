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

package io.druid.cli.convert;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.logger.Logger;
import io.airlift.command.Command;
import io.airlift.command.Option;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 */
@Command(
    name = "convertProps",
    description = "Converts runtime.properties files from version 0.5 to 0.6"
)
public class ConvertProperties implements Runnable
{
  private static final Logger log = new Logger(ConvertProperties.class);

  private static final List<PropertyConverter> converters = Lists.newArrayList(
      new DatabasePropertiesConverter(),
      new Rename("druid.database.rules.defaultDatasource", "druid.manager.rules.defaultTier"),
      new Rename("druid.zk.paths.discoveryPath", "druid.discovery.curator.path"),
      new Rename("druid.http.numThreads", "druid.server.http.numThreads"),
      new Rename("druid.http.maxIdleTimeMillis", "druid.server.http.maxIdleTime"),
      new Rename("druid.database.connectURI", "druid.db.connector.connectURI"),
      new Rename("druid.database.user", "druid.db.connector.user"),
      new Rename("druid.database.password", "druid.db.connector.password"),
      new Rename("druid.database.poll.duration", "druid.manager.segment.pollDuration"),
      new Rename("druid.database.password", "druid.db.connector.password"),
      new Rename("druid.database.validation", "druid.db.connector.useValidationQuery"),
      new Rename("com.metamx.emitter", "druid.emitter"),
      new Rename("com.metamx.emitter.logging", "druid.emitter.logging"),
      new Rename("com.metamx.emitter.logging.level", "druid.emitter.logging.logLevel"),
      new Rename("com.metamx.emitter.http", "druid.emitter.http"),
      new Rename("com.metamx.emitter.http.url", "druid.emitter.http.recipientBaseUrl"),
      new Rename("com.metamx.emitter.period", "druid.monitoring.emissionPeriod"),
      new Rename("com.metamx.druid.emitter.period", "druid.monitoring.emissionPeriod"),
      new Rename("com.metamx.metrics.emitter.period", "druid.monitoring.emissionPeriod"),
      new PrefixRename("com.metamx.emitter", "druid.emitter"),
      new PrefixRename("com.metamx.druid.emitter", "druid.emitter"),
      new IndexCacheConverter(),
      new Rename("druid.paths.segmentInfoCache", "druid.segmentCache.infoDir"),
      new Rename("com.metamx.aws.accessKey", "druid.s3.accessKey"),
      new Rename("com.metamx.aws.secretKey", "druid.s3.secretKey"),
      new Rename("druid.bard.maxIntervalDuration", "druid.query.chunkDuration"),
      new PrefixRename("druid.bard.cache", "druid.broker.cache"),
      new Rename("druid.client.http.connections", "druid.broker.http.numConnections"),
      new Rename("com.metamx.query.groupBy.maxResults", "druid.query.groupBy.maxResults"),
      new Rename("com.metamx.query.search.maxSearchLimit", "druid.query.search.maxSearchLimit"),
      new Rename("druid.indexer.runner", "druid.indexer.runner.type"),
      new Rename("druid.indexer.storage", "druid.indexer.storage.type"),
      new Rename("druid.indexer.threads", "druid.indexer.runner.forks"),
      new Rename("druid.indexer.taskDir", "druid.indexer.runner.taskDir"),
      new Rename("druid.indexer.fork.java", "druid.indexer.runner.javaCommand"),
      new Rename("druid.indexer.fork.opts", "druid.indexer.runner.javaOpts"),
      new Rename("druid.indexer.fork.classpath", "druid.indexer.runner.classpath"),
      new Rename("druid.indexer.fork.hostpattern", "druid.indexer.runner.hostPattern"),
      new Rename("druid.indexer.fork.startport", "druid.indexer.runner.startPort"),
      new Rename("druid.indexer.properties.prefixes", "druid.indexer.runner.allowedPrefixes"),
      new Rename("druid.indexer.taskAssignmentTimeoutDuration", "druid.indexer.runner.taskAssignmentTimeout"),
      new Rename("druid.indexer.worker.version", "druid.indexer.runner.minWorkerVersion"),
      new Rename("druid.zk.maxNumBytes", "druid.indexer.runner.maxZnodeBytes"),
      new Rename("druid.indexer.provisionResources.duration", "druid.indexer.autoscale.provisionPeriod"),
      new Rename("druid.indexer.terminateResources.duration", "druid.indexer.autoscale.terminatePeriod"),
      new Rename("druid.indexer.terminateResources.originDateTime", "druid.indexer.autoscale.originTime"),
      new Rename("druid.indexer.autoscaling.strategy", "druid.indexer.autoscale.strategy"),
      new Rename("druid.indexer.logs.s3bucket", "druid.indexer.logs.s3Bucket"),
      new Rename("druid.indexer.logs.s3prefix", "druid.indexer.logs.s3Prefix"),
      new Rename("druid.indexer.maxWorkerIdleTimeMillisBeforeDeletion", "druid.indexer.autoscale.workerIdleTimeout"),
      new Rename("druid.indexer.maxScalingDuration", "druid.indexer.autoscale.scalingTimeout"),
      new Rename("druid.indexer.numEventsToTrack", "druid.indexer.autoscale.numEventsToTrack"),
      new Rename("druid.indexer.maxPendingTaskDuration", "druid.indexer.autoscale.pendingTaskTimeout"),
      new Rename("druid.indexer.worker.version", "druid.indexer.autoscale.workerVersion"),
      new Rename("druid.indexer.worker.port", "druid.indexer.autoscale.workerPort"),
      new Rename("druid.worker.masterService", "druid.selectors.indexing.serviceName"),
      new ChatHandlerConverter(),
      new Rename("druid.indexer.baseDir", "druid.indexer.task.baseDir"),
      new Rename("druid.indexer.taskDir", "druid.indexer.task.taskDir"),
      new Rename("druid.indexer.hadoopWorkingPath", "druid.indexer.task.hadoopWorkingPath"),
      new Rename("druid.indexer.rowFlushBoundary", "druid.indexer.task.rowFlushBoundary"),
      new Rename("druid.worker.taskActionClient.retry.minWaitMillis", "druid.worker.taskActionClient.retry.minWait"),
      new Rename("druid.worker.taskActionClient.retry.maxWaitMillis", "druid.worker.taskActionClient.retry.maxWait"),
      new Rename("druid.master.merger.service", "druid.selectors.indexing.serviceName"),
      new Rename("druid.master.period.segmentMerger", "druid.coordinator.period.indexingPeriod"),
      new Rename("druid.master.merger.on", "druid.coordinator.merge.on"),
      new Rename("druid.master.period", "druid.coordinator.period"),
      new PrefixRename("druid.master", "druid.coordinator"),
      new PrefixRename("druid.pusher", "druid.storage"),
      new DataSegmentPusherDefaultConverter(),
      new Rename("druid.pusher.hdfs.storageDirectory", "druid.storage.storageDirectory"),
      new Rename("druid.pusher.cassandra.host", "druid.storage.host"),
      new Rename("druid.pusher.cassandra.keySpace", "druid.storage.keySpace")
  );

  @Option(name = "-f", title = "file", description = "The properties file to convert", required = true)
  public String filename;

  @Option(name = "-o", title = "outFile", description = "The file to write updated properties to.", required = true)
  public String outFilename;

  @Override
  public void run()
  {
    File file = new File(filename);
    if (!file.exists()) {
      System.out.printf("File[%s] does not exist.%n", file);
    }

    File outFile = new File(outFilename);
    if (outFile.getParentFile() != null && !outFile.getParentFile().exists()) {
      outFile.getParentFile().mkdirs();
    }

    Properties fromFile = new Properties();

    try (Reader in = new InputStreamReader(new FileInputStream(file), Charsets.UTF_8)) {
      fromFile.load(in);
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }

    Properties updatedProps = new Properties();

    int count = 0;
    for (String property : fromFile.stringPropertyNames()) {
      boolean handled = false;
      for (PropertyConverter converter : converters) {
        if (converter.canHandle(property)) {
          for (Map.Entry<String, String> entry : converter.convert(fromFile).entrySet()) {
            if (entry.getValue() != null) {
              ++count;
              log.info("Converting [%s] to [%s]", property, entry.getKey());
              updatedProps.setProperty(entry.getKey(), entry.getValue());
            }
          }
          handled = true;
        }
      }

      if (!handled) {
        log.info("Not converting [%s]", property);
        updatedProps.put(property, fromFile.getProperty(property));
      }
    }

    updatedProps.setProperty(
        "druid.monitoring.monitors", "[\"com.metamx.metrics.SysMonitor\"]"
    );

    BufferedWriter out = null;
    try {
      out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile), Charsets.UTF_8));
      for (Map.Entry<Object, Object> prop : updatedProps.entrySet()) {
        out.write((String) prop.getKey());
        out.write("=");
        out.write((String) prop.getValue());
        out.newLine();
      }
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
    finally {
      if (out != null) {
        CloseQuietly.close(out);
      }
    }

    log.info("Completed!  Converted[%,d] properties.", count);
  }
}
