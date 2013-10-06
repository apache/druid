/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.cli.convert;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.metamx.common.logger.Logger;
import io.airlift.command.Command;
import io.airlift.command.Option;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
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
      new Rename("com.metamx.emitter", "druid.emitter"),
      new Rename("com.metamx.emitter.logging", "druid.emitter.logging"),
      new Rename("com.metamx.emitter.logging.level", "druid.emitter.logging.logLevel"),
      new Rename("com.metamx.emitter.http", "druid.emitter.http"),
      new Rename("com.metamx.emitter.http.url", "druid.emitter.http.url"),
      new Rename("com.metamx.druid.emitter.period", "druid.emitter.emissionPeriod"),
      new PrefixRename("com.metamx.emitter", "druid.emitter"),
      new PrefixRename("com.metamx.druid.emitter", "druid.emitter"),
      new IndexCacheConverter(),
      new Rename("druid.paths.segmentInfoCache", "druid.segmentCache.infoPath"),
      new Rename("com.metamx.aws.accessKey", "druid.s3.accessKey"),
      new Rename("com.metamx.aws.secretKey", "druid.s3.secretKey"),
      new Rename("druid.bard.maxIntervalDuration", "druid.query.chunkDuration"),
      new PrefixRename("druid.bard.cache", "druid.broker.cache"),
      new Rename("druid.client.http.connections", "druid.broker.http.numConnections"),
      new Rename("com.metamx.query.groupBy.maxResults", "druid.query.groupBy.maxResults"),
      new Rename("com.metamx.query.search.maxSearchLimit", "druid.query.search.maxSearchLimit"),
      new Rename("druid.indexer.storage", "druid.indexer.storage.type"),
      new Rename("druid.indexer.threads", "druid.indexer.runner.forks"),
      new Rename("druid.indexer.taskDir", "druid.indexer.runner.taskDir"),
      new Rename("druid.indexer.fork.java", "druid.indexer.runner.javaCommand"),
      new Rename("druid.indexer.fork.opts", "druid.indexer.runner.javaOpts"),
      new Rename("druid.indexer.fork.classpath", "druid.indexer.runner.classpath"),
      new Rename("druid.indexer.fork.main", "druid.indexer.runner.mainClass"),
      new Rename("druid.indexer.fork.hostpattern", "druid.indexer.runner.hostPattern"),
      new Rename("druid.indexer.fork.startport", "druid.indexer.runner.startPort"),
      new Rename("druid.indexer.properties.prefixes", "druid.indexer.runner.allowedPrefixes"),
      new Rename("druid.indexer.taskAssignmentTimeoutDuration", "druid.indexer.runner.taskAssignmentTimeout"),
      new Rename("druid.indexer.worker.version", "druid.indexer.runner.workerVersion"),
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
      new Rename("druid.master.merger.on", "druid.coordinator.merge.on"),
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

    try (Reader in = new InputStreamReader(new FileInputStream(file), Charsets.UTF_8))
    {
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
              updatedProps.setProperty(entry.getKey(), entry.getValue());
            }
          }
          handled = true;
        }
      }

      if (!handled) {
        updatedProps.put(property, fromFile.getProperty(property));
      }
    }

    updatedProps.setProperty(
        "druid.monitoring.monitors", "[\"io.druid.server.metrics.ServerMonitor\", \"com.metamx.metrics.SysMonitor\"]"
    );

    try (Writer out = new OutputStreamWriter(new FileOutputStream(outFile), Charsets.UTF_8))
    {
      updatedProps.store(out, null);
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }

    log.info("Completed!  Converted[%,d] properties.", count);
  }
}
