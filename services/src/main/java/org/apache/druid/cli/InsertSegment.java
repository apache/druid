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

package org.apache.druid.cli;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.druid.guice.DruidProcessingModule;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.QueryRunnerFactoryModule;
import org.apache.druid.guice.QueryableModule;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.loading.DataSegmentFinder;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.server.DruidNode;
import org.apache.druid.timeline.DataSegment;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 */
@Command(
    name = "insert-segment-to-db",
    description = "insert a segment into metadata storage"
)
public class InsertSegment extends GuiceRunnable
{
  private static final Logger log = new Logger(InsertSegment.class);

  @Option(name = "--workingDir", description = "The directory path where segments are stored. This tool will recursively look for segments underneath this directory and insert/update these segments in metdata storage.", required = true)
  private String workingDirPath;

  @Option(name = "--updateDescriptor", description = "if set to true, this tool will update loadSpec field in descriptor.json (partitionNum_descriptor.json for HDFS data storage) if the path in loadSpec is different from where desciptor.json (partitionNum_descriptor.json for HDFS data storage) was found. Default value is true", required = false)
  private String updateDescriptor;

  private IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;

  public InsertSegment()
  {
    super(log);
  }

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.of(
        // It's unknown if those modules are required in InsertSegment.
        // Maybe some of those modules could be removed.
        // See https://github.com/apache/incubator-druid/pull/4429#discussion_r123603498
        new DruidProcessingModule(),
        new QueryableModule(),
        new QueryRunnerFactoryModule(),
        binder -> JsonConfigProvider.bindInstance(
            binder,
            Key.get(DruidNode.class, Self.class),
            new DruidNode("tools", "localhost", false, -1, null, true, false)
        )
    );
  }

  @Override
  public void run()
  {
    final Injector injector = makeInjector();
    indexerMetadataStorageCoordinator = injector.getInstance(IndexerMetadataStorageCoordinator.class);
    final DataSegmentFinder dataSegmentFinder = injector.getInstance(DataSegmentFinder.class);

    log.info("Start searching segments under [%s]", workingDirPath);

    Set<DataSegment> segments = null;
    try {
      segments = dataSegmentFinder.findSegments(workingDirPath, Boolean.valueOf(updateDescriptor));
    }
    catch (SegmentLoadingException e) {
      Throwables.propagate(e);
    }

    log.info(
        "Done searching segments under [%s], [%d] segments were found",
        workingDirPath,
        segments.size()
    );

    try {
      insertSegments(segments);
    }
    catch (IOException e) {
      Throwables.propagate(e);
    }

    log.info("Done processing [%d] segments", segments.size());
  }

  private void insertSegments(final Set<DataSegment> segments) throws IOException
  {
    final Set<DataSegment> segmentsInserted = indexerMetadataStorageCoordinator.announceHistoricalSegments(segments);
    for (DataSegment dataSegment : segmentsInserted) {
      log.info("Sucessfully inserted Segment [%s] into metadata storage", dataSegment.getId());
    }
    final Set<DataSegment> segmentsAlreadyExist = Sets.difference(segments, segmentsInserted);
    if (!segmentsAlreadyExist.isEmpty()) {
      for (DataSegment dataSegment : segmentsAlreadyExist) {
        log.info("Segment [%s] already exists in metadata storage, updating the payload", dataSegment.getId());
      }
      indexerMetadataStorageCoordinator.updateSegmentMetadata(segmentsAlreadyExist);
    }
  }
}
