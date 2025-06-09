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

package org.apache.druid.msq.dart.controller;

import com.google.common.collect.FluentIterable;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.druid.client.QueryableDruidServer;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.msq.dart.worker.DartQueryableSegment;
import org.apache.druid.msq.dart.worker.WorkerId;
import org.apache.druid.msq.exec.SegmentSource;
import org.apache.druid.msq.exec.WorkerManager;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.input.InputSpecSlicer;
import org.apache.druid.msq.input.NilInputSlice;
import org.apache.druid.msq.input.table.DataServerRequestDescriptor;
import org.apache.druid.msq.input.table.DataServerSelector;
import org.apache.druid.msq.input.table.RichSegmentDescriptor;
import org.apache.druid.msq.input.table.SegmentsInputSlice;
import org.apache.druid.msq.input.table.TableInputSpec;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.CloneQueryMode;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.filter.DimFilterUtils;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineLookup;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

/**
 * Slices {@link TableInputSpec} into {@link SegmentsInputSlice} for persistent servers using
 * {@link TimelineServerView}.
 */
public class DartTableInputSpecSlicer implements InputSpecSlicer
{
  private static final int UNKNOWN = -1;

  /**
   * Worker host:port -> worker number. This is the reverse of the mapping from {@link WorkerManager#getWorkerIds()}.
   */
  private final Object2IntMap<String> workerIdToNumber;

  /**
   * Server view for identifying which segments exist and which servers (workers) have which segments.
   */
  private final TimelineServerView serverView;

  /**
   * Determines the kind of tasks that should be queried.
   */
  private final SegmentSource segmentSource;

  /**
   * Determines if cloning historicals should be queried.
   */
  private final CloneQueryMode cloneQueryMode;

  DartTableInputSpecSlicer(
      final Object2IntMap<String> workerIdToNumber,
      final TimelineServerView serverView,
      final QueryContext queryContext
  )
  {
    this.workerIdToNumber = workerIdToNumber;
    this.serverView = serverView;
    this.segmentSource = MultiStageQueryContext.getSegmentSources(queryContext, DartControllerContext.DEFAULT_SEGMENT_SOURCE);
    this.cloneQueryMode = queryContext.getCloneQueryMode();
  }

  public static DartTableInputSpecSlicer createFromWorkerIds(
      final List<String> workerIds,
      final TimelineServerView serverView,
      final QueryContext queryContext
  )
  {
    final Object2IntMap<String> reverseWorkers = new Object2IntOpenHashMap<>();
    reverseWorkers.defaultReturnValue(UNKNOWN);

    for (int i = 0; i < workerIds.size(); i++) {
      reverseWorkers.put(WorkerId.fromString(workerIds.get(i)).getHostAndPort(), i);
    }

    return new DartTableInputSpecSlicer(reverseWorkers, serverView, queryContext);
  }

  @Override
  public boolean canSliceDynamic(final InputSpec inputSpec)
  {
    return false;
  }

  @Override
  public List<InputSlice> sliceStatic(final InputSpec inputSpec, final int maxNumSlices)
  {
    final TableInputSpec tableInputSpec = (TableInputSpec) inputSpec;
    final TimelineLookup<String, ServerSelector> timeline =
        serverView.getTimeline(new TableDataSource(tableInputSpec.getDataSource())).orElse(null);

    if (timeline == null) {
      return Collections.emptyList();
    }

    final Set<DartQueryableSegment> prunedSegments =
        findQueryableDataSegments(
            tableInputSpec,
            timeline,
            serverSelector -> findWorkerForServerSelector(serverSelector, maxNumSlices)
        );

    final List<DartSegmentAssignment> assignments = new ArrayList<>(maxNumSlices);
    while (assignments.size() < maxNumSlices) {
      assignments.add(DartSegmentAssignment.empty());
    }

    int nextRoundRobinWorker = 0;
    final Map<DruidServerMetadata, List<DartQueryableSegment>> serverRequestMap = new HashMap<>();
    for (final DartQueryableSegment segment : prunedSegments) {
      final int worker;
      DruidServerMetadata realtimeServer = segment.getRealtimeServer();
      if (realtimeServer != null) {
        serverRequestMap.computeIfAbsent(realtimeServer, s -> new ArrayList<>()).add(segment);
        continue;
      }
      if (segment.getWorkerNumber() == UNKNOWN) {
        // Segment is not available on any worker. Assign to some worker, round-robin. Today, that server will throw
        // an error about the segment not being findable, but perhaps one day, it will be able to load the segment
        // on demand.
        worker = nextRoundRobinWorker;
        nextRoundRobinWorker = (nextRoundRobinWorker + 1) % maxNumSlices;
      } else {
        worker = segment.getWorkerNumber();
      }

      assignments.get(worker).addSegments(segment);
    }

    for (Map.Entry<DruidServerMetadata, List<DartQueryableSegment>> entry : serverRequestMap.entrySet()) {
      final int worker;
      DruidServerMetadata server = entry.getKey();
      worker = nextRoundRobinWorker;
      nextRoundRobinWorker = (nextRoundRobinWorker + 1) % maxNumSlices;
      List<RichSegmentDescriptor> descriptors = serverRequestMap.get(server).stream()
                                                                .map(DartTableInputSpecSlicer::toRichSegmentDescriptor)
                                                                .collect(Collectors.toList());
      assignments.get(worker).addRequest(new DataServerRequestDescriptor(server, descriptors));
    }

    return makeSegmentSlices(tableInputSpec.getDataSource(), assignments);
  }

  @Override
  public List<InputSlice> sliceDynamic(
      final InputSpec inputSpec,
      final int maxNumSlices,
      final int maxFilesPerSlice,
      final long maxBytesPerSlice
  )
  {
    throw new UnsupportedOperationException();
  }

  /**
   * Return the worker ID that corresponds to a particular {@link ServerSelector}, or {@link #UNKNOWN} if none does.
   *
   * @param serverSelector the server selector
   * @param maxNumSlices   maximum number of worker IDs to use
   */
  int findWorkerForServerSelector(final ServerSelector serverSelector, final int maxNumSlices)
  {
    // Currently, Dart does not support clone query modes, all servers can be queried.
    final QueryableDruidServer server = serverSelector.pick(null, cloneQueryMode);

    if (server == null) {
      return UNKNOWN;
    }

    final String serverHostAndPort = server.getServer().getHost();
    final int workerNumber = workerIdToNumber.getInt(serverHostAndPort);

    // The worker number may be UNKNOWN in a race condition, such as the set of Historicals changing while
    // the query is being planned. I don't think it can be >= maxNumSlices, but if it is, treat it like UNKNOWN.
    if (workerNumber != UNKNOWN && workerNumber < maxNumSlices) {
      return workerNumber;
    } else {
      return UNKNOWN;
    }
  }

  /**
   * Pull the list of {@link DataSegment} that we should query, along with a clipping interval for each one, and
   * a worker to get it from.
   */
  private Set<DartQueryableSegment> findQueryableDataSegments(
      final TableInputSpec tableInputSpec,
      final TimelineLookup<?, ServerSelector> timeline,
      final ToIntFunction<ServerSelector> toWorkersFunction
  )
  {
    final FluentIterable<DartQueryableSegment> allSegments =
        FluentIterable.from(JodaUtils.condenseIntervals(tableInputSpec.getIntervals()))
                      .transformAndConcat(timeline::lookup)
                      .transformAndConcat(
                          holder ->
                              FluentIterable
                                  .from(holder.getObject())
                                  .filter(chunk -> shouldIncludeSegment(chunk.getObject()))
                                  .transform(chunk -> {
                                    final ServerSelector serverSelector = chunk.getObject();
                                    return toDartQueryableSegment(serverSelector, holder.getInterval(), toWorkersFunction);
                                  })
                                  .filter(segment -> !segment.getSegment().isTombstone())
                      );

    return DimFilterUtils.filterShards(
        tableInputSpec.getFilter(),
        tableInputSpec.getFilterFields(),
        allSegments,
        segment -> segment.getSegment().getShardSpec(),
        new HashMap<>()
    );
  }

  private DartQueryableSegment toDartQueryableSegment(
      ServerSelector serverSelector,
      Interval interval,
      ToIntFunction<ServerSelector> toWorkersFunction
  )
  {
    final DataSegment dataSegment = serverSelector.getSegment();
    if (serverSelector.isRealtimeSegment()) {
      final Set<DruidServerMetadata> servers =
          serverSelector.getAllServers(cloneQueryMode)
                        .stream()
                        .filter(druidServerMetadata -> segmentSource.getUsedServerTypes()
                                                                    .contains(druidServerMetadata.getType()))
                        .collect(Collectors.toSet());
      return new DartQueryableSegment(dataSegment, interval, -1, DataServerSelector.RANDOM.getSelectServerFunction().apply(servers));
    } else {
      final int worker = toWorkersFunction.applyAsInt(serverSelector);
      return new DartQueryableSegment(dataSegment, interval, worker, null);
    }
  }

  /**
   * Create a list of {@link SegmentsInputSlice} and {@link NilInputSlice} assignments.
   *
   * @param dataSource  datasource to read
   * @param assignments list of assignment lists, one per slice
   *
   * @return a list of the same length as "assignments"
   *
   * @throws IllegalStateException if any provided segments do not match the provided datasource
   */
  private List<InputSlice> makeSegmentSlices(
      final String dataSource,
      final List<DartSegmentAssignment> assignments
  )
  {
    final List<InputSlice> retVal = new ArrayList<>(assignments.size());

    for (final DartSegmentAssignment assignment : assignments) {
      if (assignment == null || assignment.isEmpty()) {
        retVal.add(NilInputSlice.INSTANCE);
      } else {
        final List<RichSegmentDescriptor> descriptors = new ArrayList<>();
        for (DartQueryableSegment segment : assignment.getDartQueryableSegments()) {
          if (!dataSource.equals(segment.getSegment().getDataSource())) {
            throw new ISE("Expected dataSource[%s] but got[%s]", dataSource, segment.getSegment().getDataSource());
          }
          descriptors.add(toRichSegmentDescriptor(segment));
        }
        final List<DataServerRequestDescriptor> queryableDruidServers = assignment.getDataServerRequestDescriptor();
        retVal.add(new SegmentsInputSlice(dataSource, descriptors, queryableDruidServers));
      }
    }

    return retVal;
  }

  /**
   * Returns a {@link RichSegmentDescriptor}, which is used by {@link SegmentsInputSlice}.
   */
  static RichSegmentDescriptor toRichSegmentDescriptor(final DartQueryableSegment segment)
  {
    return new RichSegmentDescriptor(
        segment.getSegment().getInterval(),
        segment.getInterval(),
        segment.getSegment().getVersion(),
        segment.getSegment().getShardSpec().getPartitionNum()
    );
  }

  /**
   * Whether to include a segment from the timeline. Segments are included if they are not tombstones, and for realtime
   * segments, are only included based on the segmentSource.
   */
  private boolean shouldIncludeSegment(final ServerSelector serverSelector)
  {
    if (serverSelector.getSegment().isTombstone()) {
      return false;
    }
    if (serverSelector.isRealtimeSegment()) {
      return SegmentSource.shouldQueryRealtimeServers(segmentSource);
    }
    return true;
  }
}
