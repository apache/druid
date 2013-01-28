/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.merger.common.task;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultiset;
import com.google.common.primitives.Ints;
import com.metamx.common.logger.Logger;
import com.metamx.druid.input.InputRow;
import com.metamx.druid.merger.common.TaskCallback;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.TaskToolbox;
import com.metamx.druid.merger.coordinator.TaskContext;
import com.metamx.druid.realtime.Firehose;
import com.metamx.druid.realtime.FirehoseFactory;
import com.metamx.druid.realtime.Schema;
import com.metamx.druid.shard.NoneShardSpec;
import com.metamx.druid.shard.ShardSpec;
import com.metamx.druid.shard.SingleDimensionShardSpec;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class IndexDeterminePartitionsTask extends AbstractTask
{
  @JsonProperty
  private final FirehoseFactory firehoseFactory;
  @JsonProperty
  private final Schema schema;
  @JsonProperty
  private final long targetPartitionSize;

  private static final Logger log = new Logger(IndexTask.class);

  @JsonCreator
  public IndexDeterminePartitionsTask(
      @JsonProperty("groupId") String groupId,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("firehose") FirehoseFactory firehoseFactory,
      @JsonProperty("schema") Schema schema,
      @JsonProperty("targetPartitionSize") long targetPartitionSize
  )
  {
    super(
        String.format(
            "%s_partitions_%s_%s",
            groupId,
            interval.getStart(),
            interval.getEnd()
        ),
        groupId,
        schema.getDataSource(),
        interval
    );

    this.firehoseFactory = firehoseFactory;
    this.schema = schema;
    this.targetPartitionSize = targetPartitionSize;
  }

  @Override
  public Type getType()
  {
    return Type.INDEX;
  }

  @Override
  public TaskStatus run(TaskContext context, TaskToolbox toolbox, TaskCallback callback) throws Exception
  {
    log.info("Running with targetPartitionSize[%d]", targetPartitionSize);

    // This is similar to what DeterminePartitionsJob does in the hadoop indexer, but we don't require
    // a preconfigured partition dimension (we'll just pick the one with highest cardinality).

    // XXX - Space-efficiency (stores all unique dimension values, although at least not all combinations)
    // XXX - Time-efficiency (runs all this on one single node instead of through map/reduce)

    // Blacklist dimensions that have multiple values per row
    final Set<String> unusableDimensions = Sets.newHashSet();

    // Track values of all non-blacklisted dimensions
    final Map<String, TreeMultiset<String>> dimensionValueMultisets = Maps.newHashMap();

    // Load data
    final Firehose firehose = firehoseFactory.connect();

    try {
      while (firehose.hasMore()) {

        final InputRow inputRow = firehose.nextRow();

        if (getInterval().contains(inputRow.getTimestampFromEpoch())) {

          // Extract dimensions from event
          for (final String dim : inputRow.getDimensions()) {
            final List<String> dimValues = inputRow.getDimension(dim);

            if (!unusableDimensions.contains(dim)) {

              if (dimValues.size() == 1) {

                // Track this value
                TreeMultiset<String> dimensionValueMultiset = dimensionValueMultisets.get(dim);

                if (dimensionValueMultiset == null) {
                  dimensionValueMultiset = TreeMultiset.create();
                  dimensionValueMultisets.put(dim, dimensionValueMultiset);
                }

                dimensionValueMultiset.add(dimValues.get(0));

              } else {

                // Only single-valued dimensions can be used for partitions
                unusableDimensions.add(dim);
                dimensionValueMultisets.remove(dim);

              }

            }
          }

        }

      }
    }
    finally {
      firehose.close();
    }

    // ShardSpecs for index generator tasks
    final List<ShardSpec> shardSpecs = Lists.newArrayList();

    // Select highest-cardinality dimension
    Ordering<Map.Entry<String, TreeMultiset<String>>> byCardinalityOrdering = new Ordering<Map.Entry<String, TreeMultiset<String>>>()
    {
      @Override
      public int compare(
          Map.Entry<String, TreeMultiset<String>> left,
          Map.Entry<String, TreeMultiset<String>> right
      )
      {
        return Ints.compare(left.getValue().elementSet().size(), right.getValue().elementSet().size());
      }
    };

    if (dimensionValueMultisets.isEmpty()) {
      // No suitable partition dimension. We'll make one big segment and hope for the best.
      log.info("No suitable partition dimension found");
      shardSpecs.add(new NoneShardSpec());
    } else {
      // Find best partition dimension (heuristic: highest cardinality).
      final Map.Entry<String, TreeMultiset<String>> partitionEntry =
          byCardinalityOrdering.max(dimensionValueMultisets.entrySet());

      final String partitionDim = partitionEntry.getKey();
      final TreeMultiset<String> partitionDimValues = partitionEntry.getValue();

      log.info(
          "Partitioning on dimension[%s] with cardinality[%d] over rows[%d]",
          partitionDim,
          partitionDimValues.elementSet().size(),
          partitionDimValues.size()
      );

      // Iterate over unique partition dimension values in sorted order
      String currentPartitionStart = null;
      int currentPartitionSize = 0;
      for (final String partitionDimValue : partitionDimValues.elementSet()) {
        currentPartitionSize += partitionDimValues.count(partitionDimValue);
        if (currentPartitionSize >= targetPartitionSize) {
          final ShardSpec shardSpec = new SingleDimensionShardSpec(
              partitionDim,
              currentPartitionStart,
              partitionDimValue,
              shardSpecs.size()
          );

          log.info("Adding shard: %s", shardSpec);
          shardSpecs.add(shardSpec);

          currentPartitionSize = partitionDimValues.count(partitionDimValue);
          currentPartitionStart = partitionDimValue;
        }
      }

      if (currentPartitionSize > 0) {
        // One last shard to go
        final ShardSpec shardSpec;

        if (shardSpecs.isEmpty()) {
          shardSpec = new NoneShardSpec();
        } else {
          shardSpec = new SingleDimensionShardSpec(
              partitionDim,
              currentPartitionStart,
              null,
              shardSpecs.size()
          );
        }

        log.info("Adding shard: %s", shardSpec);
        shardSpecs.add(shardSpec);
      }
    }

    return TaskStatus.continued(
        getId(),
        Lists.transform(
            shardSpecs,
            new Function<ShardSpec, Task>()
            {
              @Override
              public Task apply(ShardSpec shardSpec)
              {
                return new IndexGeneratorTask(
                    getGroupId(),
                    getInterval(),
                    firehoseFactory,
                    new Schema(
                        schema.getDataSource(),
                        schema.getAggregators(),
                        schema.getIndexGranularity(),
                        shardSpec
                    )
                );
              }
            }
        )
    ).withAction(TaskStatus.Action.ANNOUNCE_SEGMENTS);
  }
}
