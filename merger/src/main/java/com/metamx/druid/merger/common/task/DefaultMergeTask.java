package com.metamx.druid.merger.common.task;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.index.v1.IndexIO;
import com.metamx.druid.index.v1.IndexMerger;
import com.metamx.druid.index.v1.MMappedIndex;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.annotation.Nullable;
import java.io.File;
import java.util.List;
import java.util.Map;

/**
 */
public class DefaultMergeTask extends MergeTask
{
  private final List<AggregatorFactory> aggregators;

  @JsonCreator
  public DefaultMergeTask(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("segments") List<DataSegment> segments,
      @JsonProperty("aggregations") List<AggregatorFactory> aggregators
  )
  {
    super(dataSource, segments);
    this.aggregators = aggregators;
  }

  @Override
  public File merge(final Map<DataSegment, File> segments, final File outDir)
      throws Exception
  {
    return IndexMerger.mergeMMapped(
        Lists.transform(
            ImmutableList.copyOf(segments.values()),
            new Function<File, MMappedIndex>()
            {
              @Override
              public MMappedIndex apply(@Nullable File input)
              {
                try {
                  return IndexIO.mapDir(input);
                }
                catch (Exception e) {
                  throw Throwables.propagate(e);
                }
              }
            }
        ),
        aggregators.toArray(new AggregatorFactory[aggregators.size()]),
        outDir
    );
  }

  @Override
  public Type getType()
  {
    return Task.Type.MERGE;
  }
}
