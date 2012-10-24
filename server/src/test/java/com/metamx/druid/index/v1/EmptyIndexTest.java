package com.metamx.druid.index.v1;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.aggregation.AggregatorFactory;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;

public class EmptyIndexTest
{
  @Test
  public void testEmptyIndex() throws Exception
  {
    File tmpDir = File.createTempFile("emptyIndex", "");
    if (!tmpDir.delete()) {
      throw new IllegalStateException("tmp delete failed");
    }
    if (!tmpDir.mkdir()) {
      throw new IllegalStateException("tmp mkdir failed");
    }
    tmpDir.deleteOnExit();

    IncrementalIndex emptyIndex = new IncrementalIndex(0, QueryGranularity.NONE, new AggregatorFactory[0]);
    IncrementalIndexAdapter emptyIndexAdapter = new IncrementalIndexAdapter(
        new Interval("2012-08-01/P3D"),
        emptyIndex,
        new ArrayList<String>(),
        new ArrayList<String>()
    );
    IndexMerger.merge(Lists.<IndexableAdapter>newArrayList(emptyIndexAdapter), new AggregatorFactory[0], tmpDir);

    MMappedIndex emptyIndexMMapped = IndexIO.mapDir(tmpDir);

    Assert.assertEquals("getAvailableDimensions", 0, Iterables.size(emptyIndexMMapped.getAvailableDimensions()));
    Assert.assertEquals("getAvailableMetrics", 0, Iterables.size(emptyIndexMMapped.getAvailableMetrics()));
    Assert.assertEquals("getDataInterval", new Interval("2012-08-01/P3D"), emptyIndexMMapped.getDataInterval());
    Assert.assertEquals("getReadOnlyTimestamps", 0, emptyIndexMMapped.getReadOnlyTimestamps().size());
  }
}
