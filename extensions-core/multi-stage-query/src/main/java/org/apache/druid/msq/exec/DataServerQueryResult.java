package org.apache.druid.msq.exec;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.msq.input.table.RichSegmentDescriptor;
import org.apache.druid.msq.input.table.SegmentsInputSlice;

import java.util.List;

/**
 * Contains the results for a query to a dataserver. {@link #resultsYielder} contains the results.
 * {@link #segmentsInputSlice} contains the list of segments which were not found on the dataserver.
 */
public class DataServerQueryResult<RowType>
{

  private final Yielder<RowType> resultsYielder;

  private final SegmentsInputSlice segmentsInputSlice;

  public DataServerQueryResult(
      Yielder<RowType> resultsYielder,
      List<RichSegmentDescriptor> handedOffSegments,
      String dataSource
  )
  {
    this.resultsYielder = resultsYielder;
    this.segmentsInputSlice = new SegmentsInputSlice(dataSource, handedOffSegments, ImmutableList.of());
  }

  public Yielder<RowType> getResultsYielder()
  {
    return resultsYielder;
  }

  public SegmentsInputSlice getHandedOffSegments()
  {
    return segmentsInputSlice;
  }
}
