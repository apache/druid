package org.apache.druid.msq.exec;

import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.query.SegmentDescriptor;

import java.util.List;

/**
 * Contains the results for a query to a dataserver. {@link #resultsYielder} contains the results.
 * {@link #handedOffSegments} contains the list of segments which were not found on the dataserver.
 */
public class DataServerQueryResult<RowType>
{

  private final Yielder<RowType> resultsYielder;

  private final List<SegmentDescriptor> handedOffSegments;

  public DataServerQueryResult(
      Yielder<RowType> resultsYielder,
      List<SegmentDescriptor> handedOffSegments
  )
  {
    this.resultsYielder = resultsYielder;
    this.handedOffSegments = handedOffSegments;
  }

  public Yielder<RowType> getResultsYielder()
  {
    return resultsYielder;
  }

  public List<SegmentDescriptor> getHandedOffSegments()
  {
    return handedOffSegments;
  }
}
