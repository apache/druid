package org.apache.druid.msq.statistics;

import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;

/**
 * Class maintained by the controller to merge {@link PartialKeyStatisticsInformation} sent by the worker.
 */
public class CompleteKeyStatisticsInformation
{
  private final SortedMap<Long, Set<Integer>> timeSegmentVsWorkerMap;

  private boolean hasMultipleValues;

  private double bytesRetained;

  public CompleteKeyStatisticsInformation(
      final SortedMap<Long, Set<Integer>> timeChunks,
      boolean hasMultipleValues,
      double bytesRetained
  )
  {
    this.timeSegmentVsWorkerMap = timeChunks;
    this.hasMultipleValues = hasMultipleValues;
    this.bytesRetained = bytesRetained;
  }

  public void mergePartialInformation(int workerNumber, PartialKeyStatisticsInformation partialKeyStatisticsInformation)
  {
    for (Long timeSegment : partialKeyStatisticsInformation.getTimeSegments()) {
      this.timeSegmentVsWorkerMap
          .computeIfAbsent(timeSegment, key -> new HashSet<>())
          .add(workerNumber);
    }
    this.hasMultipleValues = this.hasMultipleValues || partialKeyStatisticsInformation.isHasMultipleValues();
    this.bytesRetained += bytesRetained;
  }

  public SortedMap<Long, Set<Integer>> getTimeSegmentVsWorkerMap()
  {
    return timeSegmentVsWorkerMap;
  }

  public boolean isHasMultipleValues()
  {
    return hasMultipleValues;
  }

  public double getBytesRetained()
  {
    return bytesRetained;
  }
}
