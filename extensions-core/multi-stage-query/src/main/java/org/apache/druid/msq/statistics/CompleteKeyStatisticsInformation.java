package org.apache.druid.msq.statistics;

import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;

/**
 * Class maintained by the controller to merge {@link PartialKeyStatisticsInformation} sent by the worker.
 */
public class CompleteKeyStatisticsInformation
{
  private final SortedMap<Long, Set<Integer>> timeSegmentVsWorkerIdMap;

  private boolean hasMultipleValues;

  private double bytesRetained;

  public CompleteKeyStatisticsInformation(
      final SortedMap<Long, Set<Integer>> timeChunks,
      boolean hasMultipleValues,
      double bytesRetained
  )
  {
    this.timeSegmentVsWorkerIdMap = timeChunks;
    this.hasMultipleValues = hasMultipleValues;
    this.bytesRetained = bytesRetained;
  }

  public void mergePartialInformation(int workerId, PartialKeyStatisticsInformation partialKeyStatisticsInformation)
  {
    for (Long timeSegment : partialKeyStatisticsInformation.getTimeSegments()) {
      this.timeSegmentVsWorkerIdMap
          .computeIfAbsent(timeSegment, key -> new HashSet<>())
          .add(workerId);
    }
    this.hasMultipleValues = this.hasMultipleValues || partialKeyStatisticsInformation.isHasMultipleValues();
    this.bytesRetained += bytesRetained;
  }

  public SortedMap<Long, Set<Integer>> getTimeSegmentVsWorkerIdMap()
  {
    return timeSegmentVsWorkerIdMap;
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
