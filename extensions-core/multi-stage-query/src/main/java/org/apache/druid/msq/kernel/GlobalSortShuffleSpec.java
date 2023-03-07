package org.apache.druid.msq.kernel;

import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.msq.statistics.ClusterByStatisticsCollector;

import javax.annotation.Nullable;

/**
 * Additional methods for {@link ShuffleSpec} of kind {@link ShuffleKind#GLOBAL_SORT}.
 */
public interface GlobalSortShuffleSpec extends ShuffleSpec
{
  /**
   * Whether {@link #generatePartitionsForGlobalSort} needs a nonnull collector in order to do its work.
   */
  boolean mustGatherResultKeyStatistics();

  /**
   * Generates a set of partitions based on the provided statistics.
   *
   * Only valid if {@link #kind()} is {@link ShuffleKind#GLOBAL_SORT}. Otherwise, throws {@link IllegalStateException}.
   *
   * @param collector        must be nonnull if {@link #mustGatherResultKeyStatistics()} is true; ignored otherwise
   * @param maxNumPartitions maximum number of partitions to generate
   *
   * @return either the partition assignment, or (as an error) a number of partitions, greater than maxNumPartitions,
   * that would be expected to be created
   *
   * @throws IllegalStateException if {@link #kind()} is not {@link ShuffleKind#GLOBAL_SORT}.
   */
  Either<Long, ClusterByPartitions> generatePartitionsForGlobalSort(
      @Nullable ClusterByStatisticsCollector collector,
      int maxNumPartitions
  );
}
