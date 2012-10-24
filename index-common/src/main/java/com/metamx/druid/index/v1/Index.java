package com.metamx.druid.index.v1;

import com.metamx.common.logger.Logger;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;
import org.joda.time.Interval;

import java.util.HashMap;
import java.util.Map;

/**
 * In-memory representation of a segment
 */
public class Index
{
  private static final Logger log = new Logger(Index.class);
  private static final ImmutableConciseSet emptySet = new ImmutableConciseSet();

  final Map<String, Integer> dimToPositionMap = new HashMap<String, Integer>();
  final Map<String, Integer> metricToPositionMap = new HashMap<String, Integer>();

  final String[] dimensions;
  final String[] metrics;
  final Interval dataInterval;
  final long[] timeOffsets;
  final Map<String, MetricHolder> metricVals;
  final Map<String, Map<String, Integer>> dimIdLookup;
  final Map<String, String[]> reverseDimLookup;
  final Map<String, ImmutableConciseSet[]> indexes;
  final Map<String, DimensionColumn> dimensionValues;

  /*
   * If we name the various occurrences of String and int then the types are more informative:
   *
   *   dimToPositionMap         : Dim       -> DimColumn
   *   dimensions               : DimColumn -> Dim
   *   metricToPositionMap      : Met       -> MetColumn
   *   metrics                  : MetColumn -> Met
   *
   *   dataInterval             : Interval
   *
   *   timeOffsets              : Milli[]
   *   metricVals               : float[] (size() == timeOffsets.size() * metrics.size())
   *
   *   dimIdLookup              : Dim -> Value   -> ValueID
   *   reverseDimLookup         : Dim -> ValueID -> Value
   *   indexes                  : Dim -> ValueID -> Row[]
   *
   *   dimensionValues          : Dim ->
   *     getRowValues           :        Row         -> ExpansionID
   *     getDimensionExpansions :        ExpansionID -> ValueID[]
   *     getDimValues           :        Row         -> ValueID[]
   */

  public Index(
      String[] dimensions,
      String[] metrics,
      Interval dataInterval,
      long[] timeOffsets,
      Map<String, MetricHolder> metricVals,
      Map<String, Map<String, Integer>> dimIdLookup,
      Map<String, String[]> reverseDimLookup,
      Map<String, ImmutableConciseSet[]> indexes,
      Map<String, DimensionColumn> dimensionValues
  )
  {
    this.dimensions = dimensions;
    this.metrics = metrics;
    this.dataInterval = dataInterval;
    this.timeOffsets = timeOffsets;
    this.metricVals = metricVals;
    this.dimIdLookup = dimIdLookup;
    this.reverseDimLookup = reverseDimLookup;
    this.indexes = indexes;
    this.dimensionValues = dimensionValues;

    for (int i = 0; i < dimensions.length; i++) {
      dimToPositionMap.put(dimensions[i].toLowerCase(), i);
    }
    for (int i = 0; i < metrics.length; i++) {
      metricToPositionMap.put(metrics[i].toLowerCase(), i);
    }
  }

  public ImmutableConciseSet getInvertedIndex(String dimension, String value)
  {
    final Map<String, Integer> lookup = dimIdLookup.get(dimension);
    if (lookup == null) {
      return emptySet;
    }

    final Integer integer = lookup.get(value);
    if (integer == null) {
      return emptySet;
    }

    try {
      return indexes.get(dimension)[integer];
    }
    catch (NullPointerException e) {
      log.warn(
          e,
          "NPE on dimension[%s], value[%s], with index over interval[%s]",
          dimension,
          value,
          dataInterval
      );
      return emptySet;
    }
  }
}
