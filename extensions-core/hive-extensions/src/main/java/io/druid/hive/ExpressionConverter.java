/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.hive;

import com.google.common.base.Function;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.metamx.common.logger.Logger;
import io.druid.common.utils.JodaUtils;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.InDimFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.ExpressionTree;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.joda.time.Interval;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
public class ExpressionConverter
{
  private static final Logger logger = new Logger(ExpressionConverter.class);

  public static final String TIME_COLUMN_NAME = "__time";

  static Map<String, List<Range>> convert(Configuration configuration)
  {
    String filterExprSerialized = configuration.get(TableScanDesc.FILTER_EXPR_CONF_STR);
    if (filterExprSerialized == null) {
      logger.info("No predicate is pushed down");
      return Collections.emptyMap();
    }
    ExprNodeGenericFuncDesc expr = SerializationUtilities.deserializeExpression(filterExprSerialized);
    return getRanges(expr, getColumnTypes(configuration));
  }

  public static List<Interval> toInterval(List<Range> ranges)
  {
    List<Interval> intervals = Lists.transform(
        ranges, new Function<Range, Interval>()
        {
          @Override
          public Interval apply(Range range)
          {
            long start = range.hasLowerBound() ? toLong(range.lowerEndpoint()) : JodaUtils.MIN_INSTANT;
            long end = range.hasUpperBound() ? toLong(range.upperEndpoint()) : JodaUtils.MAX_INSTANT;
            if (range.hasLowerBound() && range.lowerBoundType() == BoundType.OPEN) {
              start++;
            }
            if (range.hasUpperBound() && range.upperBoundType() == BoundType.CLOSED) {
              end++;
            }
            return new Interval(start, end);
          }
        }
    );
    logger.info("Converted time ranges %s to interval %s", ranges, intervals);
    return intervals;
  }

  // should be string type
  public static DimFilter toFilter(String dimension, List<Range> ranges)
  {
    Iterable<Range> filtered = Iterables.filter(ranges, Ranges.VALID);
    List<String> equalValues = Lists.newArrayList();
    List<DimFilter> dimFilters = Lists.newArrayList();
    for (Range range : filtered) {
      String lower = range.hasLowerBound() ? (String) range.lowerEndpoint() : null;
      String upper = range.hasUpperBound() ? (String) range.upperEndpoint() : null;
      if (lower == null && upper == null) {
        return null;
      }
      if (Objects.equals(lower, upper)) {
        equalValues.add(lower);
        continue;
      }
      boolean lowerStrict = range.hasLowerBound() && range.lowerBoundType() == BoundType.OPEN;
      boolean upperStrict = range.hasUpperBound() && range.upperBoundType() == BoundType.OPEN;
      dimFilters.add(new BoundDimFilter(dimension, lower, upper, lowerStrict, upperStrict, false, null));
    }
    if (equalValues.size() > 1) {
      dimFilters.add(new InDimFilter(dimension, equalValues, null));
    } else if (equalValues.size() == 1) {
      dimFilters.add(new SelectorDimFilter(dimension, equalValues.get(0), null));
    }
    DimFilter dimFilter = new OrDimFilter(dimFilters).optimize();
    logger.info("Converted dimension '%s' ranges %s to filter %s", dimension, ranges, dimFilter);
    return dimFilter;
  }

  private static Map<String, PrimitiveTypeInfo> getColumnTypes(Configuration configuration)
  {
    String[] colNames = configuration.getStrings(serdeConstants.LIST_COLUMNS);
    String[] colTypes = configuration.getStrings(serdeConstants.LIST_COLUMN_TYPES);
    Set<Integer> projections = Sets.newHashSet(ColumnProjectionUtils.getReadColumnIDs(configuration));
    if (colNames == null || colTypes == null) {
      return ImmutableMap.of();
    }
    Map<String, PrimitiveTypeInfo> typeMap = Maps.newHashMap();
    for (int i = 0; i < colTypes.length; i++) {
      if (!projections.isEmpty() && !projections.contains(i)) {
        continue;
      }
      String colName = colNames[i].trim();
      PrimitiveTypeInfo typeInfo = TypeInfoFactory.getPrimitiveTypeInfo(colTypes[i]);
      if (colName.equals(ExpressionConverter.TIME_COLUMN_NAME) &&
          typeInfo.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.LONG) {
        logger.warn("time column should be defined as bigint type, yet");
      }
      typeMap.put(colName, typeInfo);
    }
    return typeMap;
  }

  static Map<String, List<Range>> getRanges(ExprNodeGenericFuncDesc filterExpr, Map<String, PrimitiveTypeInfo> types)
  {
    logger.info("Start analyzing predicate " + filterExpr.getExprString());
    SearchArgument searchArgument = ConvertAstToSearchArg.create(filterExpr);
    ExpressionTree root = searchArgument.getExpression();

    List<PredicateLeaf> leaves = Lists.newArrayList(searchArgument.getLeaves());

    Map<String, List<Range>> rangeMap = Maps.newHashMap();
    if (root.getOperator() == ExpressionTree.Operator.AND) {
      for (ExpressionTree child : root.getChildren()) {
        String extracted = extractSoleColumn(child, leaves);
        if (extracted == null) {
          continue;
        }
        PrimitiveTypeInfo type = types.get(extracted);
        List<Range> ranges = extractRanges(type, child, leaves, false);
        if (ranges == null) {
          continue;
        }
        if (rangeMap.get(extracted) == null) {
          rangeMap.put(extracted, ranges);
          continue;
        }
        // (a or b) and (c or d) -> (a and c) or (b and c) or (a and d) or (b and d)
        List<Range> overlapped = Lists.newArrayList();
        for (Range current : rangeMap.get(extracted)) {
          for (Range interval : ranges) {
            if (current.isConnected(interval)) {
              overlapped.add(current.intersection(interval));
            }
          }
          rangeMap.put(extracted, overlapped);
        }
      }
    } else {
      String extracted = extractSoleColumn(root, leaves);
      if (extracted != null) {
        PrimitiveTypeInfo type = types.get(extracted);
        List<Range> ranges = extractRanges(type, root, leaves, false);
        if (ranges != null) {
          rangeMap.put(extracted, ranges);
        }
      }
    }

    Map<String, List<Range>> rangesMap = Maps.transformValues(rangeMap, Ranges.COMPACT);
    for (Map.Entry<String, List<Range>> entry : rangesMap.entrySet()) {
      logger.info(">> " + entry);
    }
    return rangesMap;
  }

  private static String extractSoleColumn(ExpressionTree tree, List<PredicateLeaf> leaves)
  {
    if (tree.getOperator() == ExpressionTree.Operator.LEAF) {
      return leaves.get(tree.getLeaf()).getColumnName();
    }
    String current = null;
    List<ExpressionTree> children = tree.getChildren();
    if (children != null && !children.isEmpty()) {
      for (ExpressionTree child : children) {
        String resolved = extractSoleColumn(child, leaves);
        if (current != null && !current.equals(resolved)) {
          return null;
        }
        current = resolved;
      }
    }
    return current;
  }

  private static List<Range> extractRanges(
      PrimitiveTypeInfo type,
      ExpressionTree tree,
      List<PredicateLeaf> leaves,
      boolean withNot
  )
  {
    if (tree.getOperator() == ExpressionTree.Operator.NOT) {
      return extractRanges(type, tree.getChildren().get(0), leaves, !withNot);
    }
    if (tree.getOperator() == ExpressionTree.Operator.LEAF) {
      return leafToRanges(type, leaves.get(tree.getLeaf()), withNot);
    }
    if (tree.getOperator() == ExpressionTree.Operator.OR) {
      List<Range> intervals = Lists.newArrayList();
      for (ExpressionTree child : tree.getChildren()) {
        List<Range> extracted = extractRanges(type, child, leaves, withNot);
        if (extracted != null) {
          intervals.addAll(extracted);
        }
      }
      return intervals;
    }
    return null;
  }

  private static List<Range> leafToRanges(PrimitiveTypeInfo type, PredicateLeaf hiveLeaf, boolean withNot)
  {
    PredicateLeaf.Operator operator = hiveLeaf.getOperator();
    switch (operator) {
      case LESS_THAN:
      case LESS_THAN_EQUALS:
      case EQUALS:  // in druid, all equals are null-safe equals
      case NULL_SAFE_EQUALS:
        Comparable value = literalToType(hiveLeaf.getLiteral(), type);
        if (value == null) {
          return null;
        }
        if (operator == PredicateLeaf.Operator.LESS_THAN) {
          return Arrays.<Range>asList(withNot ? Range.atLeast(value) : Range.lessThan(value));
        } else if (operator == PredicateLeaf.Operator.LESS_THAN_EQUALS) {
          return Arrays.<Range>asList(withNot ? Range.greaterThan(value) : Range.atMost(value));
        } else {
          if (!withNot) {
            return Arrays.<Range>asList(Range.closed(value, value));
          }
          return Arrays.<Range>asList(Range.lessThan(value), Range.greaterThan(value));
        }
      case BETWEEN:
        Comparable value1 = literalToType(hiveLeaf.getLiteralList().get(0), type);
        Comparable value2 = literalToType(hiveLeaf.getLiteralList().get(1), type);
        if (value1 == null || value2 == null) {
          return null;
        }
        boolean inverted = value1.compareTo(value2) > 0;
        if (!withNot) {
          return Arrays.<Range>asList(inverted ? Range.closed(value2, value1) : Range.closed(value1, value2));
        }
        return Arrays.<Range>asList(
            Range.lessThan(inverted ? value2 : value1),
            Range.greaterThan(inverted ? value1 : value2)
        );
      case IN:
        List<Range> ranges = Lists.newArrayList();
        for (Object literal : hiveLeaf.getLiteralList()) {
          Comparable element = literalToType(literal, type);
          if (element == null) {
            return null;
          }
          if (withNot) {
            ranges.addAll(Arrays.<Range>asList(Range.lessThan(element), Range.greaterThan(element)));
          } else {
            ranges.add(Range.closed(element, element));
          }
        }
        return ranges;
    }
    return null;
  }

  private static Comparable literalToType(Object literal, PrimitiveTypeInfo type)
  {
    switch (type.getPrimitiveCategory()) {
      case LONG:
        return toLong(literal);
      case INT:
        return toInt(literal);
      case FLOAT:
        return toFloat(literal);
      case DOUBLE:
        return toDouble(literal);
      case STRING:
        return String.valueOf(literal);
      case TIMESTAMP:
        return toTimestamp(literal);
    }
    return null;
  }

  private static Comparable toTimestamp(Object literal)
  {
    if (literal instanceof Timestamp) {
      return (Timestamp)literal;
    }
    if (literal instanceof Date) {
      return new Timestamp(((Date) literal).getTime());
    }
    if (literal instanceof Number) {
      return new Timestamp(((Number) literal).longValue());
    }
    if (literal instanceof String) {
        String string = (String) literal;
      if (StringUtils.isNumeric(string)) {
        return new Timestamp(Long.valueOf(string));
      }
      try {
        return Timestamp.valueOf(string);
      }
      catch (NumberFormatException e) {
        // ignore
      }
    }
    return null;
  }

  private static Long toLong(Object literal)
  {
    if (literal instanceof Number) {
      return ((Number) literal).longValue();
    }
    if (literal instanceof Date) {
      return ((Date) literal).getTime();
    }
    if (literal instanceof Timestamp) {
      return ((Timestamp) literal).getTime();
    }
    if (literal instanceof String) {
      try {
        return Long.valueOf((String) literal);
      }
      catch (NumberFormatException e) {
        // ignore
      }
      try {
        return DateFormat.getDateInstance().parse((String) literal).getTime();
      }
      catch (ParseException e) {
        // best effort. ignore
      }
    }
    return null;
  }

  private static Integer toInt(Object literal)
  {
    if (literal instanceof Number) {
      return ((Number) literal).intValue();
    }
    if (literal instanceof String) {
      try {
        return Integer.valueOf((String) literal);
      }
      catch (NumberFormatException e) {
        // ignore
      }
    }
    return null;
  }

  private static Float toFloat(Object literal)
  {
    if (literal instanceof Number) {
      return ((Number) literal).floatValue();
    }
    if (literal instanceof String) {
      try {
        return Float.valueOf((String) literal);
      }
      catch (NumberFormatException e) {
        // ignore
      }
    }
    return null;
  }

  private static Double toDouble(Object literal)
  {
    if (literal instanceof Number) {
      return ((Number) literal).doubleValue();
    }
    if (literal instanceof String) {
      try {
        return Double.valueOf((String) literal);
      }
      catch (NumberFormatException e) {
        // ignore
      }
    }
    return null;
  }
}
