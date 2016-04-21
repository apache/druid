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

import com.google.common.collect.Lists;
import com.metamx.common.logger.Logger;
import io.druid.common.utils.JodaUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.ExpressionTree;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.joda.time.Interval;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 */
public class ExpressionConverter
{
  private static final Logger logger = new Logger(ExpressionConverter.class);

  public static final String TIME_COLUMN_NAME = "__time";

  static List<Interval> convert(Configuration jobConf)
  {
    String filterExprSerialized = jobConf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
    if (filterExprSerialized == null) {
      logger.info("No predicate is pushed down");
      return Collections.<Interval>emptyList();
    }
    List<Interval> intervals = getIntervals(SerializationUtilities.deserializeExpression(filterExprSerialized));
    logger.info("Extracted intervals : " + intervals);
    return intervals;
  }

  static List<Interval> getIntervals(ExprNodeGenericFuncDesc filterExpr)
  {
    logger.info("Start analyzing predicate " + filterExpr.getExprString());
    SearchArgument searchArgument = ConvertAstToSearchArg.create(filterExpr);
    ExpressionTree root = searchArgument.getExpression();

    List<PredicateLeaf> leaves = Lists.newArrayList(searchArgument.getLeaves());

    List<Interval> currents = null;
    if (root.getOperator() == ExpressionTree.Operator.AND) {
      for (ExpressionTree child : root.getChildren()) {
        String extracted = extractSoleColumn(child, leaves, null);
        if (TIME_COLUMN_NAME.equals(extracted)) {
          List<Interval> intervals = extractIntervals(child, leaves, false);
          if (currents == null) {
            currents = intervals;
            continue;
          }
          // (a or b) and (c or d) -> (a and c) or (b and c) or (a and d) or (b and d)
          List<Interval> overlapped = Lists.newArrayList();
          for (Interval current : currents) {
            for (Interval interval : intervals) {
              Interval overlap = current.overlap(interval);
              if (overlap != null) {
                overlapped.add(overlap);
              }
            }
          }
          currents = overlapped;
        }
      }
    } else {
      String extracted = extractSoleColumn(root, leaves, null);
      if (TIME_COLUMN_NAME.equals(extracted)) {
        currents = extractIntervals(root, leaves, false);
      }
    }
    return currents != null ? JodaUtils.condenseIntervals(currents) : Collections.<Interval>emptyList();
  }

  private static String extractSoleColumn(ExpressionTree tree, List<PredicateLeaf> leaves, String current)
  {
    if (tree.getOperator() == ExpressionTree.Operator.LEAF) {
      String columnName = leaves.get(tree.getLeaf()).getColumnName();
      if (current == null || current.equals(columnName)) {
        return columnName;
      }
      return null;
    }
    List<ExpressionTree> children = tree.getChildren();
    if (children != null && !children.isEmpty()) {
      for (ExpressionTree child : children) {
        current = extractSoleColumn(child, leaves, current);
      }
    }
    return current;
  }

  private static List<Interval> extractIntervals(ExpressionTree tree, List<PredicateLeaf> leaves, boolean withNot)
  {
    if (tree.getOperator() == ExpressionTree.Operator.NOT) {
      return extractIntervals(tree.getChildren().get(0), leaves, !withNot);
    }
    if (tree.getOperator() == ExpressionTree.Operator.LEAF) {
      return leafToIntervals(leaves.get(tree.getLeaf()), withNot);
    }
    if (tree.getOperator() == ExpressionTree.Operator.OR) {
      List<Interval> intervals = Lists.newArrayList();
      for (ExpressionTree child : tree.getChildren()) {
        List<Interval> extracted = extractIntervals(child, leaves, withNot);
        if (extracted != null) {
          intervals.addAll(extracted);
        }
      }
    }
    return null;
  }

  private static List<Interval> leafToIntervals(PredicateLeaf hiveLeaf, boolean withNot)
  {
    switch (hiveLeaf.getOperator()) {
      case LESS_THAN:
      case LESS_THAN_EQUALS:
        Long time = literalToTime(hiveLeaf.getLiteral());
        if (time != null) {
          return Arrays.asList(
              withNot ? new Interval(time, JodaUtils.MAX_INSTANT) : new Interval(JodaUtils.MIN_INSTANT, time)
          );
        }
        return null;
      case BETWEEN:
        Long start = literalToTime(hiveLeaf.getLiteralList().get(0));
        Long end = literalToTime(hiveLeaf.getLiteralList().get(1));
        if (start != null && end != null) {
          return withNot ? Arrays.asList(
              new Interval(JodaUtils.MIN_INSTANT, start),
              new Interval(end, JodaUtils.MAX_INSTANT)
          ) : Arrays.asList(new Interval(start, end));
        }
        return null;
    }
    if (hiveLeaf.getOperator() == PredicateLeaf.Operator.EQUALS && hiveLeaf.getLiteral() instanceof String) {
      try {
        Interval interval = new Interval(hiveLeaf.getLiteral());
        return withNot ? Arrays.asList(
            new Interval(JodaUtils.MIN_INSTANT, interval.getStartMillis()),
            new Interval(interval.getEndMillis(), JodaUtils.MAX_INSTANT)
        ) : Arrays.asList(interval);
      }
      catch (IllegalArgumentException e) {
        // best effort. ignore
      }
    }
    return null;
  }

  private static Long literalToTime(Object literal)
  {
    if (literal instanceof Long) {
      return (Long) literal;
    }
    if (literal instanceof Date) {
      return ((Date) literal).getTime();
    }
    if (literal instanceof String) {
      try {
        return DateFormat.getDateInstance().parse((String) literal).getTime();
      }
      catch (ParseException e) {
        // best effort. ignore
      }
    }
    return null;
  }
}
