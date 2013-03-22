package com.metamx.druid.query.order;

import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Ordering;
import com.metamx.druid.input.Row;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: dyuan
 * Date: 3/18/13
 * Time: 10:35 AM
 * To change this template use File | Settings | File Templates.
 */
public class TopNSorter<T>
{
  private Ordering<T> ordering;
  public TopNSorter(Ordering<T> ordering)
  {
    this.ordering = ordering;
  }

  public Iterable<T> toTopN(Iterable<T> rows, int n)
  {
    if(n <= 0) return new OrderedQueueItems(MinMaxPriorityQueue.create());

    MinMaxPriorityQueue<T> queue = MinMaxPriorityQueue.orderedBy(ordering).maximumSize(n).create(rows);

    return new OrderedQueueItems(queue);
  }

  public Iterable<T> sortedCopy(Iterable<T> rows)
  {
    return ordering.sortedCopy(rows);
  }
}
