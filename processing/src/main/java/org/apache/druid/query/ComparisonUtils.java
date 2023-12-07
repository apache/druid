package org.apache.druid.query;

import com.google.common.collect.Ordering;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.TypeSignature;
import org.apache.druid.segment.column.ValueType;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class ComparisonUtils
{

  public static Comparator<Object> getComparatorForType(TypeSignature<ValueType> type)
  {
    switch (type.getType()) {
      case LONG:
        // The type of the object should be Long
      case DOUBLE:
        // The type of the object should be Double
      case FLOAT:
        // The type of the object should be Float
      case STRING:
        // The type of the object should be String
        return Comparator.nullsFirst((lhs, rhs) -> ((Comparable) lhs).compareTo(rhs));
      case ARRAY:
        switch (type.getElementType().getType()) {
          case STRING:
          case LONG:
          case FLOAT:
          case DOUBLE:
            // The type of the object should be List<ElementType>
            return Comparator.nullsFirst(new ListComparator(Comparators.naturalNullsFirst()));
        }
      default:
        throw DruidException.defensive("Type[%s] cannot be compared", type);
    }
  }

  /**
   * Should be a null friendly element comparator
   */
  public static class ListComparator<T> implements Comparator<Object[]>
  {
    final Comparator<T> elementComparator;

    public ListComparator(Comparator<T> elementComparator)
    {
      this.elementComparator = elementComparator;
    }


    // TODO(laksh): Test this code
    @Override
    public int compare(Object[] lhs, Object[] rhs)
    {
      //noinspection ArrayEquality
      if (lhs == rhs) {
        return 0;
      }

      if (lhs == null) {
        return -1;
      }

      if (rhs == null) {
        return 1;
      }

      final int minSize = Math.min(lhs.length, rhs.length);

      for (int index = 0; index < minSize; ++index) {
        Object lhsElement = lhs[index];
        Object rhsElement = rhs[index];
        int cmp = elementComparator.compare((T) lhsElement, (T) rhsElement);
        if (cmp != 0) {
          return cmp;
        }
      }

      if (lhs.length == rhs.length) {
        return 0;
      } else if (lhs.length < rhs.length) {
        return -1;
      }
      return 1;
    }
  }
}
