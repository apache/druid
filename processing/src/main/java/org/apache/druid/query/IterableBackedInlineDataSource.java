package org.apache.druid.query;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import org.apache.druid.frame.Frame;
import org.apache.druid.segment.RowAdapter;
import org.apache.druid.segment.column.RowSignature;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class IterableBackedInlineDataSource extends InlineDataSource
{
  private final Iterable<Object[]> rows;
  private final RowSignature signature;

  public IterableBackedInlineDataSource(
      final Iterable<Object[]> rows,
      final RowSignature signature
  )
  {
    this.rows = Preconditions.checkNotNull(rows, "'rows' must be non null");
    this.signature = Preconditions.checkNotNull(signature, "'signature' must be non null");
  }

  private static int rowsHashCode(final Iterable<Object[]> rows)
  {
    if (rows instanceof List) {
      final List<Object[]> list = (List<Object[]>) rows;

      int code = 1;
      for (final Object[] row : list) {
        code = 31 * code + Arrays.hashCode(row);
      }

      return code;
    } else {
      return Objects.hash(rows);
    }
  }

  @JsonIgnore
  @Override
  public Iterable<Object[]> getRows()
  {
    return rows;
  }

  @Override
  public RowSignature getRowSignature()
  {
    return null;
  }

  public RowAdapter<Object[]> rowAdapter()
  {
    return columnName -> {
      final int columnNumber = signature.indexOf(columnName);

      if (columnNumber >= 0) {
        return row -> row[columnNumber];
      } else {
        return row -> null;
      }
    };
  }


  @Override
  public int hashCode()
  {
    return Objects.hash(rowsHashCode(rows), signature);
  }

}
