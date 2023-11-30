package org.apache.druid.query.rowsandcols.concrete;

import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.segment.FrameStorageAdapter;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.LinkedHashMap;

public class RowBasedFrameRowAndColumns implements RowsAndColumns
{
  private final Frame frame;
  private final RowSignature signature;
  private final LinkedHashMap<String, Column> colCache = new LinkedHashMap<>();

  public RowBasedFrameRowAndColumns(Frame frame, RowSignature signature) {
    this.frame = FrameType.ROW_BASED.ensureType(frame);;
    this.signature = signature;
  }

  @Override
  public Collection<String> getColumnNames()
  {
    return signature.getColumnNames();
  }

  @Override
  public int numRows()
  {
    return frame.numRows();
  }

  @Nullable
  @Override
  public Column findColumn(String name)
  {
    return null;
  }

  @SuppressWarnings("unchecked")
  @Nullable
  @Override
  public <T> T as(Class<T> clazz)
  {
    if (StorageAdapter.class.equals(clazz)) {
      return (T) new FrameStorageAdapter(frame, FrameReader.create(signature), Intervals.ETERNITY);
    }
    return null;
  }
}
