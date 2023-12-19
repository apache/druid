package org.apache.druid.data.input.impl.delta;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.Row;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.List;

public class DeltaInputRow implements InputRow
{
  private final io.delta.kernel.data.Row row;
  private final InputRowSchema schema;

  public DeltaInputRow(io.delta.kernel.data.Row row, InputRowSchema schema)
  {
    this.row = row;
    this.schema = schema;
  }
  @Override
  public List<String> getDimensions()
  {
    return row.getSchema().fieldNames();
  }

  @Override
  public long getTimestampFromEpoch()
  {
    return row.getLong(0);
  }

  @Override
  public DateTime getTimestamp()
  {
    String tsCol = schema.getTimestampSpec().getTimestampColumn();
    return new DateTime(row.getLong(tsCol));
    return null;
  }

  @Override
  public List<String> getDimension(String dimension)
  {
    return null;
  }

  @Nullable
  @Override
  public Object getRaw(String dimension)
  {
    return null;
  }

  @Nullable
  @Override
  public Number getMetric(String metric)
  {
    return null;
  }

  @Override
  public int compareTo(Row o)
  {
    return 0;
  }
}
