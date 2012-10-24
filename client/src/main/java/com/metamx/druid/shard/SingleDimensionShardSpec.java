package com.metamx.druid.shard;

import com.metamx.druid.input.InputRow;
import com.metamx.druid.partition.PartitionChunk;
import com.metamx.druid.partition.StringPartitionChunk;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonTypeName;

import java.util.List;
import java.util.Map;

/**
 * Class uses getters/setters to work around http://jira.codehaus.org/browse/MSHADE-92
 *
 * Adjust to JsonCreator and final fields when resolved.
 */
public class SingleDimensionShardSpec implements ShardSpec
{
  private String dimension;
  private String start;
  private String end;
  private int partitionNum;

  public SingleDimensionShardSpec()
  {
    this(null, null, null, -1);
  }

  public SingleDimensionShardSpec(
      String dimension,
      String start,
      String end,
      int partitionNum
  )
  {
    this.dimension = dimension;
    this.start = start;
    this.end = end;
    this.partitionNum = partitionNum;
  }

  @JsonProperty("dimension")
  public String getDimension()
  {
    return dimension;
  }

  public void setDimension(String dimension)
  {
    this.dimension = dimension;
  }

  @JsonProperty("start")
  public String getStart()
  {
    return start;
  }

  public void setStart(String start)
  {
    this.start = start;
  }

  @JsonProperty("end")
  public String getEnd()
  {
    return end;
  }

  public void setEnd(String end)
  {
    this.end = end;
  }

  @JsonProperty("partitionNum")
  public int getPartitionNum()
  {
    return partitionNum;
  }

  public void setPartitionNum(int partitionNum)
  {
    this.partitionNum = partitionNum;
  }

  @Override
  public <T> PartitionChunk<T> createChunk(T obj)
  {
    return new StringPartitionChunk<T>(start, end, partitionNum, obj);
  }

  @Override
  public boolean isInChunk(Map<String, String> dimensions)
  {
    return checkValue(dimensions.get(dimension));
  }

  @Override
  public boolean isInChunk(InputRow inputRow)
  {
    final List<String> values = inputRow.getDimension(dimension);

    if (values == null || values.size() != 1) {
      return checkValue(null);
    } else {
      return checkValue(values.get(0));
    }
  }

  private boolean checkValue(String value)
  {
    if (value == null) {
      return start == null;
    }

    if (start == null) {
      return end == null || value.compareTo(end) < 0;
    }

    return value.compareTo(start) >= 0 &&
           (end == null || value.compareTo(end) < 0);
  }

  @Override
  public String toString()
  {
    return "SingleDimensionShardSpec{" +
           "dimension='" + dimension + '\'' +
           ", start='" + start + '\'' +
           ", end='" + end + '\'' +
           ", partitionNum=" + partitionNum +
           '}';
  }
}
