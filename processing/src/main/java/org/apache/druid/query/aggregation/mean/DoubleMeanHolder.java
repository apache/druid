package org.apache.druid.query.aggregation.mean;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.primitives.Doubles;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;

public class DoubleMeanHolder
{
  public static final int MAX_INTERMEDIATE_SIZE = Long.SIZE + Double.SIZE;
  public static final Comparator<DoubleMeanHolder> COMPARATOR = (o1, o2) -> Doubles.compare(o1.mean(), o2.mean());

  private double sum;
  private long count;

  public DoubleMeanHolder(double sum, long count)
  {
    this.sum = sum;
    this.count = count;
  }

  public void update(double sum)
  {
    this.sum += sum;
    count++;
  }

  public DoubleMeanHolder update(DoubleMeanHolder other)
  {
    sum += other.sum;
    count += other.count;
    return this;
  }

  public double mean()
  {
    return count == 0 ? 0 : sum/count;
  }

  public byte[] toBytes()
  {
    ByteBuffer buf = ByteBuffer.allocate(Double.SIZE + Long.SIZE);
    buf.putDouble(0, sum);
    buf.putLong(Double.SIZE, count);
    return buf.array();
  }

  public static DoubleMeanHolder fromBytes(byte[] data)
  {
    ByteBuffer buf = ByteBuffer.wrap(data);
    return new DoubleMeanHolder(buf.getDouble(0), buf.getLong(Double.SIZE));
  }

  public static void init(ByteBuffer buf, int position)
  {
    writeSum(buf, position, 0d);
    writeCount(buf, position, 0);
  }

  public static void update(ByteBuffer buf, int position, double sum)
  {
    writeSum(buf, position, getSum(buf, position) + sum);
    writeCount(buf, position, getCount(buf, position) + 1);
  }

  public static void update(ByteBuffer buf, int position, DoubleMeanHolder other)
  {
    writeSum(buf, position, getSum(buf, position) + other.sum);
    writeCount(buf, position, getCount(buf, position) + other.count);
  }

  public static DoubleMeanHolder get(ByteBuffer buf, int position)
  {
    return new DoubleMeanHolder(getSum(buf, position), getCount(buf, position));
  }

  private static void writeSum(ByteBuffer buf, int position, double sum)
  {
    buf.putDouble(position, sum);
  }

  private static double getSum(ByteBuffer buf, int position)
  {
    return buf.getDouble(position);
  }

  private static void writeCount(ByteBuffer buf, int position, long count)
  {
    buf.putLong(position + Double.SIZE, count);
  }

  private static long getCount(ByteBuffer buf, int position)
  {
    return buf.getLong(position + Double.SIZE);
  }

  public static class Serializer extends JsonSerializer<DoubleMeanHolder>
  {
    public static final Serializer INSTANCE = new Serializer();

    private Serializer() {}

    @Override
    public void serialize(DoubleMeanHolder obj, JsonGenerator jgen, SerializerProvider provider)
      throws IOException
    {
      jgen.writeBinary(obj.toBytes());
    }
  }
}
