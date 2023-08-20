package org.apache.druid.frame.field;

import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.frame.write.FrameWriterUtils;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class NumericArrayFieldWriter implements FieldWriter
{

  public static final byte NULL_ROW = 0x00;
  public static final byte NON_NULL_ROW = 0x01;

  private final ColumnValueSelector selector;
  private final NumericFieldWriterFactory writerFactory;

  public static NumericArrayFieldWriter getLongArrayFieldWriter(final ColumnValueSelector selector)
  {
    return new NumericArrayFieldWriter(selector, LongFieldWriter::new);
  }

  public static NumericArrayFieldWriter getFloatArrayFieldWriter(final ColumnValueSelector selector)
  {
    return new NumericArrayFieldWriter(selector, FloatFieldWriter::new);
  }

  public static NumericArrayFieldWriter getDoubleArrayFieldWRiter(final ColumnValueSelector selector)
  {
    return new NumericArrayFieldWriter(selector, DoubleFieldWriter::new);
  }

  public NumericArrayFieldWriter(final ColumnValueSelector selector, NumericFieldWriterFactory writerFactory)
  {
    this.selector = selector;
    this.writerFactory = writerFactory;
  }

  @Override
  public long writeTo(WritableMemory memory, long position, long maxSize)
  {
    if (selector.isNull()) {
      int requiredSize = Byte.BYTES;
      if (requiredSize < maxSize) {
        return -1;
      }
      memory.putByte(position, NULL_ROW);
      return requiredSize;
    } else {
      List<? extends Number> list = FrameWriterUtils.getNumericArrayFromNumericArraySelector(selector);

      if (list == null) {
        int requiredSize = Byte.BYTES;
        if (requiredSize < maxSize) {
          return -1;
        }
        memory.putByte(position, NULL_ROW);
        return requiredSize;
      }

      AtomicInteger index = new AtomicInteger(0);
      ColumnValueSelector<Number> columnValueSelector = new ColumnValueSelector<Number>()
      {
        @Override
        public double getDouble()
        {
          final Number n = getObject();
          assert NullHandling.replaceWithDefault() || n != null;
          return n != null ? n.doubleValue() : 0d;
        }

        @Override
        public float getFloat()
        {
          final Number n = getObject();
          assert NullHandling.replaceWithDefault() || n != null;
          return n != null ? n.floatValue() : 0f;
        }

        @Override
        public long getLong()
        {
          final Number n = getObject();
          assert NullHandling.replaceWithDefault() || n != null;
          return n != null ? n.longValue() : 0L;
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {

        }

        @Override
        public boolean isNull()
        {
          return !NullHandling.replaceWithDefault() && getObject() == null;
        }

        @Nullable
        @Override
        public Number getObject()
        {
          return list.get(index.get());
        }

        @Override
        public Class<? extends Number> classOfObject()
        {
          return Number.class;
        }
      };

      NumericFieldWriter writer = writerFactory.get(columnValueSelector);


      int requiredSize = Byte.BYTES + (writer.getNumericSize() + Byte.BYTES) * list.size();

      if (requiredSize > maxSize) {
        return -1;
      }

      memory.putByte(position, NON_NULL_ROW);

      for (; index.get() < list.size(); index.incrementAndGet()) {
        long offset = Byte.BYTES + (long) index.get() * (writer.getNumericSize() + Byte.BYTES);
        writer.writeTo(
            memory,
            position + offset,
            maxSize - offset
        );
      }

      return requiredSize;

    }
  }

  @Override
  public void close()
  {
    // Do nothing
  }
}
