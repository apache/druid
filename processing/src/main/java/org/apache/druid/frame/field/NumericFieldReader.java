package org.apache.druid.frame.field;

import org.apache.datasketches.memory.Memory;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.column.ValueTypes;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public abstract class NumericFieldReader<T extends Number> implements FieldReader
{
  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(Memory memory, ReadableFieldPointer fieldPointer)
  {
    return new Selector(memory, fieldPointer);
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      Memory memory,
      ReadableFieldPointer fieldPointer,
      @Nullable ExtractionFn extractionFn
  )
  {
    return ValueTypes.makeNumericWrappingDimensionSelector(
        getValueType(),
        makeColumnValueSelector(memory, fieldPointer),
        extractionFn
    );
  }

  @Override
  public boolean isNull(Memory memory, long position)
  {
    return memory.getByte(position) == NumericFieldWriter.NULL_BYTE;
  }

  @Override
  public boolean isComparable()
  {
    return true;
  }

  public abstract ValueType getValueType();

  public abstract Class<? extends T> getClassOfObject();

  public abstract T getValueFromMemory(final Memory memory, final long position);

  public class Selector implements ColumnValueSelector<T>
  {

    private final Memory dataRegion;
    private final ReadableFieldPointer fieldPointer;

    private Selector(final Memory dataRegion, final ReadableFieldPointer fieldPointer)
    {
      this.dataRegion = dataRegion;
      this.fieldPointer = fieldPointer;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {

    }

    @Override
    public double getDouble()
    {
      return getObject().doubleValue();
    }

    @Override
    public float getFloat()
    {
      return getObject().floatValue();
    }

    @Override
    public long getLong()
    {
      return getObject().longValue();
    }

    @Override
    public boolean isNull()
    {
      return NumericFieldReader.this.isNull(dataRegion, fieldPointer.position());
    }

    @Nonnull
    @Override
    public T getObject()
    {
      assert NullHandling.replaceWithDefault() || !isNull();
      return NumericFieldReader.this.getValueFromMemory(dataRegion, fieldPointer.position() + Byte.BYTES);
    }

    @Override
    public Class<? extends T> classOfObject()
    {
      return getClassOfObject();
    }
  }
}
