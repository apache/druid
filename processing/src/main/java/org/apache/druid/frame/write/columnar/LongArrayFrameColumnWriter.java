package org.apache.druid.frame.write.columnar;

import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.frame.allocation.AppendableMemory;
import org.apache.druid.frame.allocation.MemoryAllocator;
import org.apache.druid.frame.allocation.MemoryRange;
import org.apache.druid.frame.write.FrameWriterUtils;
import org.apache.druid.segment.ColumnValueSelector;

import java.util.List;

public class LongArrayFrameColumnWriter implements FrameColumnWriter
{
  // TODO(laksh): Copy pasted after following the logic from StringFrameColumnWriter, since numeric writers have 3
  //  regions as well. The type size is fixed, however there's no special nullable marker
  private static final int INITIAL_ALLOCATION_SIZE = 180;

  private static final int ELEMENT_SIZE = Long.BYTES;

  private static final byte NULL_ELEMENT_MARKER = 0x00;
  private static final byte NON_NULL_ELEMENT_MARKER = 0x01;

  /**
   * A byte required at the beginning for type code
   */
  public static final long DATA_OFFSET = 1;

  final ColumnValueSelector selector;
  final MemoryAllocator allocator;
  final byte typeCode;



  /**
   * Row lengths: one int per row with the number of values contained by that row and all previous rows.
   * Only written for multi-value and array columns. When the corresponding row is null itself, the length is
   * written as -(actual length) - 1. (Guaranteed to be a negative number even if "actual length" is zero.)
   */
  private final AppendableMemory cumulativeRowLengths;

  /**
   * Denotes if the element of the row is null or not
   */
  private final AppendableMemory rowNullityData;

  /**
   * Row data.
   */
  private final AppendableMemory rowData;

  private int lastCumulativeRowLength = 0;
  private int lastRowLength = 0;


  public LongArrayFrameColumnWriter(
      final ColumnValueSelector selector,
      final MemoryAllocator allocator,
      final byte typeCode
  )
  {
    this.selector = selector;
    this.allocator = allocator;
    this.typeCode = typeCode;
    this.cumulativeRowLengths = AppendableMemory.create(allocator, INITIAL_ALLOCATION_SIZE);
    this.rowNullityData = AppendableMemory.create(allocator, INITIAL_ALLOCATION_SIZE);
    this.rowData = AppendableMemory.create(allocator, INITIAL_ALLOCATION_SIZE);
  }

  @Override
  public boolean addSelection()
  {
    List<? extends Number> numericArray = FrameWriterUtils.getNumericArrayFromObject(selector.getObject());
    int rowLength = numericArray == null ? 0 : numericArray.size();

    if ((long) lastCumulativeRowLength + rowLength > Integer.MAX_VALUE) {
      return false;
    }

    if (!cumulativeRowLengths.reserveAdditional(Integer.BYTES)) {
      return false;
    }

    if (!rowNullityData.reserveAdditional(rowLength * Byte.BYTES)) {
      return false;
    }

    if (!rowData.reserveAdditional(rowLength * ELEMENT_SIZE)) {
      return false;
    }

    final MemoryRange<WritableMemory> rowLengthsCursor = cumulativeRowLengths.cursor();

    if (numericArray == null) {
      rowLengthsCursor.memory().putInt(rowLengthsCursor.start(), -(lastCumulativeRowLength + rowLength) - 1);
    }
    cumulativeRowLengths.advanceCursor(Integer.BYTES);
    lastRowLength = rowLength;
    lastCumulativeRowLength += rowLength;

    final MemoryRange<WritableMemory> rowNullityDataCursor = rowLength > 0 ? rowData.cursor() : null;
    final MemoryRange<WritableMemory> rowDataCursor = rowLength > 0 ? rowData.cursor() : null;
    for (int i = 0; i < rowLength; ++i) {
      final Number element = numericArray.get(i);
      if (element == null) {
        rowNullityDataCursor.memory().putByte(rowNullityDataCursor.start() + Byte.BYTES * i, NULL_ELEMENT_MARKER);
        rowDataCursor.memory().putLong(rowDataCursor.start() + (long) Long.BYTES * i, 0);
      } else {
        rowNullityDataCursor.memory().putByte(rowNullityDataCursor.start() + Byte.BYTES * i, NON_NULL_ELEMENT_MARKER);
        // Cast should be safe, as we have a long array column value selector, and we have checked that it is not null
        rowDataCursor.memory().putLong(rowDataCursor.start() + (long) Long.BYTES * i, (long) element);
      }
    }

  }

  @Override
  public void undo()
  {

  }

  @Override
  public long size()
  {
    return 0;
  }

  @Override
  public long writeTo(WritableMemory memory, long position)
  {
    return 0;
  }

  @Override
  public void close()
  {

  }
}
