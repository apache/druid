package io.druid.segment.store;


import java.io.EOFException;
import java.io.IOException;

/**
 * Base implementation class for buffered {@link IndexInput}.
 */
public abstract class BufferedIndexInput extends IndexInput implements RandomAccessInput
{

  /**
   * Default buffer size set to {@value #BUFFER_SIZE}.
   */
  public static final int BUFFER_SIZE = 1024;

  /**
   * Minimum buffer size allowed
   */
  public static final int MIN_BUFFER_SIZE = 8;

  // The normal read buffer size defaults to 1024, but
  // increasing this during merging seems to yield
  // performance gains.  However we don't want to increase
  // it too much because there are quite a few
  // BufferedIndexInputs created during merging.
  /**
   * A buffer size for merges set to {@value #MERGE_BUFFER_SIZE}.
   */
  public static final int MERGE_BUFFER_SIZE = 4096;

  private int bufferSize = BUFFER_SIZE;

  protected byte[] buffer;

  private long bufferStart = 0;       // position in file of buffer
  private int bufferLength = 0;       // end of valid bytes
  private int bufferPosition = 0;     // next byte to read

  @Override
  public final byte readByte() throws IOException
  {
    if (bufferPosition >= bufferLength) {
      refill();
    }
    return buffer[bufferPosition++];
  }

  public BufferedIndexInput()
  {
    this(0L,BUFFER_SIZE);
  }

  /**
   * for slice or duplicated one to specify the file start position
   * @param fileStartPosition
   * @param bufferSize if null, default value is BUFFER_SIZE
   */
  public BufferedIndexInput(long fileStartPosition, Integer bufferSize)
  {
    if(bufferSize ==null){
      bufferSize = BUFFER_SIZE;
    }
    checkBufferSize(bufferSize);
    this.bufferSize = bufferSize;
    this.bufferStart = fileStartPosition;
  }


  protected void newBuffer(byte[] newBuffer)
  {
    // Subclasses can do something here
    buffer = newBuffer;
  }


  private void checkBufferSize(int bufferSize)
  {
    if (bufferSize < MIN_BUFFER_SIZE) {
      throw new IllegalArgumentException("bufferSize must be at least MIN_BUFFER_SIZE (got " + bufferSize + ")");
    }
  }

  @Override
  public final void readBytes(byte[] b, int offset, int len) throws IOException
  {
    readBytes(b, offset, len, true);
  }


  public final void readBytes(byte[] b, int offset, int len, boolean useBuffer) throws IOException
  {
    int available = bufferLength - bufferPosition;
    if (len <= available) {
      // the buffer contains enough data to satisfy this request
      if (len > 0) // to allow b to be null if len is 0...
      {
        System.arraycopy(buffer, bufferPosition, b, offset, len);
      }
      bufferPosition += len;
    } else {
      // the buffer does not have enough data. First serve all we've got.
      if (available > 0) {
        System.arraycopy(buffer, bufferPosition, b, offset, available);
        offset += available;
        len -= available;
        bufferPosition += available;
      }
      // and now, read the remaining 'len' bytes:
      if (useBuffer && len < bufferSize) {
        // If the amount left to read is small enough, and
        // we are allowed to use our buffer, do it in the usual
        // buffered way: fill the buffer and copy from it:
        refill();
        if (bufferLength < len) {
          // Throw an exception when refill() could not read len bytes:
          System.arraycopy(buffer, 0, b, offset, bufferLength);
          throw new EOFException("read past EOF: " + this);
        } else {
          System.arraycopy(buffer, 0, b, offset, len);
          bufferPosition = len;
        }
      } else {
        // The amount left to read is larger than the buffer
        // or we've been asked to not use our buffer -
        // there's no performance reason not to read it all
        // at once. Note that unlike the previous code of
        // this function, there is no need to do a seek
        // here, because there's no need to reread what we
        // had in the buffer.
        long after = bufferStart + bufferPosition + len;
        if (len > remaining()) {
          throw new EOFException("read past EOF: " + this);
        }
        readInternal(b, offset, len);
        bufferStart = after;
        bufferPosition = 0;
        bufferLength = 0;                    // trigger refill() on read
      }
    }
  }

  @Override
  public final short readShort() throws IOException
  {
    if (2 <= (bufferLength - bufferPosition)) {
      return (short) (((buffer[bufferPosition++] & 0xFF) << 8) | (buffer[bufferPosition++] & 0xFF));
    } else {
      return super.readShort();
    }
  }

  @Override
  public final int readInt() throws IOException
  {
    if (4 <= (bufferLength - bufferPosition)) {
      return ((buffer[bufferPosition++] & 0xFF) << 24) | ((buffer[bufferPosition++] & 0xFF) << 16)
             | ((buffer[bufferPosition++] & 0xFF) << 8) | (buffer[bufferPosition++] & 0xFF);
    } else {
      return super.readInt();
    }
  }

  @Override
  public final long readLong() throws IOException
  {
    if (8 <= (bufferLength - bufferPosition)) {
      final int i1 = ((buffer[bufferPosition++] & 0xff) << 24) | ((buffer[bufferPosition++] & 0xff) << 16) |
                     ((buffer[bufferPosition++] & 0xff) << 8) | (buffer[bufferPosition++] & 0xff);
      final int i2 = ((buffer[bufferPosition++] & 0xff) << 24) | ((buffer[bufferPosition++] & 0xff) << 16) |
                     ((buffer[bufferPosition++] & 0xff) << 8) | (buffer[bufferPosition++] & 0xff);
      return (((long) i1) << 32) | (i2 & 0xFFFFFFFFL);
    } else {
      return super.readLong();
    }
  }

  //free buffer
  @Override
  public void close() throws IOException
  {
    this.buffer = null;
  }

  //***********************implements RandomAccessInput start **********************
  @Override
  public synchronized final byte readByte(long pos) throws IOException
  {
    long index = pos - bufferStart;
    if (index < 0 || index >= bufferLength) {
      bufferStart = pos;
      bufferPosition = 0;
      bufferLength = 0;  // trigger refill() on read()
      seekInternal(pos);
      refill();
      index = 0;
    }
    return buffer[(int) index];
  }

  @Override
  public synchronized final short readShort(long pos) throws IOException
  {
    long index = pos - bufferStart;
    if (index < 0 || index >= bufferLength - 1) {
      bufferStart = pos;
      bufferPosition = 0;
      bufferLength = 0;  // trigger refill() on read()
      seekInternal(pos);
      refill();
      index = 0;
    }
    return (short) (((buffer[(int) index] & 0xFF) << 8) |
                    (buffer[(int) index + 1] & 0xFF));
  }

  @Override
  public synchronized final int readInt(long pos) throws IOException
  {
    long index = pos - bufferStart;
    if (index < 0 || index >= bufferLength - 3) {
      bufferStart = pos;
      bufferPosition = 0;
      bufferLength = 0;  // trigger refill() on read()
      seekInternal(pos);
      refill();
      index = 0;
    }
    return ((buffer[(int) index] & 0xFF) << 24) |
           ((buffer[(int) index + 1] & 0xFF) << 16) |
           ((buffer[(int) index + 2] & 0xFF) << 8) |
           (buffer[(int) index + 3] & 0xFF);
  }

  @Override
  public synchronized final long readLong(long pos) throws IOException
  {
    long index = pos - bufferStart;
    if (index < 0 || index >= bufferLength - 7) {
      bufferStart = pos;
      bufferPosition = 0;
      bufferLength = 0;  // trigger refill() on read()
      seekInternal(pos);
      refill();
      index = 0;
    }
    final int i1 = ((buffer[(int) index] & 0xFF) << 24) |
                   ((buffer[(int) index + 1] & 0xFF) << 16) |
                   ((buffer[(int) index + 2] & 0xFF) << 8) |
                   (buffer[(int) index + 3] & 0xFF);
    final int i2 = ((buffer[(int) index + 4] & 0xFF) << 24) |
                   ((buffer[(int) index + 5] & 0xFF) << 16) |
                   ((buffer[(int) index + 6] & 0xFF) << 8) |
                   (buffer[(int) index + 7] & 0xFF);
    return (((long) i1) << 32) | (i2 & 0xFFFFFFFFL);
  }



  //***********************implements RandomAccessInput end**********************
  private void refill() throws IOException
  {
    long start = bufferStart + bufferPosition;
    long end = start + bufferSize;
    long remaining = this.remaining();
    if (bufferSize > remaining)  // don't read past EOF
    {
      end = start+remaining;
    }
    int newLength = (int) (end - start);
    if (newLength <= 0) {
      throw new EOFException("read past EOF: " + this);
    }

    if (buffer == null) {
      newBuffer(new byte[bufferSize]);  // allocate buffer lazily
      seekInternal(bufferStart);
    }
    readInternal(buffer, 0, newLength);
    bufferLength = newLength;
    bufferStart = start;
    bufferPosition = 0;
  }

  /**
   * Expert: implements buffer refill.  Reads bytes from the current position
   * in the input.
   *
   * @param b      the array to read bytes into
   * @param offset the offset in the array to start storing bytes
   * @param length the number of bytes to read
   */
  protected abstract void readInternal(byte[] b, int offset, int length)
      throws IOException;

  @Override
  public final long getFilePointer() throws IOException { return bufferStart + bufferPosition; }

  @Override
  public final void seek(long pos) throws IOException
  {
    if (pos >= bufferStart && pos < (bufferStart + bufferLength)) {
      bufferPosition = (int) (pos - bufferStart);  // seek within buffer
    } else {
      bufferStart = pos;
      bufferPosition = 0;
      bufferLength = 0;  // trigger refill() on read()
      seekInternal(pos);
    }
  }

  /**
   * Expert: implements seek.  Sets current position in this file, where the
   * next {@link #readInternal(byte[], int, int)} will occur.
   *
   * @see #readInternal(byte[], int, int)
   */
  protected abstract void seekInternal(long pos) throws IOException;

  /**
   *
   * create a duplicated one ,which offset position and available length should be the same with the initial one.
   * @return
   */
  public final IndexInput duplicate() throws IOException{
    long position = this.getFilePointer();
    return duplicateWithFixedPosition(position);
  }

  /**
   * duplicate according to the assigned 、fixed start position
   * @param startPosition
   * @return
   * @throws IOException
   */
  public abstract IndexInput duplicateWithFixedPosition(long startPosition)throws IOException;


}
