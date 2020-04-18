package org.apache.druid.segment.writeout;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileWriteOutBytesTest
{
  private FileWriteOutBytes fileWriteOutBytes;
  private FileChannel mockFileChannel;

  @Before
  public void setUp()
  {
    this.mockFileChannel = EasyMock.mock(FileChannel.class);
    this.fileWriteOutBytes = new FileWriteOutBytes(EasyMock.mock(File.class), mockFileChannel);
  }

  @Test
  public void testWrite4KBInts() throws IOException
  {
    // Write 4KB of ints and expect the write operation of the file channel will be triggered only once.
    EasyMock.expect(this.mockFileChannel.write(EasyMock.anyObject(ByteBuffer.class)))
            .andAnswer(() -> {
              ByteBuffer buffer = (ByteBuffer) EasyMock.getCurrentArguments()[0];
              int remaining = buffer.remaining();
              buffer.position(remaining);
              return remaining;
            }).times(1);
    EasyMock.replay(this.mockFileChannel);
    final int writeBytes = 4096;
    final int numOfInt = writeBytes / Integer.BYTES;
    for (int i = 0; i < numOfInt; i++) {
      this.fileWriteOutBytes.writeInt(i);
    }
    this.fileWriteOutBytes.flush();
    EasyMock.verify(this.mockFileChannel);
  }
}
