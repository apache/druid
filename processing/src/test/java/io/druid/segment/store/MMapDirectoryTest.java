package io.druid.segment.store;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;

/**
 * test for mmap read write operations using IndexInput and IndexOutput
 */
public class MMapDirectoryTest
{


  private MMapDirectory creatMMapDir() throws IOException
  {
    String tmpDirName = "tmp" + UUID.randomUUID().toString();
    File current = new File(tmpDirName);
    current.delete();
    current.mkdir();
    current.deleteOnExit();
    Path tmpPath = current.toPath();
    MMapDirectory mMapDirectory = new MMapDirectory(tmpPath);
    return mMapDirectory;
  }

  @Test
  public void testRWByte()
  {
    try {
      String indexFileName = "index.drd";
      MMapDirectory mMapDirectory = creatMMapDir();
      Path current = mMapDirectory.getDirectory();
      //to write
      IndexOutput indexOutput = mMapDirectory.createOutput(indexFileName);
      byte inputByte1 = 1;
      indexOutput.writeByte(inputByte1);
      indexOutput.close();
      //to read
      IndexInput indexInput = mMapDirectory.openInput(indexFileName);
      byte readByte1 = indexInput.readByte();
      Assert.assertEquals(readByte1, inputByte1);
      indexInput.close();
      FileUtils.deleteDirectory(current.toFile());
    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testRWInt()
  {
    try {
      String indexFileName = "index.drd";
      MMapDirectory mMapDirectory = creatMMapDir();
      Path current = mMapDirectory.getDirectory();
      //to write
      IndexOutput indexOutput = mMapDirectory.createOutput(indexFileName);
      int inputInt1 = 1;
      indexOutput.writeInt(inputInt1);
      indexOutput.close();
      //to read
      IndexInput indexInput = mMapDirectory.openInput(indexFileName);
      int readInt1 = indexInput.readInt();
      Assert.assertEquals(inputInt1, readInt1);
      indexInput.close();
      FileUtils.deleteDirectory(current.toFile());
    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testRWLong()
  {
    try {
      String indexFileName = "index.drd";
      MMapDirectory mMapDirectory = creatMMapDir();
      Path current = mMapDirectory.getDirectory();
      //to write
      IndexOutput indexOutput = mMapDirectory.createOutput(indexFileName);
      long inputLong1 = 1L;
      indexOutput.writeLong(inputLong1);
      indexOutput.close();
      //to read
      IndexInput indexInput = mMapDirectory.openInput(indexFileName);
      long readLong1 = indexInput.readLong();
      Assert.assertEquals(inputLong1, readLong1);
      indexInput.close();
      FileUtils.deleteDirectory(current.toFile());
    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testRWString()
  {
    try {
      String indexFileName = "index.drd";
      MMapDirectory mMapDirectory = creatMMapDir();
      Path current = mMapDirectory.getDirectory();
      //to write
      IndexOutput indexOutput = mMapDirectory.createOutput(indexFileName);
      //write string
      String inputStr = "hello Druid !";
      byte[] inputBytes = inputStr.getBytes();
      int inputBytesSize = inputBytes.length;
      indexOutput.writeBytes(inputBytes, inputBytesSize);
      indexOutput.close();
      //to read
      IndexInput indexInput = mMapDirectory.openInput(indexFileName);
      byte[] readBytes = new byte[inputBytesSize];
      indexInput.readBytes(readBytes, 0, inputBytesSize);
      String directReadStr = new String(readBytes);
      Assert.assertEquals(inputStr, directReadStr);
      indexInput.close();
      FileUtils.deleteDirectory(current.toFile());
    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testRWSlice()
  {
    try {
      String indexFileName = "index.drd";
      MMapDirectory mMapDirectory = creatMMapDir();
      Path current = mMapDirectory.getDirectory();
      //to write
      IndexOutput indexOutput = mMapDirectory.createOutput(indexFileName);
      //write string
      String inputStr = "hello Druid !";
      byte[] inputBytes = inputStr.getBytes();
      int inputBytesSize = inputBytes.length;
      indexOutput.writeBytes(inputBytes, inputBytesSize);
      indexOutput.close();
      //to read
      IndexInput indexInput = mMapDirectory.openInput(indexFileName);
      long position = indexInput.getFilePointer();
      byte[] sliceReadBytes = new byte[inputBytesSize];
      IndexInput sliceInput = indexInput.slice(position, inputBytesSize);
      sliceInput.readBytes(sliceReadBytes, 0, inputBytesSize);
      String sliceReadStr = new String(sliceReadBytes);
      Assert.assertEquals(inputStr, sliceReadStr);
      indexInput.close();
      FileUtils.deleteDirectory(current.toFile());
    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }


}
