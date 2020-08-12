package org.apache.druid.data.input;

import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.FileEntity;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

public class CountableInputEntityTest
{
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private CountableInputEntity countableInputEntity;
  private InputStats inputStats;
  private byte[] bytes;
  private final int numBytes = 100;

  @Before
  public void setUp() throws Exception
  {
    inputStats = new InputStats();
    bytes = new byte[numBytes];
  }

  @Test
  public void testWithFileEntity() throws IOException
  {
    final File sourceFile = folder.newFile("testWithFileEntity");
    final FileWriter fileWriter = new FileWriter(sourceFile);
    char[] chars = new char[numBytes];
    Arrays.fill(chars, ' ');
    fileWriter.write(chars);
    fileWriter.flush();
    fileWriter.close();
    final FileEntity fileEntity = new FileEntity(sourceFile);
    countableInputEntity = new CountableInputEntity(fileEntity, inputStats);

    final byte[] intermediateBuffer = new byte[numBytes / 2];
    countableInputEntity.open().read(intermediateBuffer);
    Assert.assertEquals(numBytes / 2, inputStats.getProcessedBytes().intValue());

    countableInputEntity.fetch(folder.newFolder(), intermediateBuffer);
    Assert.assertEquals((numBytes / 2) + numBytes, inputStats.getProcessedBytes().intValue());
  }

  @Test
  public void testWithByteEntity() throws IOException
  {
    final byte[] intermediateBuffer = new byte[numBytes];
    final ByteEntity byteEntity = new ByteEntity(bytes);
    countableInputEntity = new CountableInputEntity(byteEntity, inputStats);
    countableInputEntity.open().read(intermediateBuffer);
    Assert.assertEquals(numBytes, inputStats.getProcessedBytes().intValue());

    final byte[] smallIntermediateBuffer = new byte[25];
    final ByteEntity byteEntity1 = new ByteEntity(bytes);
    countableInputEntity = new CountableInputEntity(byteEntity1, inputStats);
    countableInputEntity.fetch(folder.newFolder(), smallIntermediateBuffer);
    Assert.assertEquals(numBytes + numBytes, inputStats.getProcessedBytes().intValue());
  }
}
