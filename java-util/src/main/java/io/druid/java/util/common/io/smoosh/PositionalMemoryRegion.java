/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.druid.java.util.common.io.smoosh;

import java.nio.ByteBuffer;

import com.google.common.primitives.Ints;
import com.yahoo.memory.Memory;
import com.yahoo.memory.MemoryRegion;
import com.yahoo.memory.NativeMemory;

import java.nio.ByteOrder;

/**
 * Created by niketh on 1/25/17.
 */
public class PositionalMemoryRegion
{
  private final Memory memory;
  long offSet = 0;
  long sizeOffset = 0;

  public Memory getReadOnlyMemory()
  {
    return memory.asReadOnlyMemory();
  }

  public Memory getMemory()
  {
    return memory;
  }

  public ByteBuffer getByteBuffer()
  {
    return memory.byteBuffer();
  }

  public Memory getRemainingMemory()
  {
    return new MemoryRegion(memory, offSet, sizeOffset - offSet);
  }

  public PositionalMemoryRegion(byte[] bytes)
  {
    this.memory = new NativeMemory(bytes);
    this.offSet = 0;
    this.sizeOffset = memory.getCapacity();
  }

  public PositionalMemoryRegion(int a)
  {
    this.memory = new NativeMemory(ByteBuffer.allocate(Ints.BYTES).order(ByteOrder.nativeOrder()).putInt(a));
    this.offSet = 0;
    this.sizeOffset = memory.getCapacity();
  }

  public PositionalMemoryRegion(MemoryRegion memory)
  {
    this.memory = memory;
    this.offSet = 0;
    this.sizeOffset = memory.getCapacity();
  }

  public PositionalMemoryRegion(ByteBuffer byteBuffer)
  {
    Memory mem = new NativeMemory(byteBuffer);
    this.memory = new MemoryRegion(mem, byteBuffer.position(), byteBuffer.capacity() - byteBuffer.position());
    this.offSet = 0;
    this.sizeOffset = memory.getCapacity();
  }

  private PositionalMemoryRegion(PositionalMemoryRegion pMemory)
  {
    this.memory = pMemory.memory;
    this.offSet = pMemory.position();
    this.sizeOffset = pMemory.getCapacity();
  }

  public PositionalMemoryRegion(PositionalMemoryRegion memory, long position, long capacity)
  {
    this.memory = new MemoryRegion(memory.getMemory(), position, capacity);
    this.offSet = 0;
    this.sizeOffset = this.memory.getCapacity();
  }

  public int getInt(){
    long offSet = this.offSet;
    this.offSet += Integer.BYTES;
    return memory.getInt(offSet);
  }

  public long getLong(){
    long offSet = this.offSet;
    this.offSet += Long.BYTES;
    return memory.getLong(offSet);
  }

  public void getFloatArray(long offSet, float[] dst, int dstStart, int dstCapacity)
  {
    memory.getFloatArray(offSet, dst, dstStart, dstCapacity);
  }

  public void getLongArray(long offSet, long[] dst, int dstStart, int dstCapacity)
  {
    memory.getLongArray(offSet, dst, dstStart, dstCapacity);
  }

  public float getFloat(){
    long offSet = this.offSet;
    this.offSet += Float.BYTES;
    return memory.getFloat(offSet);
  }

  public short getShort(){
    long offSet = this.offSet;
    this.offSet += Short.BYTES;
    return memory.getShort(offSet);
  }

  public short getShort(long offSet){
    return memory.getShort(offSet);
  }


  public int getInt(long offSet){
    return memory.getInt(offSet);
  }

  public long getLong(long offSet)
  {
    return memory.getLong(offSet);
  }

  public float getFloat(long offSet)
  {
    return memory.getFloat(offSet);
  }

  public byte getByte(){
    long offSet = this.offSet;
    this.offSet += Byte.BYTES;
    return memory.getByte(offSet);
  }

  public byte getByte(long offSet){
    return memory.getByte(offSet);
  }

  public byte[] getBytes(int numBytes)
  {
    byte[] bytes = new byte[numBytes];
    long offSet = this.offSet;
    this.offSet += numBytes;
    memory.getByteArray(offSet, bytes, 0, numBytes);
    return bytes;
  }

  public void putByte(Byte b){
    long offSet = this.offSet;
    this.offSet += Byte.BYTES;
    memory.putByte(offSet, b);
  }

  public void putInt(int b){
    long offSet = this.offSet;
    this.offSet += Ints.BYTES;
    memory.putInt(offSet, b);
  }

  public void putByteArray(byte[] bytes){
    long offSet = this.offSet;
    this.offSet += bytes.length;
    memory.putByteArray(offSet, bytes, 0, bytes.length);
  }

  public boolean hasRemaining()
  {
    return sizeOffset - offSet > 0 ? true : false;
  }

  public long getCapacity()
  {
    return memory.getCapacity();
  }
  public long position()
  {
    return offSet;
  }

  public void position(long offSet)
  {
    this.offSet = offSet;
  }

  public long remaining()
  {
    return sizeOffset - offSet;
  }

  public long limit()
  {
    return sizeOffset;
  }

  public void limit(long offSet){
    this.sizeOffset = offSet;
  }

  public long capacity()
  {
    return memory.getCapacity();
  }

  public PositionalMemoryRegion duplicate()
  {
    return new PositionalMemoryRegion(this);
  }

  public static void main(String[] args){

  }
}
