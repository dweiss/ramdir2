package com.carrotsearch.ramdir2;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;

public class DelegateIndexInput extends IndexInput implements RandomAccessInput {
  private RamDataInput in;

  public DelegateIndexInput(RamDataInput in, String resourceDescription) {
    super(resourceDescription);
    this.in = in;
  }

  @Override
  public void close() throws IOException {
    in = null;
  }

  @Override
  public long getFilePointer() {
    return in.position();
  }

  @Override
  public void seek(long pos) throws IOException {
    in.seek(pos);
  }

  @Override
  public long length() {
    return in.size();
  }

  @Override
  public DelegateIndexInput slice(String sliceDescription, long offset, long length) throws IOException {
    return new DelegateIndexInput(in.slice(offset, length), "Sliced offset=" + offset + ", length=" + length + " " + toString());
  }

  @Override
  public byte readByte() throws IOException {
    return in.readByte();
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) throws IOException {
    in.readBytes(b, offset, len);
  }

  @Override
  public RandomAccessInput randomAccessSlice(long offset, long length) throws IOException {
    return slice("", offset, length);
  }

  @Override
  public void readBytes(byte[] b, int offset, int len, boolean useBuffer) throws IOException {
    in.readBytes(b, offset, len, useBuffer);
  }

  @Override
  public short readShort() throws IOException {
    return in.readShort();
  }

  @Override
  public int readInt() throws IOException {
    return in.readInt();
  }

  @Override
  public int readVInt() throws IOException {
    return in.readVInt();
  }

  @Override
  public int readZInt() throws IOException {
    return in.readZInt();
  }

  @Override
  public long readLong() throws IOException {
    return in.readLong();
  }

  @Override
  public long readVLong() throws IOException {
    return in.readVLong();
  }

  @Override
  public long readZLong() throws IOException {
    return in.readZLong();
  }

  @Override
  public String readString() throws IOException {
    return in.readString();
  }

  @Override
  public Map<String, String> readMapOfStrings() throws IOException {
    return in.readMapOfStrings();
  }

  @Override
  public Set<String> readSetOfStrings() throws IOException {
    return in.readSetOfStrings();
  }

  @Override
  public void skipBytes(long numBytes) throws IOException {
    in.seek(in.position() + numBytes);
  }

  @Override
  public byte readByte(long pos) throws IOException {
    return in.readByte(pos);
  }

  @Override
  public short readShort(long pos) throws IOException {
    return in.readShort(pos);
  }

  @Override
  public int readInt(long pos) throws IOException {
    return in.readInt(pos);
  }

  @Override
  public long readLong(long pos) throws IOException {
    return in.readLong(pos);
  }
  
  @Override
  public IndexInput clone() {
    DelegateIndexInput cloned = new DelegateIndexInput(in.slice(0, in.size()), "Cloned: " + toString());
    return cloned;
  }
}
