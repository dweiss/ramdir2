package com.carrotsearch.ramdir2;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.zip.CRC32;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.BufferedChecksum;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexOutput;

public class DelegateIndexOutput extends IndexOutput {
  private RamDataOutput delegate;
  private BufferedChecksum crc;

  public DelegateIndexOutput(RamDataOutput delegate, String resourceDescription, String name) {
    super(resourceDescription, name);
    this.delegate = delegate;
    this.crc = new BufferedChecksum(new CRC32());
  }

  @Override
  public void close() throws IOException {
    delegate = null;
  }

  @Override
  public long getFilePointer() {
    ensureOpen();
    return delegate.size();
  }

  private void ensureOpen() {
    if (delegate == null) {
      throw new AlreadyClosedException("Already closed.");
    }
  }

  @Override
  public long getChecksum() throws IOException {
    ensureOpen();
    return crc.getValue();
  }

  @Override
  public void writeByte(byte b) throws IOException {
    ensureOpen();
    delegate.writeByte(b);
    crc.update(b);
  }

  @Override
  public void writeBytes(byte[] b, int offset, int length) throws IOException {
    ensureOpen();
    delegate.writeBytes(b, offset, length);
    crc.update(b, offset, length);
  }

  @Override
  public void writeBytes(byte[] b, int length) throws IOException {
    delegate.writeBytes(b, length);
  }

  @Override
  public void writeInt(int i) throws IOException {
    delegate.writeInt(i);
  }

  @Override
  public void writeShort(short i) throws IOException {
    delegate.writeShort(i);
  }

  @Override
  public void writeLong(long i) throws IOException {
    delegate.writeLong(i);
  }

  @Override
  public void writeString(String s) throws IOException {
    delegate.writeString(s);
  }

  @Override
  public void copyBytes(DataInput input, long numBytes) throws IOException {
    delegate.copyBytes(input, numBytes);
  }

  @Override
  public void writeMapOfStrings(Map<String, String> map) throws IOException {
    delegate.writeMapOfStrings(map);
  }

  @Override
  public void writeSetOfStrings(Set<String> set) throws IOException {
    delegate.writeSetOfStrings(set);
  }
}
