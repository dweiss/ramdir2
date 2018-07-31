package com.carrotsearch.ramdir2;

import java.io.IOException;

import org.apache.lucene.store.IndexInput;

public final class ByteArrayIndexInput extends IndexInput {
  private byte[] bytes;

  private int offs;
  private int pos;
  private int length;

  public ByteArrayIndexInput(String description, byte[] bytes, int offs, int length) {
    super(description);
    this.bytes = bytes;
    this.offs = offs;
    this.length = length;

    this.pos = offs;
  }

  public long getFilePointer() {
    return pos - offs;
  }
  
  public void seek(long pos) {
    this.pos = (int) (pos + offs);
  }

  @Override
  public long length() {
    return length;
  }

  public boolean eof() {
    return getFilePointer() == length;
  }

  @Override
  public void skipBytes(long count) {
    pos += count;
  }

  @Override
  public short readShort() {
    return (short) (((bytes[pos++] & 0xFF) <<  8) |  (bytes[pos++] & 0xFF));
  }
 
  @Override
  public int readInt() {
    return ((bytes[pos++] & 0xFF) << 24) | ((bytes[pos++] & 0xFF) << 16)
      | ((bytes[pos++] & 0xFF) <<  8) |  (bytes[pos++] & 0xFF);
  }
 
  @Override
  public long readLong() {
    final int i1 = ((bytes[pos++] & 0xff) << 24) | ((bytes[pos++] & 0xff) << 16) |
      ((bytes[pos++] & 0xff) << 8) | (bytes[pos++] & 0xff);
    final int i2 = ((bytes[pos++] & 0xff) << 24) | ((bytes[pos++] & 0xff) << 16) |
      ((bytes[pos++] & 0xff) << 8) | (bytes[pos++] & 0xff);
    return (((long)i1) << 32) | (i2 & 0xFFFFFFFFL);
  }

  @Override
  public int readVInt() {
    byte b = bytes[pos++];
    if (b >= 0) return b;
    int i = b & 0x7F;
    b = bytes[pos++];
    i |= (b & 0x7F) << 7;
    if (b >= 0) return i;
    b = bytes[pos++];
    i |= (b & 0x7F) << 14;
    if (b >= 0) return i;
    b = bytes[pos++];
    i |= (b & 0x7F) << 21;
    if (b >= 0) return i;
    b = bytes[pos++];
    // Warning: the next ands use 0x0F / 0xF0 - beware copy/paste errors:
    i |= (b & 0x0F) << 28;
    if ((b & 0xF0) == 0) return i;
    throw new RuntimeException("Invalid vInt detected (too many bits)");
  }
 
  @Override
  public long readVLong() {
    byte b = bytes[pos++];
    if (b >= 0) return b;
    long i = b & 0x7FL;
    b = bytes[pos++];
    i |= (b & 0x7FL) << 7;
    if (b >= 0) return i;
    b = bytes[pos++];
    i |= (b & 0x7FL) << 14;
    if (b >= 0) return i;
    b = bytes[pos++];
    i |= (b & 0x7FL) << 21;
    if (b >= 0) return i;
    b = bytes[pos++];
    i |= (b & 0x7FL) << 28;
    if (b >= 0) return i;
    b = bytes[pos++];
    i |= (b & 0x7FL) << 35;
    if (b >= 0) return i;
    b = bytes[pos++];
    i |= (b & 0x7FL) << 42;
    if (b >= 0) return i;
    b = bytes[pos++];
    i |= (b & 0x7FL) << 49;
    if (b >= 0) return i;
    b = bytes[pos++];
    i |= (b & 0x7FL) << 56;
    if (b >= 0) return i;
    throw new RuntimeException("Invalid vLong detected (negative values disallowed)");
  }

  // NOTE: AIOOBE not EOF if you read too much
  @Override
  public byte readByte() {
    return bytes[pos++];
  }

  // NOTE: AIOOBE not EOF if you read too much
  @Override
  public void readBytes(byte[] b, int offset, int len) {
    System.arraycopy(bytes, pos, b, offset, len);
    pos += len;
  }

  @Override
  public void close() {
  }

  public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
    return new ByteArrayIndexInput(sliceDescription, this.bytes, (int) offset, (int) length);
  }
}
