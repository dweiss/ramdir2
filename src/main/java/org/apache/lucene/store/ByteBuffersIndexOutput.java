package org.apache.lucene.store;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.zip.CRC32;

public final class ByteBuffersIndexOutput extends IndexOutput {
  private ByteBuffersDataOutput delegate;
  private BufferedChecksum crc;
  private Consumer<ByteBuffersDataOutput> onClose;

  public ByteBuffersIndexOutput(ByteBuffersDataOutput delegate, String resourceDescription, String name, Consumer<ByteBuffersDataOutput> onClose) {
    super(resourceDescription, name);
    this.delegate = delegate;
    this.crc = new BufferedChecksum(new CRC32());
    this.onClose = onClose;
  }

  @Override
  public void close() throws IOException {
    try {
      onClose.accept(delegate);
    } finally {
      delegate = null;
    }
  }

  @Override
  public long getFilePointer() {
    ensureOpen();
    return delegate.size();
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
    ensureOpen();
    delegate.writeBytes(b, length);
  }

  @Override
  public void writeInt(int i) throws IOException {
    ensureOpen();
    delegate.writeInt(i);
  }

  @Override
  public void writeShort(short i) throws IOException {
    ensureOpen();
    delegate.writeShort(i);
  }

  @Override
  public void writeLong(long i) throws IOException {
    ensureOpen();
    delegate.writeLong(i);
  }

  @Override
  public void writeString(String s) throws IOException {
    ensureOpen();
    delegate.writeString(s);
  }

  @Override
  public void copyBytes(DataInput input, long numBytes) throws IOException {
    ensureOpen();
    delegate.copyBytes(input, numBytes);
  }

  @Override
  public void writeMapOfStrings(Map<String, String> map) throws IOException {
    ensureOpen();
    delegate.writeMapOfStrings(map);
  }

  @Override
  public void writeSetOfStrings(Set<String> set) throws IOException {
    ensureOpen();
    delegate.writeSetOfStrings(set);
  }
  
  private void ensureOpen() {
    if (delegate == null) {
      throw new AlreadyClosedException("Already closed.");
    }
  }
  
}
