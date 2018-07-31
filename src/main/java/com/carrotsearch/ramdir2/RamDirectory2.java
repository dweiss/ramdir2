package com.carrotsearch.ramdir2;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.SingleInstanceLockFactory;

public class RamDirectory2 extends BaseDirectory {
  private final AtomicLong nextTempFileCounter = new AtomicLong();
  protected final Map<String, Entry> files = new ConcurrentHashMap<>();

  public RamDirectory2(LockFactory factory) {
    super(factory);
  }

  public RamDirectory2() {
    this(new SingleInstanceLockFactory());
  }

  @Override
  public String[] listAll() throws IOException {
    ensureOpen();
    return files.keySet().stream().sorted().toArray(String[]::new);
  }

  @Override
  public void deleteFile(String name) throws IOException {
    ensureOpen();

    Entry removed = files.remove(name);
    if (removed == null) {
      throw new FileNotFoundException(name);
    }
  }

  @Override
  public long fileLength(String name) throws IOException {
    ensureOpen();
    Entry file = files.get(name);
    if (file == null) {
      throw new FileNotFoundException(name);
    }
    return file.length();
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    Entry e = new Entry(name); 
    if (files.putIfAbsent(name, e) != null) {
      throw new FileAlreadyExistsException("File already exists: " + name);
    }
    return e.createOutput();
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
    while (true) {
      String name = IndexFileNames.segmentFileName(prefix, suffix + "_" + 
          Long.toString(nextTempFileCounter.getAndIncrement(), Character.MAX_RADIX), "tmp");
      Entry e = new Entry(name); 
      if (files.putIfAbsent(name, e) == null) {
        return e.createOutput();
      }
    }
  }

  @Override
  public void rename(String source, String dest) throws IOException {
    ensureOpen();

    Entry file = files.get(source);
    if (file == null) {
      throw new FileNotFoundException(source);
    }
    if (files.putIfAbsent(dest, file) != null) {
      throw new FileAlreadyExistsException(dest);
    }
    if (!files.remove(source, file)) {
      throw new IllegalStateException("File was unexpectedly replaced: " + source);
    }
    files.remove(source);
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
    // no-op.
  }

  @Override
  public void syncMetaData() throws IOException {
    // no-op.
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    ensureOpen();
    Entry e = files.get(name);
    if (e == null) {
      throw new NoSuchFileException(name);
    } else {
      return e.openInput();
    }
  }

  @Override
  public void close() throws IOException {
    isOpen = false;
    files.clear();
  }

  private final static class Entry {
    private final String name;
    // private volatile List<ByteBuffer> content;
    private volatile IndexInput content;
    private volatile long length;

    public Entry(String name) {
      this.name = name;
    }

    public long length() {
      return length;
    }

    public IndexInput openInput() throws IOException {
      IndexInput local = this.content;
      if (local == null) {
        throw new AccessDeniedException("Can't open a file still open for writing: " + name);
      }

      return local.clone();
    }

    final IndexOutput createOutput() throws IOException {
      if (content != null) {
        throw new IOException("Can only write once.");
      }

      RamDataOutput output = new RamDataOutput();
      return new DelegateIndexOutput(output, "RamDir2 output (file=" + name + ")", name) {
        @Override
        public void close() throws IOException {
          super.close();
          
          //byte[] array = ramDataOutput.toArray();
          //content = new ByteArrayIndexInput("foo", array, 0, array.length);
          content = new DelegateIndexInput(output.toDataInput(), "");
          length = output.size();
        }
      };
    }    
  }
}
