package org.apache.lucene.store;

import static org.junit.Assert.*;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.Xoroshiro128PlusRandom;
import com.carrotsearch.randomizedtesting.annotations.Repeat;

public final class ByteBuffersDataInputTest extends RandomizedTest {
  @Test
  public void testSanity() throws IOException {
    ByteBuffersDataOutput out = new ByteBuffersDataOutput();
    ByteBuffersDataInput o1 = out.toDataInput();
    assertEquals(0, o1.size());
    LuceneTestCase.expectThrows(EOFException.class, () -> {
        o1.readByte();
    });

    out.writeByte((byte) 1);

    ByteBuffersDataInput o2 = out.toDataInput();
    assertEquals(1, o2.size());
    assertEquals(0, o2.position());
    assertEquals(0, o1.size());

    assertNotEquals(0, o2.ramBytesUsed());
    assertEquals(1, o2.readByte());
    assertEquals(1, o2.position());
    assertEquals(1, o2.readByte(0));

    LuceneTestCase.expectThrows(EOFException.class, () -> {
        o2.readByte();
    });

    assertEquals(1, o2.position());
  }

  @Test
  public void testRandomReads() throws Exception {
    ByteBuffersDataOutput dst = new ByteBuffersDataOutput();

    long seed = randomLong();
    int max = 1_000_000;
    List<ThrowingConsumer<DataInput>> reply = 
        ByteBuffersDataOutputTest.addRandomData(dst, new Xoroshiro128PlusRandom(seed), max);

    ByteBuffersDataInput src = dst.toDataInput();
    System.out.println(dst);
    System.out.println(src);
    for (ThrowingConsumer<DataInput> c : reply) {
      c.accept(src);
    }

    LuceneTestCase.expectThrows(EOFException.class, () -> {
      src.readByte();
    });
  }

  @Test
  @Repeat(iterations = 100)
  public void testRandomReadsOnSlices() throws Exception {
    ByteBuffersDataOutput dst = new ByteBuffersDataOutput();

    byte [] prefix = new byte [randomIntBetween(0, 1024 * 8)];
    dst.writeBytes(prefix);

    long seed = randomLong();
    int max = 10_000;
    List<ThrowingConsumer<DataInput>> reply = 
        ByteBuffersDataOutputTest.addRandomData(dst, new Xoroshiro128PlusRandom(seed), max);

    byte [] suffix = new byte [randomIntBetween(0, 1024 * 8)];
    dst.writeBytes(suffix);
    
    ByteBuffersDataInput src = dst.toDataInput().slice(prefix.length, dst.size() - prefix.length - suffix.length);

    assertEquals(0, src.position());
    assertEquals(dst.size() - prefix.length - suffix.length, src.size());
    System.out.println(dst);
    System.out.println(src);
    for (ThrowingConsumer<DataInput> c : reply) {
      c.accept(src);
    }

    LuceneTestCase.expectThrows(EOFException.class, () -> {
      src.readByte();
    });
  }

  @Test
  public void testSeekEmpty() throws Exception {
    ByteBuffersDataOutput dst = new ByteBuffersDataOutput();
    ByteBuffersDataInput in = dst.toDataInput();
    in.seek(0);

    LuceneTestCase.expectThrows(EOFException.class, () -> {
      in.seek(1);
    });

    in.seek(0);
    LuceneTestCase.expectThrows(EOFException.class, () -> {
      in.readByte();
    });
  }

  @Test
  @Repeat(iterations = 50)
  public void testSeek() throws Exception {
    ByteBuffersDataOutput dst = new ByteBuffersDataOutput();

    byte [] prefix = {};
    if (randomBoolean()) {
      prefix = new byte [randomIntBetween(1, 1024 * 8)];
      dst.writeBytes(prefix);
    }

    long seed = randomLong();
    int max = 1000;
    List<ThrowingConsumer<DataInput>> reply = 
        ByteBuffersDataOutputTest.addRandomData(dst, new Xoroshiro128PlusRandom(seed), max);

    ByteBuffersDataInput in = dst.toDataInput().slice(prefix.length, dst.size() - prefix.length);

    in.seek(0);
    for (ThrowingConsumer<DataInput> c : reply) {
      c.accept(in);
    }

    in.seek(0);
    for (ThrowingConsumer<DataInput> c : reply) {
      c.accept(in);
    }

    byte [] array = dst.toArray();
    array = Arrays.copyOfRange(array, prefix.length, array.length);
    for (int i = 0; i < 1000; i++) {
      int offs = randomIntBetween(0, array.length - 1);
      in.seek(offs);
      assertEquals(offs, in.position());
      assertEquals(array[offs], in.readByte());
    }
    in.seek(in.size());
    assertEquals(in.size(), in.position());
    LuceneTestCase.expectThrows(EOFException.class, () -> {
      in.readByte();
    });
  }

  @Test
  public void testSlicingWindow() throws Exception {
    ByteBuffersDataOutput dst = new ByteBuffersDataOutput();
    assertEquals(0, dst.toDataInput().slice(0, 0).size());;

    dst.writeBytes(randomBytesOfLength(1024 * 8));
    ByteBuffersDataInput in = dst.toDataInput();
    for (int offset = 0, max = (int) dst.size(); offset < max; offset++) {
      assertEquals(0, in.slice(offset, 0).size());
      assertEquals(1, in.slice(offset, 1).size());
      
      int window = Math.min(max - offset, 1024);
      assertEquals(window, in.slice(offset, window).size());
    }
    assertEquals(0, in.slice((int) dst.size(), 0).size());
  }
}