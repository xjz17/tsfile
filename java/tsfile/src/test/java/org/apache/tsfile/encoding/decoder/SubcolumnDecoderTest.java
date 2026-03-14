package org.apache.tsfile.encoding.decoder;

import org.apache.tsfile.encoding.encoder.SubcolumnEncoder;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class SubcolumnDecoderTest {

  @Test
  public void testIntRoundTripRandomValues() throws IOException {
    Random random = new Random(0L);
    int[] data = new int[2049];
    for (int i = 0; i < data.length; i++) {
      data[i] = random.nextInt();
    }

    assertIntRoundTrip(data, 128);
  }

  @Test
  public void testIntRoundTripConstantAndDictionaryFriendlyValues() throws IOException {
    int[] data = new int[257];
    int[] pattern = {7, 7, 7, 11, 11, 11, 15, 15, 15, 3, 3, 3, 3, 3, 3, 3};
    for (int i = 0; i < data.length; i++) {
      data[i] = -1_000 + pattern[i % pattern.length];
    }
    Arrays.fill(data, 192, 225, 42);

    assertIntRoundTrip(data, 64);
  }

  @Test
  public void testIntRoundTripAllEqualValues() throws IOException {
    int[] data = new int[33];
    Arrays.fill(data, -31415926);

    assertIntRoundTrip(data, 32);
  }

  @Test
  public void testLongRoundTripRandomValues() throws IOException {
    Random random = new Random(1L);
    long[] data = new long[2049];
    for (int i = 0; i < data.length; i++) {
      data[i] = random.nextLong();
    }

    assertLongRoundTrip(data, 128);
  }

  @Test
  public void testLongRoundTripConstantAndDictionaryFriendlyValues() throws IOException {
    long[] data = new long[259];
    long base = 1L << 42;
    long[] pattern = {0L, 3L, 3L, 7L, 7L, 7L, 15L, 15L, 0L, 0L, 1L, 1L, 1L, 1L};
    for (int i = 0; i < data.length; i++) {
      data[i] = base + pattern[i % pattern.length];
    }
    Arrays.fill(data, 128, 200, base + 9L);

    assertLongRoundTrip(data, 64);
  }

  @Test
  public void testLongRoundTripAllEqualValues() throws IOException {
    long[] data = new long[35];
    Arrays.fill(data, Long.MIN_VALUE + 1024);

    assertLongRoundTrip(data, 32);
  }

  private void assertIntRoundTrip(int[] data, int blockSize) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    SubcolumnEncoder encoder = new SubcolumnEncoder.IntSubcolumnEncoder(blockSize);
    for (int value : data) {
      encoder.encode(value, out);
    }
    encoder.flush(out);

    ByteBuffer buffer = ByteBuffer.wrap(out.toByteArray());
    SubcolumnDecoder decoder = new SubcolumnDecoder.IntSubcolumnDecoder();
    int[] decoded = new int[data.length];
    int index = 0;
    while (decoder.hasNext(buffer)) {
      decoded[index++] = decoder.readInt(buffer);
    }

    assertEquals(data.length, index);
    assertArrayEquals(data, decoded);
  }

  private void assertLongRoundTrip(long[] data, int blockSize) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    SubcolumnEncoder encoder = new SubcolumnEncoder.LongSubcolumnEncoder(blockSize);
    for (long value : data) {
      encoder.encode(value, out);
    }
    encoder.flush(out);

    ByteBuffer buffer = ByteBuffer.wrap(out.toByteArray());
    SubcolumnDecoder decoder = new SubcolumnDecoder.LongSubcolumnDecoder();
    long[] decoded = new long[data.length];
    int index = 0;
    while (decoder.hasNext(buffer)) {
      decoded[index++] = decoder.readLong(buffer);
    }

    assertEquals(data.length, index);
    assertArrayEquals(data, decoded);
  }
}
