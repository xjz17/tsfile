package org.apache.tsfile.encoding.decoder;

import org.apache.tsfile.encoding.encoder.SubcolumnEncoder;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class SubcolumnDecoderTest {

  @Test
  public void test0() throws IOException {

    ByteArrayOutputStream out = new ByteArrayOutputStream();

    SubcolumnEncoder encoder = new SubcolumnEncoder.IntSubcolumnEncoder();

    int[] data = new int[2000];
    for (int i = 0; i < 2000; i++) {
      data[i] = i * i;
    }

    for (int i = 0; i < 2000; i++) {
      encoder.encode(data[i], out);
    }
    encoder.flush(out);

    byte[] page = out.toByteArray();

    ByteBuffer buffer = ByteBuffer.wrap(page);

    SubcolumnDecoder decoder = new SubcolumnDecoder.IntSubcolumnDecoder();
    int i = 0;
    while (decoder.hasNext(buffer)) {
      int temp = decoder.readInt(buffer);
      // System.out.println(temp);
      assertEquals(data[i++], temp);
    }
  }
}
