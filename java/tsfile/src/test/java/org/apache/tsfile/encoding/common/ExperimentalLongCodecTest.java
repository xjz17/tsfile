/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tsfile.encoding.common;

import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.encoding.encoder.Encoder;
import org.apache.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.read.reader.chunk.ChunkReader;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.HexFormat;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

public class ExperimentalLongCodecTest {

  @Test
  public void testLongPayloadRoundTrips() {
    long[][] cases = {
      {},
      {42},
      new long[1025],
      {Long.MIN_VALUE, Long.MAX_VALUE, 0, -1, 1, Long.MIN_VALUE},
      monotonicValues(4097),
      randomValues(10000)
    };
    Arrays.fill(cases[2], 17);
    for (ExperimentalLongCodec.Kind kind : ExperimentalLongCodec.Kind.values()) {
      for (long[] values : cases) {
        assertArrayEquals(
            values, ExperimentalLongCodec.decode(kind, ExperimentalLongCodec.encode(kind, values)));
      }
    }
  }

  @Test
  public void testTruncatedPayloadIsRejected() {
    for (ExperimentalLongCodec.Kind kind : ExperimentalLongCodec.Kind.values()) {
      byte[] payload = ExperimentalLongCodec.encode(kind, monotonicValues(1000));
      byte[] truncated = Arrays.copyOf(payload, payload.length - 1);
      assertThrows(
          IllegalArgumentException.class, () -> ExperimentalLongCodec.decode(kind, truncated));
    }
  }

  @Test
  public void testBosOptimizedPathKeepsWireFormat() throws Exception {
    long[] values = new long[1025];
    long state = 0x123456789abcdef0L;
    for (int i = 0; i < values.length; i++) {
      state ^= state << 13;
      state ^= state >>> 7;
      state ^= state << 17;
      values[i] = state;
    }

    byte[] payload = ExperimentalLongCodec.encode(ExperimentalLongCodec.Kind.BOS, values);
    assertEquals(8411, payload.length);
    assertEquals(
        "8f2a0e75bc2865173251dd4aac406883cfd1894fa75e853b5f0b4db8df27ddb5",
        HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(payload)));
    assertArrayEquals(
        values, ExperimentalLongCodec.decode(ExperimentalLongCodec.Kind.BOS, payload));
  }

  @Test
  public void testTsFileEncoderDecoderFactoriesAreBitExact() throws Exception {
    for (TSEncoding encoding : new TSEncoding[] {TSEncoding.BOS, TSEncoding.SUBCOLUMN}) {
      assertEquals(encoding, TSEncoding.deserialize(encoding.serialize()));
      roundTripInt(encoding, new int[] {Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE});
      roundTripLong(
          encoding, new long[] {Long.MIN_VALUE, -1, 0, 1, Long.MAX_VALUE, Long.MIN_VALUE});
      roundTripFloat(
          encoding,
          new float[] {
            -0.0f, 0.0f, 1.25f, Float.NaN, Float.intBitsToFloat(0x7fc00001), Float.POSITIVE_INFINITY
          });
      roundTripDouble(
          encoding,
          new double[] {
            -0.0,
            0.0,
            1.25,
            Double.NaN,
            Double.longBitsToDouble(0x7ff8000000000001L),
            Double.NEGATIVE_INFINITY
          });
    }
  }

  @Test
  public void testPhysicalTsFileRoundTrip() throws Exception {
    IDeviceID deviceID = new StringArrayDeviceID("root.experimental.d1");
    for (TSEncoding encoding : new TSEncoding[] {TSEncoding.BOS, TSEncoding.SUBCOLUMN}) {
      File file =
          Files.createTempFile("tsfile-" + encoding.name().toLowerCase(), ".tsfile").toFile();
      long[] expected = monotonicValues(2049);
      MeasurementSchema schema =
          new MeasurementSchema("s1", TSDataType.INT64, encoding, CompressionType.UNCOMPRESSED);
      try {
        try (TsFileWriter writer = new TsFileWriter(file)) {
          writer.registerTimeseries(deviceID, schema);
          Tablet tablet =
              new Tablet(deviceID.toString(), Collections.singletonList(schema), expected.length);
          for (int i = 0; i < expected.length; i++) {
            tablet.addTimestamp(i, i);
            tablet.addValue(i, 0, expected[i]);
          }
          writer.writeTree(tablet);
        }

        int row = 0;
        try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getAbsolutePath())) {
          TimeseriesMetadata metadata = reader.getDeviceTimeseriesMetadata(deviceID).get(0);
          for (IChunkMetadata chunkMetadata : metadata.getChunkMetadataList()) {
            Chunk chunk = reader.readMemChunk((ChunkMetadata) chunkMetadata);
            assertEquals(encoding, chunk.getHeader().getEncodingType());
            ChunkReader chunkReader = new ChunkReader(chunk);
            while (chunkReader.hasNextSatisfiedPage()) {
              BatchData batchData = chunkReader.nextPageData();
              IPointReader pointReader = batchData.getBatchDataIterator();
              while (pointReader.hasNextTimeValuePair()) {
                TimeValuePair pair = pointReader.nextTimeValuePair();
                assertEquals(row, pair.getTimestamp());
                assertEquals(expected[row], ((Long) pair.getValues()[0]).longValue());
                row++;
              }
            }
          }
        }
        assertEquals(expected.length, row);
      } finally {
        Files.deleteIfExists(file.toPath());
      }
    }
  }

  private static void roundTripInt(TSEncoding encoding, int[] expected) throws Exception {
    Encoder encoder = TSEncodingBuilder.getEncodingBuilder(encoding).getEncoder(TSDataType.INT32);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (int value : expected) {
      encoder.encode(value, out);
    }
    encoder.flush(out);
    Decoder decoder = Decoder.getDecoderByType(encoding, TSDataType.INT32);
    ByteBuffer buffer = ByteBuffer.wrap(out.toByteArray());
    for (int value : expected) {
      assertEquals(value, decoder.readInt(buffer));
    }
    assertFalse(decoder.hasNext(buffer));
  }

  private static void roundTripLong(TSEncoding encoding, long[] expected) throws Exception {
    Encoder encoder = TSEncodingBuilder.getEncodingBuilder(encoding).getEncoder(TSDataType.INT64);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (long value : expected) {
      encoder.encode(value, out);
    }
    encoder.flush(out);
    Decoder decoder = Decoder.getDecoderByType(encoding, TSDataType.INT64);
    ByteBuffer buffer = ByteBuffer.wrap(out.toByteArray());
    for (long value : expected) {
      assertEquals(value, decoder.readLong(buffer));
    }
    assertFalse(decoder.hasNext(buffer));
  }

  private static void roundTripFloat(TSEncoding encoding, float[] expected) throws Exception {
    Encoder encoder = TSEncodingBuilder.getEncodingBuilder(encoding).getEncoder(TSDataType.FLOAT);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (float value : expected) {
      encoder.encode(value, out);
    }
    encoder.flush(out);
    Decoder decoder = Decoder.getDecoderByType(encoding, TSDataType.FLOAT);
    ByteBuffer buffer = ByteBuffer.wrap(out.toByteArray());
    for (float value : expected) {
      assertEquals(
          Float.floatToRawIntBits(value), Float.floatToRawIntBits(decoder.readFloat(buffer)));
    }
    assertFalse(decoder.hasNext(buffer));
  }

  private static void roundTripDouble(TSEncoding encoding, double[] expected) throws Exception {
    Encoder encoder = TSEncodingBuilder.getEncodingBuilder(encoding).getEncoder(TSDataType.DOUBLE);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (double value : expected) {
      encoder.encode(value, out);
    }
    encoder.flush(out);
    Decoder decoder = Decoder.getDecoderByType(encoding, TSDataType.DOUBLE);
    ByteBuffer buffer = ByteBuffer.wrap(out.toByteArray());
    for (double value : expected) {
      assertEquals(
          Double.doubleToRawLongBits(value),
          Double.doubleToRawLongBits(decoder.readDouble(buffer)));
    }
    assertFalse(decoder.hasNext(buffer));
  }

  private static long[] monotonicValues(int size) {
    long[] values = new long[size];
    for (int i = 1; i < values.length; i++) {
      values[i] = values[i - 1] + ((i % 101 == 0) ? 1000000 : i % 5);
    }
    return values;
  }

  private static long[] randomValues(int size) {
    long[] values = new long[size];
    Random random = new Random(20260825L);
    for (int i = 0; i < values.length; i++) {
      values[i] = random.nextLong();
    }
    return values;
  }
}
