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

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

/** Lossless page payloads shared by the experimental BOS and Sub-column encodings. */
public final class ExperimentalLongCodec {

  public enum Kind {
    BOS,
    SUBCOLUMN
  }

  private static final int BOS_BLOCK_SIZE = 512;
  private static final int SUBCOLUMN_BLOCK_SIZE = 512;

  private ExperimentalLongCodec() {}

  public static byte[] encode(Kind kind, long[] values) {
    return kind == Kind.BOS ? encodeBos(values) : encodeSubcolumn(values);
  }

  public static long[] decode(Kind kind, byte[] payload) {
    return kind == Kind.BOS ? decodeBos(payload) : decodeSubcolumn(payload);
  }

  private static byte[] encodeBos(long[] values) {
    ByteArrayOutputStream out = new ByteArrayOutputStream(Math.max(32, values.length * 4));
    putInt(out, values.length);
    putInt(out, BOS_BLOCK_SIZE);
    for (int offset = 0; offset < values.length; offset += BOS_BLOCK_SIZE) {
      int length = Math.min(BOS_BLOCK_SIZE, values.length - offset);
      putInt(out, length);
      putLong(out, values[offset]);
      if (length == 1) {
        putLong(out, 0);
        out.write(0);
        putInt(out, 0);
        putInt(out, 0);
        putInt(out, 0);
        continue;
      }

      long[] deltas = new long[length - 1];
      long previous = values[offset];
      for (int i = 1; i < length; i++) {
        long current = values[offset + i];
        deltas[i - 1] = current - previous;
        previous = current;
      }
      BosPlan plan = chooseBosPlan(deltas);
      byte[] bitmap = new byte[(deltas.length + 7) >>> 3];
      BitWriter packedWriter = new BitWriter(deltas.length - plan.outlierCount, plan.width);
      for (int i = 0; i < deltas.length; i++) {
        long delta = deltas[i];
        if (delta < plan.lower || delta > plan.upper) {
          bitmap[i >>> 3] |= (byte) (1 << (i & 7));
        } else {
          packedWriter.write(delta - plan.lower, plan.width);
        }
      }
      byte[] packed = packedWriter.finish();

      // BOS pipeline: delta -> outlier separation -> FOR -> bit packing.
      putLong(out, plan.lower);
      out.write(plan.width);
      putInt(out, bitmap.length);
      putInt(out, packed.length);
      putInt(out, plan.outlierCount);
      putBytes(out, bitmap);
      putBytes(out, packed);
      for (long delta : deltas) {
        if (delta < plan.lower || delta > plan.upper) {
          putLong(out, delta);
        }
      }
    }
    return out.toByteArray();
  }

  private static long[] decodeBos(byte[] payload) {
    Reader reader = new Reader(payload);
    int total = reader.readNonNegativeInt("BOS row count");
    int blockSize = reader.readNonNegativeInt("BOS block size");
    if (blockSize == 0) {
      throw new IllegalArgumentException("BOS block size is zero");
    }
    long[] values = new long[total];
    int written = 0;
    while (written < total) {
      int length = reader.readNonNegativeInt("BOS block length");
      if (length == 0 || length > blockSize || written + length > total) {
        throw new IllegalArgumentException("invalid BOS block length");
      }
      values[written] = reader.readLong();
      long lower = reader.readLong();
      int width = reader.readUnsignedByte();
      int bitmapLength = reader.readNonNegativeInt("BOS bitmap length");
      int packedLength = reader.readNonNegativeInt("BOS packed length");
      int outlierCount = reader.readNonNegativeInt("BOS outlier count");
      if (width > Long.SIZE || bitmapLength != ((length - 1 + 7) >>> 3)) {
        throw new IllegalArgumentException("invalid BOS block header");
      }
      int inlierCount = length - 1 - outlierCount;
      if (inlierCount < 0) {
        throw new IllegalArgumentException("invalid BOS outlier count");
      }
      long expectedPackedLength = (((long) inlierCount * width) + 7) >>> 3;
      if (expectedPackedLength != packedLength) {
        throw new IllegalArgumentException("invalid BOS packed length");
      }
      int bitmapOffset = reader.position;
      reader.skip(bitmapLength);
      int actualOutliers = 0;
      for (int i = 0; i < bitmapLength; i++) {
        actualOutliers += Integer.bitCount(payload[bitmapOffset + i] & 0xff);
      }
      if (actualOutliers != outlierCount) {
        throw new IllegalArgumentException("invalid BOS outlier bitmap");
      }
      int packedOffset = reader.position;
      reader.skip(packedLength);
      BitReader packed = new BitReader(payload, packedOffset, packedLength);
      long previous = values[written];
      for (int i = 0; i < length - 1; i++) {
        boolean outlier = ((payload[bitmapOffset + (i >>> 3)] >>> (i & 7)) & 1) != 0;
        long delta = outlier ? reader.readLong() : lower + packed.read(width);
        previous += delta;
        values[written + i + 1] = previous;
      }
      written += length;
    }
    reader.requireEnd();
    return values;
  }

  private static BosPlan chooseBosPlan(long[] deltas) {
    long[] sorted = Arrays.copyOf(deltas, deltas.length);
    Arrays.sort(sorted);
    int[] trims = {
      0, deltas.length / 100, deltas.length / 50, deltas.length / 20, deltas.length / 10
    };
    BosPlan best = null;
    int previousTrim = -1;
    for (int trim : trims) {
      if (trim == previousTrim || trim * 2 >= sorted.length) {
        continue;
      }
      previousTrim = trim;
      long lower = sorted[trim];
      long upper = sorted[sorted.length - 1 - trim];
      long range = upper - lower;
      int width = unsignedBitWidth(range);
      int outliers = lowerBound(sorted, lower) + sorted.length - upperBound(sorted, upper);
      long inliers = (long) deltas.length - outliers;
      long cost =
          ((deltas.length + 7L) >>> 3)
              + ((inliers * width + 7L) >>> 3)
              + outliers * (long) Long.BYTES;
      BosPlan candidate = new BosPlan(lower, upper, width, outliers, cost);
      if (best == null
          || candidate.cost < best.cost
          || (candidate.cost == best.cost && candidate.outlierCount < best.outlierCount)) {
        best = candidate;
      }
    }
    if (best == null) {
      throw new IllegalArgumentException("cannot choose BOS plan");
    }
    return best;
  }

  private static int lowerBound(long[] values, long target) {
    int low = 0;
    int high = values.length;
    while (low < high) {
      int middle = (low + high) >>> 1;
      if (values[middle] < target) {
        low = middle + 1;
      } else {
        high = middle;
      }
    }
    return low;
  }

  private static int upperBound(long[] values, long target) {
    int low = 0;
    int high = values.length;
    while (low < high) {
      int middle = (low + high) >>> 1;
      if (values[middle] <= target) {
        low = middle + 1;
      } else {
        high = middle;
      }
    }
    return low;
  }

  private static byte[] encodeSubcolumn(long[] values) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    putInt(out, values.length);
    putInt(out, SUBCOLUMN_BLOCK_SIZE);
    for (int offset = 0; offset < values.length; offset += SUBCOLUMN_BLOCK_SIZE) {
      int length = Math.min(SUBCOLUMN_BLOCK_SIZE, values.length - offset);
      long minimum = values[offset];
      for (int i = 1; i < length; i++) {
        minimum = Math.min(minimum, values[offset + i]);
      }
      long[] residuals = new long[length];
      long maximumResidual = 0;
      for (int i = 0; i < length; i++) {
        residuals[i] = values[offset + i] - minimum;
        if (Long.compareUnsigned(residuals[i], maximumResidual) > 0) {
          maximumResidual = residuals[i];
        }
      }
      int maximumBits = unsignedBitWidth(maximumResidual);
      int beta = maximumBits <= 1 ? 1 : 4;
      int groups = maximumBits == 0 ? 0 : (maximumBits + beta - 1) / beta;
      putLong(out, minimum);
      putInt(out, length);
      out.write(maximumBits);
      out.write(beta);
      long mask = (1L << beta) - 1;
      for (int group = 0; group < groups; group++) {
        long[] subcolumn = new long[length];
        int shift = group * beta;
        for (int i = 0; i < length; i++) {
          subcolumn[i] = (residuals[i] >>> shift) & mask;
        }
        byte[] encoded = encodeSubcolumnGroup(subcolumn, beta);
        putInt(out, encoded.length);
        putBytes(out, encoded);
      }
    }
    return out.toByteArray();
  }

  private static long[] decodeSubcolumn(byte[] payload) {
    Reader reader = new Reader(payload);
    int total = reader.readNonNegativeInt("Sub-column row count");
    int blockSize = reader.readNonNegativeInt("Sub-column block size");
    if (blockSize == 0) {
      throw new IllegalArgumentException("Sub-column block size is zero");
    }
    long[] values = new long[total];
    int written = 0;
    while (written < total) {
      long minimum = reader.readLong();
      int length = reader.readNonNegativeInt("Sub-column block length");
      int maximumBits = reader.readUnsignedByte();
      int beta = reader.readUnsignedByte();
      if (length == 0
          || length > blockSize
          || written + length > total
          || beta == 0
          || beta > 8
          || maximumBits > Long.SIZE) {
        throw new IllegalArgumentException("invalid Sub-column block header");
      }
      int groups = maximumBits == 0 ? 0 : (maximumBits + beta - 1) / beta;
      long[] residuals = new long[length];
      for (int group = 0; group < groups; group++) {
        int encodedLength = reader.readNonNegativeInt("Sub-column group length");
        long[] subcolumn = decodeSubcolumnGroup(reader.readBytes(encodedLength), length);
        int shift = group * beta;
        for (int i = 0; i < length; i++) {
          residuals[i] |= subcolumn[i] << shift;
        }
      }
      for (int i = 0; i < length; i++) {
        values[written++] = minimum + residuals[i];
      }
    }
    reader.requireEnd();
    return values;
  }

  private static byte[] encodeSubcolumnGroup(long[] values, int beta) {
    int width = 0;
    for (long value : values) {
      width = Math.max(width, unsignedBitWidth(value));
    }
    ByteArrayOutputStream bitPacked = new ByteArrayOutputStream();
    bitPacked.write(0);
    bitPacked.write(width);
    byte[] bitBody = pack(values, width);
    putInt(bitPacked, bitBody.length);
    putBytes(bitPacked, bitBody);
    byte[] best = bitPacked.toByteArray();

    long[] runValues = new long[values.length];
    long[] runLengths = new long[values.length];
    int runCount = 0;
    if (values.length > 0) {
      long current = values[0];
      int start = 0;
      for (int i = 1; i < values.length; i++) {
        if (values[i] != current) {
          runValues[runCount] = current;
          runLengths[runCount++] = i - start;
          current = values[i];
          start = i;
        }
      }
      runValues[runCount] = current;
      runLengths[runCount++] = values.length - start;
    }
    int runLengthWidth = 0;
    for (int i = 0; i < runCount; i++) {
      runLengthWidth = Math.max(runLengthWidth, unsignedBitWidth(runLengths[i]));
    }
    int runValueWidth = Math.max(width, 1);
    byte[] lengthBody = pack(Arrays.copyOf(runLengths, runCount), runLengthWidth);
    byte[] valueBody = pack(Arrays.copyOf(runValues, runCount), runValueWidth);
    ByteArrayOutputStream rle = new ByteArrayOutputStream();
    rle.write(1);
    rle.write(runLengthWidth);
    rle.write(runValueWidth);
    putInt(rle, runCount);
    putInt(rle, lengthBody.length);
    putInt(rle, valueBody.length);
    putBytes(rle, lengthBody);
    putBytes(rle, valueBody);
    if (rle.size() < best.length) {
      best = rle.toByteArray();
    }

    long[] keys = Arrays.copyOf(values, values.length);
    Arrays.sort(keys);
    int keyCount = 0;
    for (int i = 0; i < keys.length; i++) {
      if (i == 0 || keys[i] != keys[i - 1]) {
        keys[keyCount++] = keys[i];
      }
    }
    if (keyCount > 0 && keyCount <= Math.min(256, 1 << beta)) {
      long[] codes = new long[values.length];
      for (int i = 0; i < values.length; i++) {
        codes[i] = Arrays.binarySearch(keys, 0, keyCount, values[i]);
      }
      int keyWidth = Math.max(width, 1);
      int codeWidth = unsignedBitWidth(keyCount - 1L);
      byte[] keyBody = pack(Arrays.copyOf(keys, keyCount), keyWidth);
      byte[] codeBody = pack(codes, codeWidth);
      ByteArrayOutputStream dictionary = new ByteArrayOutputStream();
      dictionary.write(2);
      dictionary.write(keyWidth);
      dictionary.write(codeWidth);
      putInt(dictionary, keyCount);
      putInt(dictionary, keyBody.length);
      putInt(dictionary, codeBody.length);
      putBytes(dictionary, keyBody);
      putBytes(dictionary, codeBody);
      if (dictionary.size() < best.length) {
        best = dictionary.toByteArray();
      }
    }
    return best;
  }

  private static long[] decodeSubcolumnGroup(byte[] payload, int count) {
    Reader reader = new Reader(payload);
    int type = reader.readUnsignedByte();
    if (type == 0) {
      int width = reader.readUnsignedByte();
      int length = reader.readNonNegativeInt("Sub-column bitpack length");
      long[] values = unpack(reader.readBytes(length), count, width);
      reader.requireEnd();
      return values;
    }
    if (type == 1) {
      int lengthWidth = reader.readUnsignedByte();
      int valueWidth = reader.readUnsignedByte();
      int runs = reader.readNonNegativeInt("Sub-column run count");
      int lengthBytes = reader.readNonNegativeInt("Sub-column run-length bytes");
      int valueBytes = reader.readNonNegativeInt("Sub-column run-value bytes");
      long[] lengths = unpack(reader.readBytes(lengthBytes), runs, lengthWidth);
      long[] runValues = unpack(reader.readBytes(valueBytes), runs, valueWidth);
      long[] values = new long[count];
      int position = 0;
      for (int i = 0; i < runs; i++) {
        if (lengths[i] > count - position) {
          throw new IllegalArgumentException("invalid Sub-column run length");
        }
        Arrays.fill(values, position, position + (int) lengths[i], runValues[i]);
        position += (int) lengths[i];
      }
      if (position != count) {
        throw new IllegalArgumentException("truncated Sub-column runs");
      }
      reader.requireEnd();
      return values;
    }
    if (type != 2) {
      throw new IllegalArgumentException("unknown Sub-column group type");
    }
    int keyWidth = reader.readUnsignedByte();
    int codeWidth = reader.readUnsignedByte();
    int keyCount = reader.readNonNegativeInt("Sub-column key count");
    int keyBytes = reader.readNonNegativeInt("Sub-column key bytes");
    int codeBytes = reader.readNonNegativeInt("Sub-column code bytes");
    long[] keys = unpack(reader.readBytes(keyBytes), keyCount, keyWidth);
    long[] codes = unpack(reader.readBytes(codeBytes), count, codeWidth);
    long[] values = new long[count];
    for (int i = 0; i < count; i++) {
      if (codes[i] < 0 || codes[i] >= keyCount) {
        throw new IllegalArgumentException("invalid Sub-column dictionary code");
      }
      values[i] = keys[(int) codes[i]];
    }
    reader.requireEnd();
    return values;
  }

  private static byte[] pack(long[] values, int width) {
    if (values.length == 0 || width == 0) {
      return new byte[0];
    }
    if (width < 0 || width > Long.SIZE) {
      throw new IllegalArgumentException("invalid bit width");
    }
    BitWriter writer = new BitWriter(values.length, width);
    for (long value : values) {
      writer.write(value, width);
    }
    return writer.finish();
  }

  private static long[] unpack(byte[] payload, int count, int width) {
    if (count < 0 || width < 0 || width > Long.SIZE) {
      throw new IllegalArgumentException("invalid packed payload header");
    }
    long expectedBits = (long) count * width;
    if (((expectedBits + 7) >>> 3) != payload.length) {
      throw new IllegalArgumentException("invalid packed payload length");
    }
    long[] values = new long[count];
    if (width == 0) {
      return values;
    }
    BitReader reader = new BitReader(payload);
    for (int i = 0; i < count; i++) {
      values[i] = reader.read(width);
    }
    return values;
  }

  private static int unsignedBitWidth(long value) {
    return value == 0 ? 0 : Long.SIZE - Long.numberOfLeadingZeros(value);
  }

  private static void putBytes(ByteArrayOutputStream out, byte[] bytes) {
    out.write(bytes, 0, bytes.length);
  }

  private static void putInt(ByteArrayOutputStream out, int value) {
    out.write((value >>> 24) & 0xff);
    out.write((value >>> 16) & 0xff);
    out.write((value >>> 8) & 0xff);
    out.write(value & 0xff);
  }

  private static void putLong(ByteArrayOutputStream out, long value) {
    for (int shift = 56; shift >= 0; shift -= 8) {
      out.write((int) ((value >>> shift) & 0xff));
    }
  }

  private static final class BosPlan {
    private final long lower;
    private final long upper;
    private final int width;
    private final int outlierCount;
    private final long cost;

    private BosPlan(long lower, long upper, int width, int outlierCount, long cost) {
      this.lower = lower;
      this.upper = upper;
      this.width = width;
      this.outlierCount = outlierCount;
      this.cost = cost;
    }
  }

  private static final class BitWriter {
    private final byte[] data;
    private int bitPosition;

    private BitWriter(int count, int width) {
      long bytes = (((long) count * width) + 7) >>> 3;
      if (bytes > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("packed payload is too large");
      }
      data = new byte[(int) bytes];
    }

    private void write(long value, int width) {
      int remaining = width;
      while (remaining > 0) {
        int bitOffset = bitPosition & 7;
        int take = Math.min(Byte.SIZE - bitOffset, remaining);
        int sourceShift = remaining - take;
        int mask = (1 << take) - 1;
        int chunk = (int) ((value >>> sourceShift) & mask);
        data[bitPosition >>> 3] |= (byte) (chunk << (Byte.SIZE - bitOffset - take));
        bitPosition += take;
        remaining -= take;
        if (bitOffset == 0 && remaining >= Byte.SIZE) {
          int bytes = remaining >>> 3;
          for (int i = 0; i < bytes; i++) {
            int shift = remaining - Byte.SIZE;
            data[bitPosition >>> 3] = (byte) (value >>> shift);
            bitPosition += Byte.SIZE;
            remaining -= Byte.SIZE;
          }
        }
      }
    }

    private byte[] finish() {
      return data;
    }
  }

  private static final class BitReader {
    private final byte[] data;
    private final int byteOffset;
    private final int byteLength;
    private int bitPosition;

    private BitReader(byte[] data) {
      this(data, 0, data.length);
    }

    private BitReader(byte[] data, int byteOffset, int byteLength) {
      this.data = data;
      this.byteOffset = byteOffset;
      this.byteLength = byteLength;
    }

    private long read(int width) {
      long value = 0;
      int remaining = width;
      while (remaining > 0) {
        if ((bitPosition >>> 3) >= byteLength) {
          throw new IllegalArgumentException("truncated bit-packed payload");
        }
        int current = data[byteOffset + (bitPosition >>> 3)] & 0xff;
        int bitOffset = bitPosition & 7;
        int take = Math.min(Byte.SIZE - bitOffset, remaining);
        int shift = Byte.SIZE - bitOffset - take;
        int mask = (1 << take) - 1;
        value = (value << take) | ((current >>> shift) & mask);
        bitPosition += take;
        remaining -= take;
      }
      return value;
    }
  }

  private static final class Reader {
    private final byte[] data;
    private int position;

    private Reader(byte[] data) {
      this.data = data;
    }

    private int readUnsignedByte() {
      require(1);
      return data[position++] & 0xff;
    }

    private int readInt() {
      require(Integer.BYTES);
      int value =
          ((data[position] & 0xff) << 24)
              | ((data[position + 1] & 0xff) << 16)
              | ((data[position + 2] & 0xff) << 8)
              | (data[position + 3] & 0xff);
      position += Integer.BYTES;
      return value;
    }

    private int readNonNegativeInt(String name) {
      int value = readInt();
      if (value < 0) {
        throw new IllegalArgumentException(name + " is negative");
      }
      return value;
    }

    private long readLong() {
      require(Long.BYTES);
      long value = 0;
      for (int i = 0; i < Long.BYTES; i++) {
        value = (value << 8) | (data[position + i] & 0xffL);
      }
      position += Long.BYTES;
      return value;
    }

    private byte[] readBytes(int length) {
      require(length);
      byte[] result = Arrays.copyOfRange(data, position, position + length);
      position += length;
      return result;
    }

    private void skip(int length) {
      require(length);
      position += length;
    }

    private void require(int length) {
      if (length < 0 || position > data.length - length) {
        throw new IllegalArgumentException("truncated experimental codec payload");
      }
    }

    private void requireEnd() {
      if (position != data.length) {
        throw new IllegalArgumentException("trailing experimental codec bytes");
      }
    }
  }
}
