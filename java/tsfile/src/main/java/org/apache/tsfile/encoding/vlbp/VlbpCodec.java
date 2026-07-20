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

package org.apache.tsfile.encoding.vlbp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Shared value-level bit-packing codec for VLBP-DP and VLBP-RL encodings. */
public final class VlbpCodec {

  public enum Strategy {
    DP,
    RL
  }

  public enum Transform {
    NONE,
    ZIGZAG,
    SPRINTZ,
    TS_2DIFF
  }

  private static final int GROUP_SIZE_BITS = 32;
  private static final int BIT_WIDTH_BITS = 6;
  private static final int[] RL_LENGTH_CANDIDATES = {1, 2, 4, 8, 16, 32, 64, 128, 256};

  private VlbpCodec() {}

  public static byte[] encode(long[] values, Strategy strategy) {
    return encode(values, strategy, Transform.NONE);
  }

  public static byte[] encode(long[] values, Strategy strategy, Transform transform) {
    if (values.length == 0) {
      return new byte[0];
    }
    TransformedValues transformed = transform(values, transform);
    long[] encodedValues = transformed.values;
    int[] bitWidths = bitWidths(encodedValues);
    PackingPlan plan =
        strategy == Strategy.DP ? computeDpPlan(bitWidths) : computeRlPlan(bitWidths);
    return writePlan(encodedValues, plan, transformed);
  }

  public static long[] decode(byte[] data, int valueCount) {
    return decode(data, valueCount, Transform.NONE);
  }

  public static long[] decode(byte[] data, int valueCount, Transform transform) {
    if (data.length == 0 || valueCount == 0) {
      return new long[0];
    }
    BitReader reader = new BitReader(data);
    long firstValue = 0;
    long minDiff = 0;
    if (transform == Transform.SPRINTZ || transform == Transform.TS_2DIFF) {
      firstValue = reader.readLong();
    }
    if (transform == Transform.TS_2DIFF) {
      minDiff = reader.readLong();
    }
    int groupSizeBits = (int) reader.readBits(6);
    int groupCount = (int) reader.readBits(32);
    if (groupSizeBits <= 0 || groupSizeBits > GROUP_SIZE_BITS) {
      throw new IllegalArgumentException("Invalid VLBP group-size width: " + groupSizeBits);
    }
    if (groupCount < 0 || groupCount > valueCount) {
      throw new IllegalArgumentException("Invalid VLBP group count: " + groupCount);
    }

    long[] values = new long[valueCount];
    int valueIndex = 0;
    for (int group = 0; group < groupCount; group++) {
      int groupSize = (int) reader.readBits(groupSizeBits);
      int bitWidth = decodeBitWidth((int) reader.readBits(BIT_WIDTH_BITS));
      if (groupSize <= 0 || valueIndex + groupSize > valueCount) {
        throw new IllegalArgumentException("Invalid VLBP group size: " + groupSize);
      }
      for (int i = 0; i < groupSize; i++) {
        values[valueIndex++] = reader.readBits(bitWidth);
      }
    }
    if (valueIndex != valueCount) {
      throw new IllegalArgumentException(
          "VLBP value count mismatch: decoded=" + valueIndex + " expected=" + valueCount);
    }
    return inverseTransform(values, transform, firstValue, minDiff);
  }

  private static TransformedValues transform(long[] values, Transform transform) {
    long[] transformed = new long[values.length];
    switch (transform) {
      case NONE:
      case ZIGZAG:
        for (int i = 0; i < values.length; i++) {
          transformed[i] = zigzagEncode(values[i]);
        }
        return new TransformedValues(transformed, 0, 0, 0);
      case SPRINTZ:
        for (int i = 1; i < values.length; i++) {
          transformed[i - 1] = zigzagEncode(values[i] - values[i - 1]);
        }
        return new TransformedValues(transformed, values[0], 0, 1);
      case TS_2DIFF:
        long minDiff = Long.MAX_VALUE;
        long[] diffs = new long[values.length - 1];
        for (int i = 1; i < values.length; i++) {
          long diff = values[i] - values[i - 1];
          diffs[i - 1] = diff;
          minDiff = Math.min(minDiff, diff);
        }
        if (diffs.length == 0) {
          minDiff = 0;
        }
        for (int i = 0; i < diffs.length; i++) {
          transformed[i] = diffs[i] - minDiff;
        }
        return new TransformedValues(transformed, values[0], minDiff, 2);
      default:
        throw new IllegalArgumentException("Unsupported VLBP transform: " + transform);
    }
  }

  private static long[] inverseTransform(
      long[] values, Transform transform, long firstValue, long minDiff) {
    switch (transform) {
      case NONE:
      case ZIGZAG:
        for (int i = 0; i < values.length; i++) {
          values[i] = zigzagDecode(values[i]);
        }
        return values;
      case SPRINTZ:
        long[] sprintzValues = Arrays.copyOf(values, values.length);
        values[0] = firstValue;
        for (int i = 1; i < values.length; i++) {
          values[i] = values[i - 1] + zigzagDecode(sprintzValues[i - 1]);
        }
        return values;
      case TS_2DIFF:
        long[] normalizedDiffs = Arrays.copyOf(values, values.length);
        values[0] = firstValue;
        for (int i = 1; i < values.length; i++) {
          values[i] = values[i - 1] + normalizedDiffs[i - 1] + minDiff;
        }
        return values;
      default:
        throw new IllegalArgumentException("Unsupported VLBP transform: " + transform);
    }
  }

  private static int[] bitWidths(long[] values) {
    int[] bitWidths = new int[values.length];
    for (int i = 0; i < values.length; i++) {
      bitWidths[i] = bitWidth(values[i]);
    }
    return bitWidths;
  }

  private static PackingPlan computeDpPlan(int[] bitWidths) {
    int n = bitWidths.length;
    int bestCost = Integer.MAX_VALUE;
    int bestGroupSizeBits = 1;
    int[] bestGroupSizes = new int[0];
    int[] bestGroupBitWidths = new int[0];

    int maxPossibleGroupSizeBits = 32 - Integer.numberOfLeadingZeros(n);
    for (int groupSizeBits = 1; groupSizeBits <= maxPossibleGroupSizeBits; groupSizeBits++) {
      int maxGroupSize = Math.min((1 << groupSizeBits) - 1, n);
      int metadataBits = groupSizeBits + BIT_WIDTH_BITS;
      int[] dp = new int[n + 1];
      int[] prev = new int[n + 1];
      int[] prevBitWidth = new int[n + 1];
      Arrays.fill(dp, Integer.MAX_VALUE / 4);
      Arrays.fill(prev, -1);
      dp[0] = 0;

      for (int i = 1; i <= n; i++) {
        int maxBitWidth = 1;
        int lower = Math.max(0, i - maxGroupSize);
        for (int j = i - 1; j >= lower; j--) {
          maxBitWidth = Math.max(maxBitWidth, bitWidths[j]);
          int groupSize = i - j;
          int candidate = dp[j] + groupSize * maxBitWidth + metadataBits;
          if (candidate < dp[i]) {
            dp[i] = candidate;
            prev[i] = j;
            prevBitWidth[i] = maxBitWidth;
          }
        }
      }

      if (dp[n] < bestCost) {
        bestCost = dp[n];
        bestGroupSizeBits = groupSizeBits;
        int[] reverseSizes = new int[n];
        int[] reverseBitWidths = new int[n];
        int count = 0;
        for (int i = n; i > 0; i = prev[i]) {
          int p = prev[i];
          if (p < 0) {
            throw new IllegalStateException("Broken VLBP DP backtrace");
          }
          reverseSizes[count] = i - p;
          reverseBitWidths[count] = prevBitWidth[i];
          count++;
        }
        bestGroupSizes = reverse(reverseSizes, count);
        bestGroupBitWidths = reverse(reverseBitWidths, count);
      }
    }
    return new PackingPlan(bestGroupSizeBits, bestGroupSizes, bestGroupBitWidths);
  }

  private static PackingPlan computeRlPlan(int[] bitWidths) {
    List<Group> groups = new ArrayList<>();
    int i = 0;
    while (i < bitWidths.length) {
      int remaining = bitWidths.length - i;
      int bestLength = 1;
      int bestBitWidth = bitWidths[i];
      int bestCost = Integer.MAX_VALUE;
      for (int length : RL_LENGTH_CANDIDATES) {
        if (length > remaining) {
          break;
        }
        int maxBitWidth = 1;
        for (int j = i; j < i + length; j++) {
          maxBitWidth = Math.max(maxBitWidth, bitWidths[j]);
        }
        int groupSizeBits = minBitsFor(length);
        int cost = length * maxBitWidth + groupSizeBits + BIT_WIDTH_BITS;
        if (cost < bestCost) {
          bestCost = cost;
          bestLength = length;
          bestBitWidth = maxBitWidth;
        }
      }
      groups.add(new Group(bestLength, bestBitWidth));
      i += bestLength;
    }
    mergeAdjacentGroups(groups);
    return toPlan(groups);
  }

  private static void mergeAdjacentGroups(List<Group> groups) {
    boolean changed;
    do {
      changed = false;
      for (int i = 0; i + 1 < groups.size(); i++) {
        Group left = groups.get(i);
        Group right = groups.get(i + 1);
        int before = encodedBits(groups);
        Group merged = new Group(left.size + right.size, Math.max(left.bitWidth, right.bitWidth));
        groups.set(i, merged);
        groups.remove(i + 1);
        int after = encodedBits(groups);
        if (after <= before) {
          changed = true;
        } else {
          groups.set(i, left);
          groups.add(i + 1, right);
        }
      }
    } while (changed);
  }

  private static PackingPlan toPlan(List<Group> groups) {
    int[] groupSizes = new int[groups.size()];
    int[] groupBitWidths = new int[groups.size()];
    for (int i = 0; i < groups.size(); i++) {
      groupSizes[i] = groups.get(i).size;
      groupBitWidths[i] = groups.get(i).bitWidth;
    }
    return new PackingPlan(minGroupSizeBits(groupSizes), groupSizes, groupBitWidths);
  }

  private static byte[] writePlan(long[] values, PackingPlan plan, TransformedValues transformed) {
    int bitCount = 6 + 32;
    for (int i = 0; i < plan.groupSizes.length; i++) {
      bitCount += plan.groupSizeBits + BIT_WIDTH_BITS + plan.groupSizes[i] * plan.groupBitWidths[i];
    }
    int headerBytes = transformed.headerBytes();
    byte[] out = new byte[headerBytes + (bitCount + 7) / 8];
    BitWriter writer = new BitWriter(out);
    if (transformed.headerBytes() >= Long.BYTES) {
      writer.writeBits(transformed.firstValue, 64);
    }
    if (transformed.headerBytes() >= Long.BYTES * 2) {
      writer.writeBits(transformed.minDiff, 64);
    }
    writer.writeBits(plan.groupSizeBits, 6);
    writer.writeBits(plan.groupSizes.length, 32);
    int valueIndex = 0;
    for (int i = 0; i < plan.groupSizes.length; i++) {
      int groupSize = plan.groupSizes[i];
      int bitWidth = plan.groupBitWidths[i];
      writer.writeBits(groupSize, plan.groupSizeBits);
      writer.writeBits(encodeBitWidth(bitWidth), BIT_WIDTH_BITS);
      for (int j = 0; j < groupSize; j++) {
        writer.writeBits(values[valueIndex++], bitWidth);
      }
    }
    return out;
  }

  private static int encodedBits(List<Group> groups) {
    int[] groupSizes = new int[groups.size()];
    for (int i = 0; i < groups.size(); i++) {
      groupSizes[i] = groups.get(i).size;
    }
    int groupSizeBits = minGroupSizeBits(groupSizes);
    int bits = 6 + 32;
    for (Group group : groups) {
      bits += groupSizeBits + BIT_WIDTH_BITS + group.size * group.bitWidth;
    }
    return ((bits + 7) / 8) * 8;
  }

  private static int bitWidth(long value) {
    return Math.max(1, 64 - Long.numberOfLeadingZeros(value));
  }

  private static long zigzagEncode(long value) {
    return (value << 1) ^ (value >> 63);
  }

  private static long zigzagDecode(long value) {
    return (value >>> 1) ^ -(value & 1L);
  }

  private static int encodeBitWidth(int bitWidth) {
    if (bitWidth <= 0 || bitWidth > 64) {
      throw new IllegalArgumentException("Invalid VLBP bit width: " + bitWidth);
    }
    return bitWidth == 64 ? 0 : bitWidth;
  }

  private static int decodeBitWidth(int value) {
    if (value < 0 || value > 63) {
      throw new IllegalArgumentException("Invalid encoded VLBP bit width: " + value);
    }
    return value == 0 ? 64 : value;
  }

  private static int minGroupSizeBits(int[] groupSizes) {
    int max = 1;
    for (int groupSize : groupSizes) {
      max = Math.max(max, groupSize);
    }
    return minBitsFor(max);
  }

  private static int minBitsFor(int value) {
    return Math.max(1, 32 - Integer.numberOfLeadingZeros(value));
  }

  private static int[] reverse(int[] values, int count) {
    int[] out = new int[count];
    for (int i = 0; i < count; i++) {
      out[i] = values[count - 1 - i];
    }
    return out;
  }

  private static final class PackingPlan {
    final int groupSizeBits;
    final int[] groupSizes;
    final int[] groupBitWidths;

    PackingPlan(int groupSizeBits, int[] groupSizes, int[] groupBitWidths) {
      this.groupSizeBits = groupSizeBits;
      this.groupSizes = groupSizes;
      this.groupBitWidths = groupBitWidths;
    }
  }

  private static final class Group {
    final int size;
    final int bitWidth;

    Group(int size, int bitWidth) {
      this.size = size;
      this.bitWidth = bitWidth;
    }
  }

  private static final class TransformedValues {
    final long[] values;
    final long firstValue;
    final long minDiff;
    final int headerLongs;

    TransformedValues(long[] values, long firstValue, long minDiff, int headerLongs) {
      this.values = values;
      this.firstValue = firstValue;
      this.minDiff = minDiff;
      this.headerLongs = headerLongs;
    }

    int headerBytes() {
      return headerLongs * Long.BYTES;
    }
  }

  private static final class BitWriter {
    private final byte[] data;
    private int bytePos;
    private int bitPos;

    BitWriter(byte[] data) {
      this.data = data;
    }

    void writeBits(long value, int numBits) {
      while (numBits > 0) {
        int freeBits = 8 - bitPos;
        int take = Math.min(freeBits, numBits);
        int shift = numBits - take;
        int chunk = (int) ((value >>> shift) & ((1L << take) - 1L));
        data[bytePos] |= (byte) (chunk << (freeBits - take));
        bitPos += take;
        if (bitPos == 8) {
          bitPos = 0;
          bytePos++;
        }
        numBits -= take;
      }
    }
  }

  private static final class BitReader {
    private final byte[] data;
    private int bytePos;
    private int bitPos;

    BitReader(byte[] data) {
      this.data = data;
    }

    long readBits(int numBits) {
      long value = 0L;
      while (numBits > 0) {
        int availableBits = 8 - bitPos;
        int take = Math.min(availableBits, numBits);
        int shift = availableBits - take;
        int chunk = ((data[bytePos] & 0xFF) >>> shift) & ((1 << take) - 1);
        value = (value << take) | chunk;
        bitPos += take;
        if (bitPos == 8) {
          bitPos = 0;
          bytePos++;
        }
        numBits -= take;
      }
      return value;
    }

    long readLong() {
      return readBits(64);
    }
  }
}
