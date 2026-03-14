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

package org.apache.tsfile.encoding.decoder;

import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public abstract class SubcolumnDecoder extends Decoder {

  protected long count = 0;
  protected byte[] deltaBuf;

  protected int readIntTotalCount = 0;
  protected int nextReadIndex = 0;
  protected int packWidth;
  protected int packNum;
  protected int encodingLength;

  public SubcolumnDecoder() {
    super(TSEncoding.SUBCOLUMN);
  }

  protected abstract void readHeader(ByteBuffer buffer) throws IOException;

  protected abstract void allocateDataArray();

  @Override
  public boolean hasNext(ByteBuffer buffer) throws IOException {
    return (nextReadIndex < readIntTotalCount) || buffer.remaining() > 0;
  }

  public static int decodeBitPacking(
      byte[] encoded, int decodePos, int bitWidth, int numValues, int[] resultList) {
    if (bitWidth <= 0 || numValues <= 0) {
      return decodePos;
    }

    int blockNum = numValues / 8;
    int remainder = numValues % 8;

    for (int i = 0; i < blockNum; i++) {
      unpack8Values(encoded, decodePos, bitWidth, resultList, i * 8);
      decodePos += bitWidth;
    }

    decodePos *= 8;

    for (int i = 0; i < remainder; i++) {
      resultList[blockNum * 8 + i] = BytesUtils.bytesToInt(encoded, decodePos, bitWidth);
      decodePos += bitWidth;
    }

    return (decodePos + 7) / 8;
  }

  public static void unpack8Values(
      byte[] encoded, int offset, int width, int[] resultList, int resultOffset) {
    int byteIdx = offset;
    long buffer = 0;
    int totalBits = 0;
    int valueIdx = 0;

    while (valueIdx < 8) {
      while (totalBits < width) {
        buffer = (buffer << 8) | (encoded[byteIdx] & 0xFFL);
        byteIdx++;
        totalBits += 8;
      }

      while (totalBits >= width && valueIdx < 8) {
        resultList[resultOffset + valueIdx] = (int) (buffer >>> (totalBits - width));
        valueIdx++;
        totalBits -= width;
        buffer &= (1L << totalBits) - 1;
      }
    }
  }

  public static int decodeBitPacking(
      byte[] encoded, int decodePos, int bitWidth, int numValues, long[] resultList) {
    if (bitWidth <= 0 || numValues <= 0) {
      return decodePos;
    }

    int blockNum = numValues / 8;
    int remainder = numValues % 8;

    for (int i = 0; i < blockNum; i++) {
      unpack8Values(encoded, decodePos, bitWidth, resultList, i * 8);
      decodePos += bitWidth;
    }

    decodePos *= 8;

    for (int i = 0; i < remainder; i++) {
      resultList[blockNum * 8 + i] = BytesUtils.bytesToLong(encoded, decodePos, bitWidth);
      decodePos += bitWidth;
    }

    return (decodePos + 7) / 8;
  }

  public static void unpack8Values(
      byte[] encoded, int offset, int width, long[] resultList, int resultOffset) {
    int byteIdx = offset;
    long buffer = 0;
    int totalBits = 0;
    int valueIdx = 0;

    while (valueIdx < 8) {
      while (totalBits < width) {
        buffer = (buffer << 8) | (encoded[byteIdx] & 0xFFL);
        byteIdx++;
        totalBits += 8;
      }

      while (totalBits >= width && valueIdx < 8) {
        resultList[resultOffset + valueIdx] = buffer >>> (totalBits - width);
        valueIdx++;
        totalBits -= width;
        buffer &= (1L << totalBits) - 1;
      }
    }
  }

  public static class IntSubcolumnDecoder extends SubcolumnDecoder {

    private int[] data;
    private int minDeltaBase;

    public IntSubcolumnDecoder() {
      super();
    }

    protected int readT(ByteBuffer buffer) {
      if (nextReadIndex == readIntTotalCount) {
        loadIntBatch(buffer);
      }
      return data[nextReadIndex++];
    }

    @Override
    public int readInt(ByteBuffer buffer) {
      return readT(buffer);
    }

    protected void loadIntBatch(ByteBuffer buffer) {
      packNum = ReadWriteIOUtils.readInt(buffer);
      packWidth = ReadWriteIOUtils.readInt(buffer);
      count++;

      readHeader(buffer);
      allocateDataArray();
      readIntTotalCount = packNum;
      nextReadIndex = 0;

      if (packWidth != 0) {
        encodingLength = ReadWriteIOUtils.readInt(buffer);
        deltaBuf = new byte[encodingLength];
        buffer.get(deltaBuf);
        readPack();
      }

      for (int i = 0; i < packNum; i++) {
        data[i] += minDeltaBase;
      }
    }

    private int getValueWidth(int value) {
      return value == 0 ? 0 : 32 - Integer.numberOfLeadingZeros(value);
    }

    private void readPack() {
      int decodePos = 0;
      int beta = deltaBuf[decodePos++] & 0xFF;
      int l = (packWidth + beta - 1) / beta;
      int[] bitWidthList = new int[l];
      decodePos = decodeBitPacking(deltaBuf, decodePos, 8, l, bitWidthList);

      int[] encodingType = new int[l];
      decodePos = decodeBitPacking(deltaBuf, decodePos, 2, l, encodingType);

      int[] subcolumnBuffer = new int[packNum];
      int runLengthBitWidth = getValueWidth(packNum);

      for (int i = 0; i < l; i++) {
        Arrays.fill(subcolumnBuffer, 0);
        int currentBitWidth = bitWidthList[i];
        if (encodingType[i] == 0) {
          decodePos =
              decodeBitPacking(deltaBuf, decodePos, currentBitWidth, packNum, subcolumnBuffer);
        } else if (encodingType[i] == 1) {
          int runCount = ((deltaBuf[decodePos] & 0xFF) << 8) | (deltaBuf[decodePos + 1] & 0xFF);
          decodePos += 2;

          int[] runLength = new int[runCount];
          int[] rleValues = new int[runCount];
          decodePos =
              decodeBitPacking(deltaBuf, decodePos, runLengthBitWidth, runCount, runLength);
          decodePos = decodeBitPacking(deltaBuf, decodePos, currentBitWidth, runCount, rleValues);

          int currentIndex = 0;
          for (int j = 0; j < runCount; j++) {
            int endPos = runLength[j];
            int value = rleValues[j];
            while (currentIndex < endPos) {
              subcolumnBuffer[currentIndex++] = value;
            }
          }
        } else {
          int cardinality =
              ((deltaBuf[decodePos] & 0xFF) << 8) | (deltaBuf[decodePos + 1] & 0xFF);
          decodePos += 2;

          int dictBitWidth = getValueWidth(cardinality);
          int[] dictKeyList = new int[cardinality];
          decodePos =
              decodeBitPacking(deltaBuf, decodePos, currentBitWidth, cardinality, dictKeyList);
          decodePos = decodeBitPacking(deltaBuf, decodePos, dictBitWidth, packNum, subcolumnBuffer);

          for (int j = 0; j < packNum; j++) {
            subcolumnBuffer[j] = dictKeyList[subcolumnBuffer[j]];
          }
        }

        int shiftAmount = i * beta;
        for (int j = 0; j < packNum; j++) {
          data[j] |= subcolumnBuffer[j] << shiftAmount;
        }
      }
    }

    @Override
    protected void readHeader(ByteBuffer buffer) {
      minDeltaBase = ReadWriteIOUtils.readInt(buffer);
    }

    @Override
    protected void allocateDataArray() {
      data = new int[packNum];
    }

    @Override
    public void reset() {
      // do nothing
    }
  }

  public static class LongSubcolumnDecoder extends SubcolumnDecoder {

    private long[] data;
    private long minDeltaBase;

    public LongSubcolumnDecoder() {
      super();
    }

    protected long readT(ByteBuffer buffer) {
      if (nextReadIndex == readIntTotalCount) {
        loadLongBatch(buffer);
      }
      return data[nextReadIndex++];
    }

    @Override
    public long readLong(ByteBuffer buffer) {
      return readT(buffer);
    }

    protected void loadLongBatch(ByteBuffer buffer) {
      packNum = ReadWriteIOUtils.readInt(buffer);
      packWidth = ReadWriteIOUtils.readInt(buffer);
      count++;

      readHeader(buffer);
      allocateDataArray();
      readIntTotalCount = packNum;
      nextReadIndex = 0;

      if (packWidth != 0) {
        encodingLength = ReadWriteIOUtils.readInt(buffer);
        deltaBuf = new byte[encodingLength];
        buffer.get(deltaBuf);
        readPack();
      }

      for (int i = 0; i < packNum; i++) {
        data[i] += minDeltaBase;
      }
    }

    private int getValueWidth(long value) {
      return value == 0 ? 0 : 64 - Long.numberOfLeadingZeros(value);
    }

    private void readPack() {
      int decodePos = 0;
      int beta = deltaBuf[decodePos++] & 0xFF;
      int l = (packWidth + beta - 1) / beta;
      int[] bitWidthList = new int[l];
      decodePos = decodeBitPacking(deltaBuf, decodePos, 8, l, bitWidthList);

      int[] encodingType = new int[l];
      decodePos = decodeBitPacking(deltaBuf, decodePos, 2, l, encodingType);

      long[] subcolumnBuffer = new long[packNum];
      int runLengthBitWidth = getValueWidth(packNum);

      for (int i = 0; i < l; i++) {
        Arrays.fill(subcolumnBuffer, 0L);
        int currentBitWidth = bitWidthList[i];
        if (encodingType[i] == 0) {
          decodePos =
              decodeBitPacking(deltaBuf, decodePos, currentBitWidth, packNum, subcolumnBuffer);
        } else if (encodingType[i] == 1) {
          int runCount = ((deltaBuf[decodePos] & 0xFF) << 8) | (deltaBuf[decodePos + 1] & 0xFF);
          decodePos += 2;

          int[] runLength = new int[runCount];
          long[] rleValues = new long[runCount];
          decodePos =
              decodeBitPacking(deltaBuf, decodePos, runLengthBitWidth, runCount, runLength);
          decodePos = decodeBitPacking(deltaBuf, decodePos, currentBitWidth, runCount, rleValues);

          int currentIndex = 0;
          for (int j = 0; j < runCount; j++) {
            int endPos = runLength[j];
            long value = rleValues[j];
            while (currentIndex < endPos) {
              subcolumnBuffer[currentIndex++] = value;
            }
          }
        } else {
          int cardinality =
              ((deltaBuf[decodePos] & 0xFF) << 8) | (deltaBuf[decodePos + 1] & 0xFF);
          decodePos += 2;

          int dictBitWidth = getValueWidth(cardinality);
          long[] dictKeyList = new long[cardinality];
          decodePos =
              decodeBitPacking(deltaBuf, decodePos, currentBitWidth, cardinality, dictKeyList);
          decodePos = decodeBitPacking(deltaBuf, decodePos, dictBitWidth, packNum, subcolumnBuffer);

          for (int j = 0; j < packNum; j++) {
            subcolumnBuffer[j] = dictKeyList[(int) subcolumnBuffer[j]];
          }
        }

        int shiftAmount = i * beta;
        for (int j = 0; j < packNum; j++) {
          data[j] |= subcolumnBuffer[j] << shiftAmount;
        }
      }
    }

    @Override
    protected void readHeader(ByteBuffer buffer) {
      minDeltaBase = ReadWriteIOUtils.readLong(buffer);
    }

    @Override
    protected void allocateDataArray() {
      data = new long[packNum];
    }

    @Override
    public void reset() {
      // do nothing
    }
  }
}
