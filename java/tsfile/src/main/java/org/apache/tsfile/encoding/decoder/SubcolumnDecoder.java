package org.apache.tsfile.encoding.decoder;

import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class SubcolumnDecoder extends Decoder {

  protected long count = 0;
  protected byte[] deltaBuf;

  protected int readIntTotalCount = 0;

  protected int nextReadIndex = 0;

  /** max bit length of all value in a pack. */
  protected int packWidth;

  /** data number in this pack. */
  protected int packNum;

  protected int encodingLength;

  protected int beta = 3;

  protected int encode_pos = 0;

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
      byte[] encoded, int decode_pos, int bit_width, int num_values, int[] result_list) {
    int block_num = num_values / 8;
    int remainder = num_values % 8;

    for (int i = 0; i < block_num; i++) { // bitpacking
      unpack8Values(encoded, decode_pos, bit_width, result_list, i * 8);
      decode_pos += bit_width;
    }

    decode_pos *= 8;

    for (int i = 0; i < remainder; i++) {
      result_list[block_num * 8 + i] = BytesUtils.bytesToInt(encoded, decode_pos, bit_width);
      decode_pos += bit_width;
    }

    return (decode_pos + 7) / 8;
  }

  public static void unpack8Values(
      byte[] encoded, int offset, int width, int[] result_list, int result_offset) {
    int byteIdx = offset;
    long buffer = 0;
    // total bits which have read from 'buf' to 'buffer'. i.e.,
    // number of available bits to be decoded.
    int totalBits = 0;
    int valueIdx = 0;

    while (valueIdx < 8) {
      // If current available bits are not enough to decode one Integer,
      // then add next byte from buf to 'buffer' until totalBits >= width
      while (totalBits < width) {
        buffer = (buffer << 8) | (encoded[byteIdx] & 0xFF);
        byteIdx++;
        totalBits += 8;
      }

      // If current available bits are enough to decode one Integer,
      // then decode one Integer one by one until left bits in 'buffer' is
      // not enough to decode one Integer.
      while (totalBits >= width && valueIdx < 8) {
        // result_list.add((int) (buffer >>> (totalBits - width)));
        result_list[result_offset + valueIdx] = (int) (buffer >>> (totalBits - width));
        valueIdx++;
        totalBits -= width;
        buffer = buffer & ((1L << totalBits) - 1);
      }
    }
  }

  public static int decodeBitPacking(
      byte[] encoded, int decode_pos, int bit_width, int num_values, long[] result_list) {
    int block_num = num_values / 8;
    int remainder = num_values % 8;

    for (int i = 0; i < block_num; i++) { // bitpacking
      unpack8Values(encoded, decode_pos, bit_width, result_list, i * 8);
      decode_pos += bit_width;
    }

    decode_pos *= 8;

    for (int i = 0; i < remainder; i++) {
      result_list[block_num * 8 + i] = BytesUtils.bytesToLong(encoded, decode_pos, bit_width);
      decode_pos += bit_width;
    }

    return (decode_pos + 7) / 8;
  }

  public static void unpack8Values(
      byte[] encoded, int offset, int width, long[] result_list, int result_offset) {
    int byteIdx = offset;
    long buffer = 0;
    // total bits which have read from 'buf' to 'buffer'. i.e.,
    // number of available
    // bits to be decoded.
    int totalBits = 0;
    int valueIdx = 0;

    while (valueIdx < 8) {
      // If current available bits are not enough to decode one Integer,
      // then add next byte from buf to 'buffer' until totalBits >= width
      while (totalBits < width) {
        buffer = (buffer << 8) | (encoded[byteIdx] & 0xFF);
        byteIdx++;
        totalBits += 8;
      }

      // If current available bits are enough to decode one Integer,
      // then decode one Integer one by one until left bits in 'buffer' is
      // not enough to decode one Integer.
      while (totalBits >= width && valueIdx < 8) {
        // result_list.add((int) (buffer >>> (totalBits - width)));
        result_list[result_offset + valueIdx] = buffer >>> (totalBits - width);
        valueIdx++;
        totalBits -= width;
        buffer = buffer & ((1L << totalBits) - 1);
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

      encode_pos = 0;

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

    private int getValueWidth(int v) {
      return 32 - Integer.numberOfLeadingZeros(v);
    }

    private void readPack() {
      int bw = getValueWidth(packNum);

      int l = (packWidth + beta - 1) / beta;

      int[] bitWidthList = new int[l];

      encode_pos = decodeBitPacking(deltaBuf, encode_pos, 8, l, bitWidthList);

      int[][] subcolumnList = new int[l][packNum];

      int[] encodingType = new int[l];

      encode_pos = decodeBitPacking(deltaBuf, encode_pos, 1, l, encodingType);

      for (int i = l - 1; i >= 0; i--) {
        int type = encodingType[i];
        int bitWidth = bitWidthList[i];
        if (type == 0) {
          encode_pos = decodeBitPacking(deltaBuf, encode_pos, bitWidth, packNum, subcolumnList[i]);
        } else {
          int index = ((deltaBuf[encode_pos] & 0xFF) << 8) | (deltaBuf[encode_pos + 1] & 0xFF);
          encode_pos += 2;

          int[] run_length = new int[index];
          int[] rle_values = new int[index];

          encode_pos = decodeBitPacking(deltaBuf, encode_pos, bw, index, run_length);
          encode_pos = decodeBitPacking(deltaBuf, encode_pos, bitWidth, index, rle_values);

          int currentIndex = 0;
          for (int j = 0; j < index; j++) {
            int endPos = run_length[j];
            int value = rle_values[j];
            while (currentIndex < endPos) {
              subcolumnList[i][currentIndex] = value;
              currentIndex++;
            }
          }
        }
      }

      for (int i = 0; i < l; i++) {
        int shiftAmount = i * beta;
        for (int j = 0; j < packNum; j++) {
          data[j] |= subcolumnList[i][j] << shiftAmount;
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

      encode_pos = 0;

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

    private int getValueWidth(long v) {
      return 64 - Long.numberOfLeadingZeros(v);
    }

    private void readPack() {
      int bw = getValueWidth(packNum);

      int l = (packWidth + beta - 1) / beta;

      int[] bitWidthList = new int[l];

      encode_pos = decodeBitPacking(deltaBuf, encode_pos, 8, l, bitWidthList);

      long[][] subcolumnList = new long[l][packNum];

      int[] encodingType = new int[l];

      encode_pos = decodeBitPacking(deltaBuf, encode_pos, 1, l, encodingType);

      for (int i = l - 1; i >= 0; i--) {
        long type = encodingType[i];
        int bitWidth = bitWidthList[i];
        if (type == 0) {
          encode_pos = decodeBitPacking(deltaBuf, encode_pos, bitWidth, packNum, subcolumnList[i]);
        } else {
          int index = ((deltaBuf[encode_pos] & 0xFF) << 8) | (deltaBuf[encode_pos + 1] & 0xFF);
          encode_pos += 2;

          long[] run_length = new long[index];
          long[] rle_values = new long[index];

          encode_pos = decodeBitPacking(deltaBuf, encode_pos, bw, index, run_length);
          encode_pos = decodeBitPacking(deltaBuf, encode_pos, bitWidth, index, rle_values);

          int currentIndex = 0;
          for (int j = 0; j < index; j++) {
            long endPos = run_length[j];
            long value = rle_values[j];
            while (currentIndex < endPos) {
              subcolumnList[i][currentIndex] = value;
              currentIndex++;
            }
          }
        }
      }

      for (int i = 0; i < l; i++) {
        int shiftAmount = i * beta;
        for (int j = 0; j < packNum; j++) {
          data[j] |= subcolumnList[i][j] << shiftAmount;
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
