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

import org.apache.tsfile.encoding.vlbp.VlbpCodec;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;

import java.nio.ByteBuffer;

public class LongVlbpDecoder extends Decoder {

  private final VlbpCodec.Transform transform;
  private long[] values = new long[0];
  private int index;

  public LongVlbpDecoder(TSEncoding encoding) {
    super(encoding);
    this.transform = transformOf(encoding);
  }

  @Override
  public long readLong(ByteBuffer buffer) {
    if (index >= values.length) {
      readBlock(buffer);
    }
    return values[index++];
  }

  @Override
  public boolean hasNext(ByteBuffer buffer) {
    return index < values.length || buffer.remaining() > 0;
  }

  @Override
  public void reset() {
    values = new long[0];
    index = 0;
  }

  private void readBlock(ByteBuffer buffer) {
    int byteLength = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
    int valueCount = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
    byte[] encoded = new byte[byteLength];
    buffer.get(encoded);
    values = VlbpCodec.decode(encoded, valueCount, transform);
    index = 0;
  }

  private static VlbpCodec.Transform transformOf(TSEncoding encoding) {
    switch (encoding) {
      case SPRINTZ_VLBP_DP:
      case SPRINTZ_VLBP_RL:
        return VlbpCodec.Transform.SPRINTZ;
      case ZIGZAG_VLBP_DP:
      case ZIGZAG_VLBP_RL:
        return VlbpCodec.Transform.ZIGZAG;
      case TS2DIFF_VLBP_DP:
      case TS2DIFF_VLBP_RL:
        return VlbpCodec.Transform.TS_2DIFF;
      default:
        return VlbpCodec.Transform.NONE;
    }
  }
}
