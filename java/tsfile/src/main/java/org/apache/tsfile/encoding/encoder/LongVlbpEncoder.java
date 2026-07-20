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

package org.apache.tsfile.encoding.encoder;

import org.apache.tsfile.encoding.vlbp.VlbpCodec;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class LongVlbpEncoder extends Encoder {

  private final VlbpCodec.Strategy strategy;
  private final VlbpCodec.Transform transform;
  private long[] values = new long[128];
  private int valueCount;

  public LongVlbpEncoder(TSEncoding encoding) {
    super(encoding);
    this.strategy = isDp(encoding) ? VlbpCodec.Strategy.DP : VlbpCodec.Strategy.RL;
    this.transform = transformOf(encoding);
  }

  @Override
  public void encode(long value, ByteArrayOutputStream out) {
    if (valueCount == values.length) {
      values = Arrays.copyOf(values, values.length * 2);
    }
    values[valueCount++] = value;
  }

  @Override
  public void flush(ByteArrayOutputStream out) throws IOException {
    if (valueCount == 0) {
      return;
    }
    byte[] encoded = VlbpCodec.encode(Arrays.copyOf(values, valueCount), strategy, transform);
    ReadWriteForEncodingUtils.writeUnsignedVarInt(encoded.length, out);
    ReadWriteForEncodingUtils.writeUnsignedVarInt(valueCount, out);
    out.write(encoded);
    valueCount = 0;
  }

  @Override
  public int getOneItemMaxSize() {
    return Long.BYTES + 2;
  }

  @Override
  public long getMaxByteSize() {
    return (long) values.length * Long.BYTES;
  }

  private static boolean isDp(TSEncoding encoding) {
    return encoding == TSEncoding.VLBP_DP
        || encoding == TSEncoding.SPRINTZ_VLBP_DP
        || encoding == TSEncoding.ZIGZAG_VLBP_DP
        || encoding == TSEncoding.TS2DIFF_VLBP_DP;
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
