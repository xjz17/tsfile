package org.apache.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class BPDP {

    private static final int CHUNK_SIZE = 1024;
    static int all_time_of_repeat = 10;
    static int all_max_pack_size = 1;
    private static final Map<String, PackingPlan> PACKING_PLAN_CACHE = new HashMap<>();

    static String trimStr(String s) {
        if (s == null) return "";
        int a = 0;
        while (a < s.length() && Character.isWhitespace(s.charAt(a))) a++;
        if (a == s.length()) return "";
        int b = s.length() - 1;
        while (b >= 0 && Character.isWhitespace(s.charAt(b))) b--;
        return s.substring(a, b + 1);
    }

    static String stripEnclosingQuotes(String s) {
        if (s == null) return "";
        if (s.length() >= 2) {
            char f = s.charAt(0);
            char l = s.charAt(s.length() - 1);
            if ((f == '"' && l == '"') || (f == '\'' && l == '\'')) {
                return s.substring(1, s.length() - 1);
            }
        }
        return s;
    }

    // scaleNumbers: use BigDecimal to parse, scale by 10^decimalMax, shift so min becomes 0, return long[] with clipping
//    static long[] scaleNumbers(List<String> numbers, int decimalMax) {
//        int n = numbers.size();
//        long[] result = new long[n];
//        if (n == 0) return result;
//
//        BigDecimal scale = BigDecimal.ONE;
//        for (int i = 0; i < decimalMax; ++i) scale = scale.multiply(BigDecimal.TEN);
//
//        BigDecimal[] vals = new BigDecimal[n];
//        for (int i = 0; i < n; ++i) {
//            String s = trimStr(numbers.get(i));
//            s = stripEnclosingQuotes(s);
//            if (s.isEmpty()) { vals[i] = BigDecimal.ZERO; continue; }
//            s = s.replace(",", ""); // remove thousands sep
//
//            // If scientific notation present, BigDecimal can parse it
//            try {
//                BigDecimal bd = new BigDecimal(s);
//                BigDecimal scaled = bd.multiply(scale);
//                // rounding to nearest whole
//                BigDecimal rounded = scaled.setScale(0, RoundingMode.HALF_UP);
//                vals[i] = rounded;
//            } catch (Exception ex) {
//                // fallback: parse double
//                try {
//                    double dv = Double.parseDouble(s);
//                    BigDecimal bd = BigDecimal.valueOf(dv).multiply(scale);
//                    vals[i] = bd.setScale(0, RoundingMode.HALF_UP);
//                } catch (Exception ex2) {
//                    System.err.println("Warning: cannot parse token '" + numbers.get(i) + "', set to 0");
//                    vals[i] = BigDecimal.ZERO;
//                }
//            }
//        }
//
//        // find min
//        BigDecimal minv = vals[0];
//        for (int i = 1; i < n; ++i) if (vals[i].compareTo(minv) < 0) minv = vals[i];
//
//        for (int i = 0; i < n; ++i) {
//            BigDecimal shifted = vals[i].subtract(minv);
//            // clamp to long range
//            try {
//                BigInteger bi = shifted.toBigIntegerExact();
//                if (bi.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) result[i] = Long.MAX_VALUE;
//                else if (bi.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0) result[i] = Long.MIN_VALUE;
//                else result[i] = bi.longValue();
//            } catch (ArithmeticException ae) {
//                // not an integer exactly: fallback by converting to long with rounding
//                BigDecimal rounded = shifted.setScale(0, RoundingMode.HALF_UP);
//                try {
//                    BigInteger bi = rounded.toBigIntegerExact();
//                    if (bi.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) result[i] = Long.MAX_VALUE;
//                    else if (bi.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0) result[i] = Long.MIN_VALUE;
//                    else result[i] = bi.longValue();
//                } catch (Exception ex) {
//                    result[i] = 0;
//                }
//            }
//        }
//        return result;
//    }



    public static int computeMinPackingCost(int[] bitWidths, int pack_size) {
        int N = bitWidths.length;
        if (N == 0) {
            return 0;
        }

        // Precompute the maximum bit width for all intervals [l, r] (0-based)
        int[][] maxB = new int[N][N];
        for (int l = 0; l < N; l++) {
            maxB[l][l] = bitWidths[l];
            for (int r = l + 1; r < N; r++) {
                maxB[l][r] = Math.max(maxB[l][r - 1], bitWidths[r]);
            }
        }

        int minTotalCost = maxB[0][N-1]*pack_size*bitWidths.length;

        // Enumerate all possible C values (ceil(log2(max_pack_size + 1)))
        int maxPossibleC = 64 - Long.numberOfLeadingZeros(N); // ceil(log2(N + 1))

        for (int C = 1; C <= maxPossibleC; C++) {
            int low_C = (C == 1) ? 1 : (1 << (C - 1));
            int high_C = Math.min((1 << C) - 1, N);

            // DP table: dp[i][a] - min cost for first i octads, a=1 if at least one pack >= low_C
            int[][] dp = new int[N + 1][2];
            int[][] prevState = new int[N + 1][2]; // for backtracking
            int[][] packSize = new int[N + 1][2]; // for backtracking

            // Initialize DP table
            for (int i = 0; i <= N; i++) {
                dp[i][0] = Integer.MAX_VALUE / 2;
                dp[i][1] = Integer.MAX_VALUE / 2;
            }
            dp[0][0] = 0;

            for (int i = 1; i <= N; i++) {
                for (int k = Math.max(1, i - high_C + 1); k <= i; k++) {
                    int packLength = i - k + 1;
                    int currentMaxB = maxB[k - 1][i - 1]; // convert to 0-based indexing

                    // Calculate pack cost: pack_size * packLength * currentMaxB + 6 + C
                    int packCost = pack_size * packLength * currentMaxB + 6 + C;

                    // Update DP states based on pack size
                    if (packLength < low_C) {
                        // Cannot transition to state 1 with small packs
                        if (dp[k - 1][0] + packCost < dp[i][0]) {
                            dp[i][0] = dp[k - 1][0] + packCost;
                            prevState[i][0] = k - 1;
                            packSize[i][0] = packLength;
                        }
                        if (dp[k - 1][1] + packCost < dp[i][1]) {
                            dp[i][1] = dp[k - 1][1] + packCost;
                            prevState[i][1] = k - 1;
                            packSize[i][1] = packLength;
                        }
                    } else {
                        // Large pack can transition both states to state 1
                        if (dp[k - 1][0] + packCost < dp[i][1]) {
                            dp[i][1] = dp[k - 1][0] + packCost;
                            prevState[i][1] = k - 1;
                            packSize[i][1] = packLength;
                        }
                        if (dp[k - 1][1] + packCost < dp[i][1]) {
                            dp[i][1] = dp[k - 1][1] + packCost;
                            prevState[i][1] = k - 1;
                            packSize[i][1] = packLength;
                        }
                    }
                }
            }

            // Update minimum total cost for this C value
            if (dp[N][1] < minTotalCost) {
                minTotalCost = dp[N][1];
            }
        }

        return minTotalCost;
    }

    // 动态规划结果类
    static class PackingPlan {
        int optimalC;
        int[] groupSizes;    // 每个分组包含的块数
        int[] groupBitWidths; // 每个分组的位宽
        int totalCost;

        PackingPlan(int optimalC, int[] groupSizes, int[] groupBitWidths, int totalCost) {
            this.optimalC = optimalC;
            this.groupSizes = groupSizes == null ? new int[0] : groupSizes;
            this.groupBitWidths = groupBitWidths == null ? new int[0] : groupBitWidths;
            this.totalCost = totalCost;
        }
    }

    public static PackingPlan computeOptimalPackingPlan(int[] bitWidths, int pack_size) {
        String cacheKey = pack_size + ":" + Arrays.toString(bitWidths);
        PackingPlan cachedPlan = PACKING_PLAN_CACHE.get(cacheKey);
        if (cachedPlan != null) {
            return cachedPlan;
        }
        PackingPlan plan = computeOptimalPackingPlanUncached(bitWidths, pack_size);
        PACKING_PLAN_CACHE.put(cacheKey, plan);
        return plan;
    }

    static class MinCandidateDeque {
        int[] indices;
        int[] values;
        int head;
        int tail;

        MinCandidateDeque(int capacity) {
            indices = new int[Math.max(2, capacity + 2)];
            values = new int[indices.length];
        }

        void clear() {
            head = 0;
            tail = 0;
        }

        boolean isEmpty() {
            return head == tail;
        }

        void add(int index, int value) {
            while (tail > head && values[tail - 1] >= value) {
                tail--;
            }
            indices[tail] = index;
            values[tail] = value;
            tail++;
        }

        void expireBefore(int minIndex) {
            while (head < tail && indices[head] < minIndex) {
                head++;
            }
            if (head == tail) {
                clear();
            }
        }

        int bestIndex() {
            return indices[head];
        }

        int bestValue() {
            return values[head];
        }
    }

    // 计算最优分组方案的完整实现
    private static PackingPlan computeOptimalPackingPlanUncached(int[] bitWidths, int pack_size) {
        int N = bitWidths.length;
        if (N == 0) {
            return new PackingPlan(0, new int[0], new int[0], 0);
        }

        int minTotalCost = Integer.MAX_VALUE;
        int bestC = 1;
        int[] bestGroupSizes = new int[0];
        int[] bestGroupBitWidths = new int[0];

        int maxPossibleC = 32 - Integer.numberOfLeadingZeros(N);

        final int INF = Integer.MAX_VALUE / 4;
        for (int C = 1; C <= maxPossibleC; C++) {
            int high_C = Math.min((1 << C) - 1, N);
            int metadataCost = 6 + C;

            int[] dp = new int[N + 1];
            int[] prev = new int[N + 1];
            int[] prevBitWidth = new int[N + 1];
            Arrays.fill(dp, INF);
            Arrays.fill(prev, -1);
            dp[0] = 0;

            MinCandidateDeque[] queues = new MinCandidateDeque[65];
            for (int b = 1; b <= 64; b++) {
                queues[b] = new MinCandidateDeque(N + 1);
                queues[b].add(0, 0);
            }

            for (int i = 1; i <= N; i++) {
                int currentBw = bitWidths[i - 1];
                for (int b = 1; b < currentBw; b++) {
                    queues[b].clear();
                }

                int bestValue = INF;
                int bestPrev = -1;
                int bestBw = currentBw;
                int minPrev = Math.max(0, i - high_C);
                for (int b = currentBw; b <= 64; b++) {
                    MinCandidateDeque q = queues[b];
                    q.expireBefore(minPrev);
                    if (q.isEmpty()) {
                        continue;
                    }
                    int candidate = q.bestValue() + pack_size * b * i + metadataCost;
                    if (candidate < bestValue) {
                        bestValue = candidate;
                        bestPrev = q.bestIndex();
                        bestBw = b;
                    }
                }

                dp[i] = bestValue;
                prev[i] = bestPrev;
                prevBitWidth[i] = bestBw;
                if (bestValue < INF) {
                    for (int b = 1; b <= 64; b++) {
                        queues[b].add(i, bestValue - pack_size * b * i);
                    }
                }
            }

            if (dp[N] < minTotalCost) {
                minTotalCost = dp[N];
                bestC = C;
                int[] reverseSizes = new int[N];
                int[] reverseBitWidths = new int[N];
                int count = 0;
                int i = N;
                while (i > 0 && prev[i] >= 0) {
                    int prevI = prev[i];
                    reverseSizes[count] = i - prevI;
                    reverseBitWidths[count] = prevBitWidth[i];
                    count++;
                    i = prevI;
                }
                if (i == 0) {
                    bestGroupSizes = new int[count];
                    bestGroupBitWidths = new int[count];
                    for (int g = 0; g < count; g++) {
                        bestGroupSizes[g] = reverseSizes[count - 1 - g];
                        bestGroupBitWidths[g] = reverseBitWidths[count - 1 - g];
                    }
                }
            }
        }

        return new PackingPlan(computeMinValidC(bestGroupSizes), bestGroupSizes, bestGroupBitWidths, minTotalCost);
    }

    /** Map logical bit width (1..64) to 6-bit on-wire field (0 means 64). */
    private static int encodeBitWidthField(int bitWidth) {
        if (bitWidth <= 0 || bitWidth > 64) {
            throw new IllegalArgumentException("Invalid bit width: " + bitWidth);
        }
        return bitWidth == 64 ? 0 : bitWidth;
    }

    /** Decode 6-bit on-wire bit-width field back to logical width (1..64). */
    private static int decodeBitWidthField(int stored) {
        if (stored < 0 || stored > 63) {
            throw new IllegalArgumentException("Invalid stored bit width: " + stored);
        }
        return stored == 0 ? 64 : stored;
    }

    /** Minimum C (bits per group-size field) that can encode all group sizes in the plan. */
    public static int computeMinValidC(int[] groupSizes) {
        if (groupSizes == null || groupSizes.length == 0) {
            return 1;
        }
        int maxGroup = 1;
        for (int size : groupSizes) {
            maxGroup = Math.max(maxGroup, size);
        }
        int maxPossibleC = 32 - Integer.numberOfLeadingZeros(maxGroup);
        for (int c = 1; c <= maxPossibleC; c++) {
            if (maxGroup <= ((1 << c) - 1)) {
                return c;
            }
        }
        return maxPossibleC;
    }

    /** Encoded size in bits for the optimal DP plan (same format as {@link #compressWithOptimalPacking}). */
    public static int computeOptimalEncodedBitCost(long[] paddedArray, int[] bitWidths, int pack_size) {
        int[] encodePos = new int[1];
        compressWithOptimalPacking(paddedArray, bitWidths, pack_size, encodePos);
        return encodePos[0] * 8;
    }

    /** Byte size for a plan without writing a bitstream (matches {@link #compressWithPackingPlan}). */
    public static int computeEncodedByteSizeForPlan(PackingPlan plan, int pack_size) {
        return calculateTotalCompressedSize(plan, pack_size);
    }

    /** Byte size for optimal DP grouping without writing a bitstream. */
    public static int computeOptimalEncodedByteSize(long[] paddedArray, int[] bitWidths, int pack_size) {
        if (bitWidths == null || bitWidths.length == 0) {
            return 0;
        }
        PackingPlan plan = computeOptimalPackingPlan(bitWidths, pack_size);
        return calculateTotalCompressedSize(plan, pack_size);
    }

    static final int[] EXACT_ORACLE_PACK_SIZES = {1, 2, 4, 8, 16, 32, 64, 128, 256, 512};

    static class ExactOracleCompressionResult {
        byte[] compressedData;
        long encodedBits;
        int packSize;
        int paddedLength;
        boolean vanilla;
        int[] vanillaGroupBitWidths;
    }

    static ExactOracleCompressionResult compressWithBestExactPacking(long[] values) {
        ExactOracleCompressionResult best = null;
        for (int packSize : EXACT_ORACLE_PACK_SIZES) {
            if (packSize <= 0) {
                continue;
            }
            int remainder = values.length % packSize;
            int paddingLength = remainder == 0 ? 0 : packSize - remainder;
            long[] padded = new long[values.length + paddingLength];
            System.arraycopy(values, 0, padded, 0, values.length);
            int groupCount = padded.length / packSize;
            int[] bitWidths = new int[groupCount];
            for (int g = 0; g < groupCount; g++) {
                bitWidths[g] = BP.groupBitWidthForPack(padded, g * packSize, packSize);
            }

            int[] encodePos = new int[1];
            byte[] encoded = compressWithOptimalPacking(padded, bitWidths, packSize, encodePos);
            long encodedBits = encodePos[0] * 8L;
            if (best == null || encodedBits < best.encodedBits) {
                best = new ExactOracleCompressionResult();
                best.compressedData = encoded;
                best.encodedBits = encodedBits;
                best.packSize = packSize;
                best.paddedLength = padded.length;
                best.vanilla = false;
            }

            BP.VanillaPackLayout vanillaLayout = BP.layoutVanillaFixedPack(values, packSize);
            byte[] vanillaEncoded = BP.encodeVanillaFixedPack(vanillaLayout);
            long vanillaBits = vanillaEncoded.length * 8L;
            if (vanillaBits < best.encodedBits) {
                best = new ExactOracleCompressionResult();
                best.compressedData = vanillaEncoded;
                best.encodedBits = vanillaBits;
                best.packSize = packSize;
                best.paddedLength = vanillaLayout.paddedValues.length;
                best.vanilla = true;
                best.vanillaGroupBitWidths = vanillaLayout.groupBitWidths;
            }
        }
        return best;
    }

    static long[] decompressExactOracle(ExactOracleCompressionResult result) {
        if (result == null || result.compressedData == null) {
            return new long[0];
        }
        if (result.vanilla) {
            return BP.decodeBitPacking(
                    result.compressedData, result.vanillaGroupBitWidths, result.packSize, result.paddedLength);
        }
        return decompressWithOptimalPacking(result.compressedData, result.paddedLength, result.packSize);
    }

    /**
     * Exact on-wire bit count for a packing plan at {@code pack_size} (matches
     * {@link #compressWithPackingPlan} before byte alignment).
     */
    public static int computeBitstreamBitCost(PackingPlan plan, int pack_size) {
        if (plan == null || plan.groupSizes == null || plan.groupSizes.length == 0) {
            return 0;
        }
        int bits = 6 + 32;
        for (int gi = 0; gi < plan.groupSizes.length; ++gi) {
            bits += plan.optimalC + 6;
            bits += plan.groupSizes[gi] * pack_size * plan.groupBitWidths[gi];
        }
        return bits;
    }

    /**
     * DP objective cost used inside {@link #computeOptimalPackingPlan} for a fixed
     * {@code plan.optimalC} (excludes the 6+32 bit stream header).
     */
    public static int computeDpObjectiveBitCost(PackingPlan plan, int pack_size) {
        if (plan == null || plan.groupSizes == null || plan.groupSizes.length == 0) {
            return 0;
        }
        int cost = 0;
        for (int gi = 0; gi < plan.groupSizes.length; ++gi) {
            cost += pack_size * plan.groupSizes[gi] * plan.groupBitWidths[gi];
            cost += 6 + plan.optimalC;
        }
        return cost;
    }

    /** Encoded size in bits for an explicit plan (bytes written × 8, p=1 value-level). */
    public static int computeEncodedBitCostForPlan(
            long[] paddedArray, PackingPlan plan, int pack_size) {
        int[] encodePos = new int[1];
        compressWithPackingPlan(paddedArray, plan, pack_size, encodePos);
        return encodePos[0] * 8;
    }

    // 计算压缩后数据总大小
    private static int calculateTotalCompressedSize(PackingPlan plan, int pack_size) {
        int totalSize = 0;

        // 元数据头大小: C(6bits) + 分组数量(32bits)
        totalSize += 6; // C占用6bits
        totalSize += 32; // 分组数量占用32bits

        // 每个分组的元数据: packsize(plan.optimalC bits) + 位宽(6bits)
        int groupMetadataBits = plan.groupSizes.length * (plan.optimalC + 6);
        totalSize += groupMetadataBits;

        // 压缩数据大小
        for (int i = 0; i < plan.groupSizes.length; i++) {
            int groupBlocks = plan.groupSizes[i];
            int bitWidth = plan.groupBitWidths[i];
            int dataBits = groupBlocks * pack_size * bitWidth;
            totalSize += dataBits;
        }

        // 转换为字节数（向上取整）
        return (totalSize + 7) / 8;
    }

    // 写入元数据头信息
    private static int writeMetadataHeader(byte[] compressedData, int currentPos,
                                           PackingPlan plan, int pack_size) {
        int bitPos = 0;
        int bytePos = currentPos;

        // 写入C (6 bits)
        writeBits(compressedData, bytePos, bitPos, plan.optimalC, 6);
        bitPos += 6;
        if (bitPos >= 8) {
            bytePos += bitPos / 8;
            bitPos = bitPos % 8;
        }

        // 写入分组数量 (32 bits)
        writeBits(compressedData, bytePos, bitPos, plan.groupSizes.length, 32);
        bytePos += 4; // 32 bits = 4 bytes

        return bytePos;
    }

    // 压缩数据块 - 修复版本，支持任意packsize
    private static int compressDataBlocks(byte[] compressedData, int currentPos,
                                          long[] paddedArray, PackingPlan plan, int pack_size) {
        int bitPos = 0;
        int bytePos = currentPos;
        int dataIndex = 0;

        for (int groupIdx = 0; groupIdx < plan.groupSizes.length; groupIdx++) {
            int groupBlocks = plan.groupSizes[groupIdx];
            int bitWidth = plan.groupBitWidths[groupIdx];

            // 写入当前分组的packsize (optimalC bits)
            writeBits(compressedData, bytePos, bitPos, groupBlocks, plan.optimalC);
            bitPos += plan.optimalC;
            if (bitPos >= 8) {
                bytePos += bitPos / 8;
                bitPos = bitPos % 8;
            }

            // 写入当前分组的位宽 (6 bits)
            writeBits(compressedData, bytePos, bitPos, encodeBitWidthField(bitWidth), 6);
            bitPos += 6;
            if (bitPos >= 8) {
                bytePos += bitPos / 8;
                bitPos = bitPos % 8;
            }

            // 压缩当前分组的数据
            int groupDataCount = groupBlocks * pack_size;
            int[] dataToPack = new int[groupDataCount];
            for (int i = 0; i < groupDataCount; i++) {
                dataToPack[i] = (int) paddedArray[dataIndex++]; // 注意：这里假设数据在int范围内
            }

            // 使用bitpacking压缩 - 修复版本，支持任意数量的数据
            bytePos = bitPacking(dataToPack, 0, bitWidth, bytePos, compressedData, bitPos);
            bitPos = 0; // bitPacking会处理位对齐
        }

        return bytePos;
    }

    // 写入指定位数的辅助函数
    private static void writeBits(byte[] data, int bytePos, int bitPos, int value, int numBits) {
        for (int i = numBits - 1; i >= 0; i--) {
            int bit = (value >> i) & 1;
            data[bytePos] |= (bit << (7 - bitPos));
            bitPos++;
            if (bitPos == 8) {
                bytePos++;
                bitPos = 0;
            }
        }
    }

    // 修复后的bitPacking方法，支持任意数量的数据（不只是8的倍数）
    public static int bitPacking(int[] numbers, int start, int bit_width,
                                 int encode_pos, byte[] encoded_result, int startBitPos) {
        int totalCount = numbers.length - start;
        int currentBytePos = encode_pos;
        int currentBitPos = startBitPos;

        // 处理所有数据，不只是8的倍数
        for (int i = 0; i < totalCount; i++) {
            int value = numbers[start + i];

            // 按位写入
            for (int bit = bit_width - 1; bit >= 0; bit--) {
                int currentBit = (value >> bit) & 1;
                encoded_result[currentBytePos] |= (currentBit << (7 - currentBitPos));
                currentBitPos++;

                if (currentBitPos == 8) {
                    currentBytePos++;
                    currentBitPos = 0;
                }
            }
        }

        return currentBytePos;
    }


    // 读取指定位数的辅助函数
    private static int readBits(byte[] data, int bytePos, int bitPos, int numBits) {
        int result = 0;
        for (int i = 0; i < numBits; i++) {
            int bit = (data[bytePos] >> (7 - bitPos)) & 1;
            result = (result << 1) | bit;
            bitPos++;
            if (bitPos == 8) {
                bytePos++;
                bitPos = 0;
            }
        }
        return result;
    }

    // 辅助：位写入器
    static class BitWriter {
        final byte[] data;
        int bytePos = 0;
        int bitPos = 0; // 0..7, next bit to write in current byte (7 - bitPos is mask shift)

        BitWriter(byte[] data, int startBytePos) { this.data = data; this.bytePos = startBytePos; }

        // 写 numBits 位（numBits <= 64），value 的低 numBits 位被写出（big-endian bit order）
        void writeBits(long value, int numBits) {
            if (numBits < 0 || numBits > 64) {
                throw new IllegalArgumentException("numBits must be in [0,64]: " + numBits);
            }
            while (numBits > 0) {
                int freeBits = 8 - bitPos;
                int take = Math.min(freeBits, numBits);
                int shift = numBits - take;
                long mask = take == 64 ? ~0L : ((1L << take) - 1L);
                int chunk = (int) ((value >>> shift) & mask);
                data[bytePos] |= (byte) (chunk << (freeBits - take));
                bitPos += take;
                if (bitPos == 8) {
                    bitPos = 0;
                    bytePos++;
                }
                numBits -= take;
            }
        }

        int getBytePos() { return bytePos; }
        int getBitPos() { return bitPos; }
        // 返回写入结束后占用的字节数（包括部分字节）
        int bytesWritten() {
            return bytePos + (bitPos > 0 ? 1 : 0);
        }
    }

    // 辅助：位读取器
    static class BitReader {
        final byte[] data;
        int bytePos = 0;
        int bitPos = 0;

        BitReader(byte[] data, int startBytePos, int startBitPos) {
            this.data = data;
            this.bytePos = startBytePos;
            this.bitPos = startBitPos;
        }

        // 读 numBits 位并作为 long 返回（numBits <= 64）
        long readBits(int numBits) {
            if (numBits < 0 || numBits > 64) {
                throw new IllegalArgumentException("numBits must be in [0,64]: " + numBits);
            }
            long res = 0L;
            while (numBits > 0) {
                int availableBits = 8 - bitPos;
                int take = Math.min(availableBits, numBits);
                int shift = availableBits - take;
                int mask = (1 << take) - 1;
                int chunk = ((data[bytePos] & 0xFF) >>> shift) & mask;
                res = (res << take) | chunk;
                bitPos += take;
                if (bitPos == 8) {
                    bitPos = 0;
                    bytePos++;
                }
                numBits -= take;
            }
            return res;
        }

        int getBytePos() { return bytePos; }
        int getBitPos() { return bitPos; }
    }

    // ---- 用新的 BitWriter/BitReader 来实现压缩/解压 ----
    public static byte[] compressWithOptimalPacking(long[] paddedArray, int[] bitWidths, int pack_size, int[] encodePos) {
        PackingPlan optimalPlan = computeOptimalPackingPlan(bitWidths, pack_size);
        return compressWithPackingPlan(paddedArray, optimalPlan, pack_size, encodePos);
    }

    /** Encode using an explicit grouping plan (DP or RL); bit layout identical to optimal DP compression. */
    public static byte[] compressWithPackingPlan(
            long[] paddedArray, PackingPlan plan, int pack_size, int[] encodePos) {
        int plannedValues = 0;
        for (int groupBlocks : plan.groupSizes) {
            plannedValues += groupBlocks * pack_size;
        }
        if (plannedValues != paddedArray.length) {
            throw new IllegalArgumentException(
                    "Packing plan value count mismatch: plan=" + plannedValues + " data=" + paddedArray.length
                            + " groups=" + plan.groupSizes.length);
        }

        int totalSize = calculateTotalCompressedSize(plan, pack_size);
        byte[] compressedData = new byte[totalSize];
        BitWriter writer = new BitWriter(compressedData, 0);

        writer.writeBits(plan.optimalC, 6);
        writer.writeBits(plan.groupSizes.length, 32);

        int dataIndex = 0;
        for (int gi = 0; gi < plan.groupSizes.length; ++gi) {
            int groupBlocks = plan.groupSizes[gi];
            int bitWidth = plan.groupBitWidths[gi];
            writer.writeBits(groupBlocks, plan.optimalC);
            writer.writeBits(encodeBitWidthField(bitWidth), 6);

            int groupDataCount = groupBlocks * pack_size;
            for (int i = 0; i < groupDataCount; ++i) {
                long v = paddedArray[dataIndex++];
                long mask = (bitWidth == 64) ? ~0L : ((1L << bitWidth) - 1L);
                writer.writeBits(v & mask, bitWidth);
            }
        }

        int finalBytes = writer.bytesWritten();
        int actualBits = writer.getBytePos() * 8 + writer.getBitPos();
        int expectedBits = 6 + 32;
        for (int gi = 0; gi < plan.groupSizes.length; ++gi) {
            expectedBits += plan.optimalC + 6;
            expectedBits += plan.groupSizes[gi] * pack_size * plan.groupBitWidths[gi];
        }
        if (actualBits != expectedBits) {
            throw new IllegalStateException(
                    "Bitstream length mismatch: wrote " + actualBits + " expected " + expectedBits);
        }
        if (finalBytes > totalSize) {
            throw new IllegalStateException(String.format(
                    "calculateTotalCompressedSize underestimated bytes: estimated=%d actual=%d.",
                    totalSize, finalBytes));
        }
        if (finalBytes == compressedData.length) {
            encodePos[0] = finalBytes;
            return compressedData;
        }
        byte[] out = new byte[finalBytes];
        System.arraycopy(compressedData, 0, out, 0, finalBytes);
        encodePos[0] = finalBytes;
        return out;
    }

    public static long[] decompressWithOptimalPacking(byte[] compressedData, int originalLength, int pack_size) {
        BitReader reader = new BitReader(compressedData, 0, 0);
        int C = (int) reader.readBits(6);
        int groupCount = (int) reader.readBits(32);

        // sanity checks
        if (C <= 0 || C > 32) throw new IllegalArgumentException("Invalid C read from header: " + C);
        if (groupCount < 0 || groupCount > (1 << 20)) throw new IllegalArgumentException("Suspicious groupCount: " + groupCount);

        long[] result = new long[originalLength];
        int resultIndex = 0;

        for (int gi = 0; gi < groupCount; ++gi) {
            // 先确保还能读 group header
            long remainingBitsBeforeGroup = ((long)(compressedData.length - reader.getBytePos())) * 8L - reader.getBitPos();
            if (remainingBitsBeforeGroup < C + 6) {
                throw new IllegalArgumentException(String.format(
                        "Not enough bits for group header #%d: need %d but only %d remain (bytePos=%d bitPos=%d totalBytes=%d).",
                        gi, C + 6, remainingBitsBeforeGroup, reader.getBytePos(), reader.getBitPos(), compressedData.length));
            }

            int groupBlocks = (int) reader.readBits(C);
            int bitWidth = decodeBitWidthField((int) reader.readBits(6));
            if (bitWidth < 0 || bitWidth > 64) throw new IllegalArgumentException("Invalid bitWidth: " + bitWidth);

            long groupDataCount = (long) groupBlocks * (long) pack_size;
            if (groupDataCount < 0 || groupDataCount > (1L << 31)) {
                throw new IllegalArgumentException("Suspicious groupDataCount: " + groupDataCount);
            }

            long neededDataBits = groupDataCount * (long) bitWidth;
            long remainingBitsAfterHeader = ((long)(compressedData.length - reader.getBytePos())) * 8L - reader.getBitPos();
            if (neededDataBits > remainingBitsAfterHeader) {
                throw new IllegalArgumentException(String.format(
                        "Not enough bits for group #%d data: need %d bits but only %d available (bytePos=%d bitPos=%d).",
                        gi, neededDataBits, remainingBitsAfterHeader, reader.getBytePos(), reader.getBitPos()));
            }

            for (long i = 0; i < groupDataCount; ++i) {
                long v = reader.readBits(bitWidth);
                if (resultIndex < originalLength) result[resultIndex++] = v;
            }
        }

        return result;
    }

    // 8. 修复：sprintz编码解码
    public static long[] zigzag(long[] numbers) {
        if (numbers == null || numbers.length == 0) {
            return new long[0];
        }

        long[] result = new long[numbers.length];

        for (int i = 0; i < numbers.length; i++) {
            // ZigZag编码
            result[i] = (numbers[i] << 1) ^ (numbers[i] >> 31);
        }

        return result;
    }

    public static long[] zigzagDecode(long[] encodedData) {
        if (encodedData == null || encodedData.length == 0) {
            return new long[0];
        }

        long[] result = new long[encodedData.length+1];

        for (int i = 0; i < encodedData.length; i++) {
            // ZigZag解码
            long zigzag = encodedData[i];
            result[i] = (zigzag >>> 1) ^ -(zigzag & 1);
        }

        return result;
    }

    // 封装结果类
    public static class SprintzEncodedResult {
        private final long[] encodedData;  // 编码后的数据
        private final long firstValue;     // 第一个原始值

        public SprintzEncodedResult(long[] encodedData, long firstValue) {
            this.encodedData = encodedData;
            this.firstValue = firstValue;
        }

        public long[] getEncodedData() {
            return encodedData;
        }

        public long getFirstValue() {
            return firstValue;
        }


        @Override
        public String toString() {
            return String.format("EncodedResult{firstValue=%d, minDiff=%d, encodedData=%s}",
                    firstValue, java.util.Arrays.toString(encodedData));
        }
    }

    // 8. 修复：sprintz编码解码
    public static SprintzEncodedResult sprintz(long[] numbers) {
        if (numbers == null || numbers.length == 0) {
            return new SprintzEncodedResult(new long[0],  0);
        }

        long[] result = new long[numbers.length];

        for (int i = 1; i < numbers.length; i++) {
            long diff = numbers[i] - numbers[i-1];
            // ZigZag编码
            result[i-1] = (diff << 1) ^ (diff >> 31);
        }

        return new SprintzEncodedResult(result, numbers[0]);
    }

    public static long[] sprintzDecode(long[] encodedData, long firstValue) {
        if (encodedData == null || encodedData.length == 0) {
            return new long[0];
        }

        long[] result = new long[encodedData.length+1];
        result[0] = firstValue;

        for (int i = 0; i < encodedData.length; i++) {
            // ZigZag解码
            long zigzag = encodedData[i];
            long diff = (zigzag >>> 1) ^ -(zigzag & 1);
            result[i+1] = result[i] + diff;
        }

        return result;
    }

    // 封装结果类
    public static class TSDIFFEncodedResult {
        private final long[] encodedData;  // 编码后的数据
        private final long firstValue;     // 第一个原始值
        private final long minDiff;        // 最小差分值

        public TSDIFFEncodedResult(long[] encodedData, long firstValue, long minDiff) {
            this.encodedData = encodedData;
            this.firstValue = firstValue;
            this.minDiff = minDiff;
        }

        public long[] getEncodedData() {
            return encodedData;
        }

        public long getFirstValue() {
            return firstValue;
        }

        public long getMinDiff() {
            return minDiff;
        }


        @Override
        public String toString() {
            return String.format("EncodedResult{firstValue=%d, minDiff=%d, encodedData=%s}",
                    firstValue, java.util.Arrays.toString(encodedData));
        }
    }

    public static TSDIFFEncodedResult ts2diff(long[] numbers) {
        if (numbers == null || numbers.length == 0) {
            return new TSDIFFEncodedResult(new long[0],0,0);
        }

        long[] result = new long[numbers.length-1];

        // 第一个值保持不变
        long firstValue = numbers[0];
//        result[0] = numbers[0];

        // 计算差分并找到最小差分
        long minDiff = Long.MAX_VALUE;
        long[] diffs = new long[numbers.length - 1];

        for (int i = 1; i < numbers.length; i++) {
            long diff = numbers[i] - numbers[i - 1];
            diffs[i - 1] = diff;

            if (diff < minDiff) {
                minDiff = diff;
            }
        }

        // 如果数组长度大于1，处理差分值
        if (numbers.length > 1) {
            // 使用最小差分进行归一化处理
            for (int i = 1; i < numbers.length; i++) {
                long normalizedDiff = diffs[i - 1] - minDiff;
                // ZigZag编码
                result[i-1] = normalizedDiff; // << 1) ^ (normalizedDiff >> 31);
            }
        }

        return new TSDIFFEncodedResult(result, firstValue, minDiff);
    }
    public static long[] ts2diffDecode(long[] result, long firstValue, long minDiff) {
        if (result == null) {
            return new long[0];
        }

        long[] numbers = new long[result.length + 1];
        numbers[0] = firstValue;

        for (int i = 1; i < numbers.length; i++) {
            long normalizedDiff = result[i - 1];
            // 还原原始差分
            long diff = normalizedDiff + minDiff;

            // 累加得到原始值
            numbers[i] = numbers[i - 1] + diff;
        }

        return numbers;
    }

    private static long[] scaleNumbers(List<String> numbers, int decimalMax) {
        // 1. 预先计算缩放因子
        BigDecimal scale = BigDecimal.TEN.pow(decimalMax);
        int size = numbers.size();
        long[] result = new long[size];

        if (size == 0) {
            return result;
        }

        // 2. 直接缩放所有数值
        for (int i = 0; i < size; i++) {
            // 缩放并转换为long
            BigDecimal scaledVal = new BigDecimal(numbers.get(i)).multiply(scale);
            result[i] = scaledVal.longValue();
        }

        return result;
    }

    @Test
    public void BPDP0() throws IOException {
        // 示例数据（实际应替换为真实时间序列）
        System.out.println("\nPerformance Testing with All Pack Sizes (1-16)...");
        String directory = "benchmark_data/ElfTestData_camel";
        String outputDirstr = "benchmark_data/output_BPDP";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
            System.out.println(file.getName());
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Compression Throughput",
                    "Decompression Throughput",
                    "Points",
                    "Compressed Size",
                    "Pack Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file
            System.out.println("Processing " + file.getName() + "...");
            List<String> numbers = new ArrayList<>();
            List<Integer> decimalPlaces = new ArrayList<>();
            CsvReader csvReader = new CsvReader(file.getPath(), ',', StandardCharsets.UTF_8);
            while (csvReader.readRecord()) {
                for (String value : csvReader.getValues()) {
                    String numStr = value.trim();
                    if (!numStr.isEmpty()) {
                        numbers.add(numStr);
                        int decimal = 0;
                        if (numStr.contains(".")) {
                            String[] parts = numStr.split("\\.");
                            decimal = parts[1].length();
                        }
                        decimalPlaces.add(decimal);
                    }
                }
            }
            int time_of_repeat = 10;

            for(int pack_size_exp = 0; pack_size_exp < 1; pack_size_exp++) {
                int pack_size = (int) Math.pow(2, pack_size_exp);

                BigDecimal modelCost = BigDecimal.ZERO;
                BigDecimal encodeTime = BigDecimal.ZERO;
                BigDecimal decodeTime = BigDecimal.ZERO;
                for (int j = 0; j < time_of_repeat; j++) {
                    for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {

                        List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                        if (chunkNumbers.size() == 1 || chunkNumbers.size() == 2)
                            continue;

                        int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                                .stream().max(Integer::compare).orElse(0);

                        long[] scaledInts = scaleNumbers(chunkNumbers, decimalMax);

                        long startEncodeTime = System.nanoTime();
                        int remainder = scaledInts.length % pack_size;
                        int paddingLength = (remainder == 0) ? 0 : pack_size - remainder;

                        // 创建新数组，长度补齐为pack_size的倍数
                        long[] paddedArray = new long[scaledInts.length + paddingLength];
                        System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                        int actual_length = paddedArray.length;
                        int[] bitWidths = new int[actual_length / pack_size]; // 存储每pack_size个值的位宽结果

                        for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += pack_size) {
                                bitWidths[scaledInts_i / pack_size] =
                                        BP.groupBitWidthForPack(paddedArray, scaledInts_i, pack_size);
                        }
                        int[] encodePos = new int[1];
                        byte[] compressedData = compressWithOptimalPacking(paddedArray, bitWidths, pack_size, encodePos);
                        long encodeDuration = System.nanoTime() - startEncodeTime;

                        // 解压测试
                        long startDecodeTime = System.nanoTime();
                        long[] decompressedData = decompressWithOptimalPacking(compressedData, paddedArray.length, pack_size);
                        long decodeDuration = System.nanoTime() - startDecodeTime;

                        // 验证解压数据的正确性（可选）
                        // for (int idx = 0; idx < scaledInts.length; idx++) {
                        //     if (paddedArray[idx] != decompressedData[idx]) {
                        //         System.err.println("Decompression error at index " + idx);
                        //     }
                        // }

                        BigDecimal cur_cost = BigDecimal.valueOf(encodePos[0] * 8L);
                        encodeTime = encodeTime.add(BigDecimal.valueOf(encodeDuration));
                        decodeTime = decodeTime.add(BigDecimal.valueOf(decodeDuration));
                        modelCost = modelCost.add(cur_cost);
                    }

                }
                BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                encodeTime = encodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                decodeTime = decodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP);

                // 计算编码吞吐量 (MB/s): 数据量(bytes) / 时间(s)
                // 原始数据大小: numbers.size() * 8 bytes (因为long是8字节)
                BigDecimal originalSizeBytes = numbersSizeBD.multiply(BigDecimal.valueOf(8));
                BigDecimal encodeTimeSeconds = encodeTime.divide(BigDecimal.valueOf(1_000_000_000), 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal encodeThroughput = originalSizeBytes.divide(encodeTimeSeconds, 10, BigDecimal.ROUND_HALF_UP)
                        .divide(BigDecimal.valueOf(1024 * 1024), 10, BigDecimal.ROUND_HALF_UP); // 转换为MB/s

                // 计算解码吞吐量
                BigDecimal decodeTimeSeconds = decodeTime.divide(BigDecimal.valueOf(1_000_000_000), 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal decodeThroughput = originalSizeBytes.divide(decodeTimeSeconds, 10, BigDecimal.ROUND_HALF_UP)
                        .divide(BigDecimal.valueOf(1024 * 1024), 10, BigDecimal.ROUND_HALF_UP); // 转换为MB/s

                String[] record = {
                        file.toString(),
                        "BP-DP-AllPackSize",
                        encodeThroughput.toPlainString(),
                        decodeThroughput.toPlainString(),
                        String.valueOf(numbers.size()),
                        modelCost.toPlainString(),
                        String.valueOf(pack_size),
                        model_ratio.toPlainString()
                };
                writer.writeRecord(record);
            }
            writer.close();
        }

    }

    @Test
    public void TestAllPackSize() throws IOException {
        // 示例数据（实际应替换为真实时间序列）
        System.out.println("\nPerformance Testing with All Pack Sizes (1-16)...");
        String directory = "benchmark_data/ElfTestData_camel";
        String outputDirstr = "benchmark_data/output_BPDP_all_pack_size";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
            System.out.println(file.getName());
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Compression Throughput",
                    "Decompression Throughput",
                    "Points",
                    "Compressed Size",
                    "Pack Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file
            System.out.println("Processing " + file.getName() + "...");
            List<String> numbers = new ArrayList<>();
            List<Integer> decimalPlaces = new ArrayList<>();
            CsvReader csvReader = new CsvReader(file.getPath(), ',', StandardCharsets.UTF_8);
            while (csvReader.readRecord()) {
                for (String value : csvReader.getValues()) {
                    String numStr = value.trim();
                    if (!numStr.isEmpty()) {
                        numbers.add(numStr);
                        int decimal = 0;
                        if (numStr.contains(".")) {
                            String[] parts = numStr.split("\\.");
                            decimal = parts[1].length();
                        }
                        decimalPlaces.add(decimal);
                    }
                }
            }
            int time_of_repeat = all_time_of_repeat;

            for(int pack_size_exp = 0; pack_size_exp < all_max_pack_size; pack_size_exp++) {
                int pack_size = (int) Math.pow(2, pack_size_exp);

                BigDecimal modelCost = BigDecimal.ZERO;
                BigDecimal encodeTime = BigDecimal.ZERO;
                BigDecimal decodeTime = BigDecimal.ZERO;
                for (int j = 0; j < time_of_repeat; j++) {
                    for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {

                        List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                        if (chunkNumbers.size() == 1 || chunkNumbers.size() == 2)
                            continue;

                        int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                                .stream().max(Integer::compare).orElse(0);

                        long[] scaledInts = scaleNumbers(chunkNumbers, decimalMax);

                        long startEncodeTime = System.nanoTime();
                        int remainder = scaledInts.length % pack_size;
                        int paddingLength = (remainder == 0) ? 0 : pack_size - remainder;

                        // 创建新数组，长度补齐为pack_size的倍数
                        long[] paddedArray = new long[scaledInts.length + paddingLength];
                        System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                        int actual_length = paddedArray.length;
                        int[] bitWidths = new int[actual_length / pack_size]; // 存储每pack_size个值的位宽结果

                        for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += pack_size) {
                                bitWidths[scaledInts_i / pack_size] =
                                        BP.groupBitWidthForPack(paddedArray, scaledInts_i, pack_size);
                        }
                        int[] encodePos = new int[1];
                        byte[] compressedData = compressWithOptimalPacking(paddedArray, bitWidths, pack_size, encodePos);
                        long encodeDuration = System.nanoTime() - startEncodeTime;

                        // 解压测试
                        long startDecodeTime = System.nanoTime();
                        long[] decompressedData = decompressWithOptimalPacking(compressedData, paddedArray.length, pack_size);
                        long decodeDuration = System.nanoTime() - startDecodeTime;

                        BigDecimal cur_cost = BigDecimal.valueOf(encodePos[0] * 8L);
                        encodeTime = encodeTime.add(BigDecimal.valueOf(encodeDuration));
                        decodeTime = decodeTime.add(BigDecimal.valueOf(decodeDuration));
                        modelCost = modelCost.add(cur_cost);
                    }

                }
                BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                encodeTime = encodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                decodeTime = decodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP);

                // 计算编码吞吐量 (MB/s)
                BigDecimal originalSizeBytes = numbersSizeBD.multiply(BigDecimal.valueOf(8));
                BigDecimal encodeTimeSeconds = encodeTime.divide(BigDecimal.valueOf(1_000_000_000), 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal encodeThroughput = originalSizeBytes.divide(encodeTimeSeconds, 10, BigDecimal.ROUND_HALF_UP)
                        .divide(BigDecimal.valueOf(1024 * 1024), 10, BigDecimal.ROUND_HALF_UP);

                // 计算解码吞吐量
                BigDecimal decodeTimeSeconds = decodeTime.divide(BigDecimal.valueOf(1_000_000_000), 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal decodeThroughput = originalSizeBytes.divide(decodeTimeSeconds, 10, BigDecimal.ROUND_HALF_UP)
                        .divide(BigDecimal.valueOf(1024 * 1024), 10, BigDecimal.ROUND_HALF_UP);

                String[] record = {
                        file.toString(),
                        "BP-DP-AllPackSize",
                        encodeThroughput.toPlainString(),
                        decodeThroughput.toPlainString(),
                        String.valueOf(numbers.size()),
                        modelCost.toPlainString(),
                        String.valueOf(pack_size),
                        model_ratio.toPlainString()
                };
                writer.writeRecord(record);
            }
            writer.close();
        }

    }

    // 新增方法：测试不同chunk size的表现
    @Test
    public void TestVariableChunkSize() throws IOException {
        System.out.println("\nPerformance Testing with Variable Chunk Sizes...");
        String directory = "benchmark_data/ElfTestData_camel";
        String outputDirstr = "benchmark_data/output_BPDP_vary_m";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);

        // 定义要测试的chunk sizes (m*8 where m is 16, 32, 64, 128, 256, 512, 1024)
        int[] chunkSizes = {16*8, 32*8, 64*8, 128*8, 256*8, 512*8}; //, 1024*8

        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
            System.out.println("Processing " + file.getName() + " with variable chunk sizes...");
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "m",
                    "Input Direction",
                    "Encoding Algorithm",
                    "Compression Throughput",
                    "Decompression Throughput",
                    "Points",
                    "Compressed Size",
                    "Pack Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head);

            List<String> numbers = new ArrayList<>();
            List<Integer> decimalPlaces = new ArrayList<>();
            CsvReader csvReader = new CsvReader(file.getPath(), ',', StandardCharsets.UTF_8);
            while (csvReader.readRecord()) {
                for (String value : csvReader.getValues()) {
                    String numStr = value.trim();
                    if (!numStr.isEmpty()) {
                        numbers.add(numStr);
                        int decimal = 0;
                        if (numStr.contains(".")) {
                            String[] parts = numStr.split("\\.");
                            decimal = parts[1].length();
                        }
                        decimalPlaces.add(decimal);
                    }
                }
            }

            int time_of_repeat = 1;
            int decimalMax = decimalPlaces.stream().max(Integer::compare).orElse(0);

            int batchSize = 1024;
            List<long[]> batches = new ArrayList<>();

            for (int i = 0; i < numbers.size(); i += batchSize) {
                int end = Math.min(numbers.size(), i + batchSize);
                List<String> batch = numbers.subList(i, end);
                long[] scaledBatch = scaleNumbers(batch, decimalMax);
                batches.add(scaledBatch);
            }

            // 计算总长度并拼接所有批次的结果
            int totalLength = batches.stream().mapToInt(arr -> arr.length).sum();
            long[] scaledInts_all = new long[totalLength];

            int currentIndex = 0;
            for (long[] batch : batches) {
                System.arraycopy(batch, 0, scaledInts_all, currentIndex, batch.length);
                currentIndex += batch.length;
            }
            // 测试每个chunk size
            for (int chunkSize : chunkSizes) {
                System.out.println("Testing chunk size: " + chunkSize);

                for(int pack_size_exp = 0; pack_size_exp < 1; pack_size_exp++) {
                    int pack_size = (int) Math.pow(2, pack_size_exp);
                    BigDecimal modelCost = BigDecimal.ZERO;
                    BigDecimal modelTime = BigDecimal.ZERO;
                    BigDecimal modelDecodeTime = BigDecimal.ZERO;

                    for (int j = 0; j < time_of_repeat; j++) {
                        int totalCost = 0;
                        for (int i = 0; i < numbers.size(); i += chunkSize) {
                            int end = Math.min(i + chunkSize, numbers.size());
                            long[] scaledInts= new long[end-i];
                            if (end - i >= 0) System.arraycopy(scaledInts_all, i, scaledInts, 0, end - i);

                            long startTime = System.nanoTime();
//                            long[] scaledInts = sprintz(scaledInt);
                            ExactOracleCompressionResult res = compressWithBestExactPacking(scaledInts);
                            pack_size = res.packSize;
                            BigDecimal cur_cost = BigDecimal.valueOf(res.encodedBits);
                            long duration = System.nanoTime() - startTime;
                            modelTime = modelTime.add(BigDecimal.valueOf(duration));
                            modelCost = modelCost.add(cur_cost);

                            long decodeStart = System.nanoTime();
                            decompressExactOracle(res);
                            long decodeDuration = System.nanoTime() - decodeStart;
                            modelDecodeTime = modelDecodeTime.add(BigDecimal.valueOf(decodeDuration));
                        }
                    }

                    BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                    modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                    modelTime = modelTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                    modelDecodeTime = modelDecodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                    BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                    BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP);
                    BigDecimal modelTime_throughput = numbersSizeBD.multiply(BigDecimal.valueOf(8000)).divide(modelTime, 10, BigDecimal.ROUND_HALF_UP);
                    BigDecimal decodeThroughput = BigDecimal.ZERO;
                    if (modelDecodeTime.compareTo(BigDecimal.ZERO) != 0) {
                        decodeThroughput =
                                numbersSizeBD.multiply(BigDecimal.valueOf(8000)).divide(modelDecodeTime, 10, BigDecimal.ROUND_HALF_UP);
                    }

                    String[] record = {
                            String.valueOf(chunkSize),
                            file.toString(),
                            "BP-DP",
                            String.valueOf(modelTime_throughput.doubleValue()),
                            decodeThroughput.toPlainString(),
                            String.valueOf(numbers.size()),
                            String.valueOf(modelCost),
                            String.valueOf(pack_size),
                            String.valueOf(model_ratio.doubleValue())
                    };
                    writer.writeRecord(record);
                }
            }
            writer.close();
        }
    }

    @Test
    public void ZigzagDP() throws IOException {
        // 示例数据（实际应替换为真实时间序列）
        System.out.println("\nPerformance Testing with All Pack Sizes (1-16)...");
        String directory = "benchmark_data/ElfTestData_camel";
        String outputDirstr = "benchmark_data/output_DP_Zigzag";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
            System.out.println(file.getName());
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Compression Throughput",
                    "Decompression Throughput",
                    "Points",
                    "Compressed Size (bits)",
                    "Pack Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file
            System.out.println("Processing " + file.getName() + "...");
            List<String> numbers = new ArrayList<>();
            List<Integer> decimalPlaces = new ArrayList<>();
            CsvReader csvReader = new CsvReader(file.getPath(), ',', StandardCharsets.UTF_8);
            while (csvReader.readRecord()) {
                for (String value : csvReader.getValues()) {
                    String numStr = value.trim();
                    if (!numStr.isEmpty()) {
                        numbers.add(numStr);
                        int decimal = 0;
                        if (numStr.contains(".")) {
                            String[] parts = numStr.split("\\.");
                            decimal = parts[1].length();
                        }
                        decimalPlaces.add(decimal);
                    }
                }
            }
            int time_of_repeat = 10;

            for(int pack_size_exp = 0; pack_size_exp < 1; pack_size_exp++) {
                int pack_size = (int) Math.pow(2, pack_size_exp);

                BigDecimal modelCost = BigDecimal.ZERO;
                BigDecimal encodeTime = BigDecimal.ZERO;
                BigDecimal decodeTime = BigDecimal.ZERO;
                for (int j = 0; j < time_of_repeat; j++) {
                    for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {

                        List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                        if (chunkNumbers.size() == 1 || chunkNumbers.size() == 2)
                            continue;

                        int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                                .stream().max(Integer::compare).orElse(0);

                        long[] scaledInt = scaleNumbers(chunkNumbers, decimalMax);

                        long startEncodeTime = System.nanoTime();

                        long[] scaledInts = zigzag(scaledInt);
                        int remainder = scaledInts.length % pack_size;
                        int paddingLength = (remainder == 0) ? 0 : pack_size - remainder;

                        // 创建新数组，长度补齐为pack_size的倍数
                        long[] paddedArray = new long[scaledInts.length + paddingLength];
                        System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                        int actual_length = paddedArray.length;
                        int[] bitWidths = new int[actual_length / pack_size]; // 存储每pack_size个值的位宽结果

                        for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += pack_size) {
                                bitWidths[scaledInts_i / pack_size] =
                                        BP.groupBitWidthForPack(paddedArray, scaledInts_i, pack_size);
                        }
                        int[] encodePos = new int[1];
                        byte[] compressedData = compressWithOptimalPacking(paddedArray, bitWidths, pack_size, encodePos);
                        long encodeDuration = System.nanoTime() - startEncodeTime;

                        // 解压测试
                        long startDecodeTime = System.nanoTime();
                        long[] decompressedData = decompressWithOptimalPacking(compressedData, paddedArray.length, pack_size);
                        long[] result = zigzagDecode(decompressedData);
                        long decodeDuration = System.nanoTime() - startDecodeTime;

                        // 验证解压数据的正确性（可选）
                        // for (int idx = 0; idx < scaledInts.length; idx++) {
                        //     if (paddedArray[idx] != decompressedData[idx]) {
                        //         System.err.println("Decompression error at index " + idx);
                        //     }
                        // }

                        BigDecimal cur_cost = BigDecimal.valueOf(encodePos[0] * 8L);
                        encodeTime = encodeTime.add(BigDecimal.valueOf(encodeDuration));
                        decodeTime = decodeTime.add(BigDecimal.valueOf(decodeDuration));
                        modelCost = modelCost.add(cur_cost);
                    }

                }
                BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                encodeTime = encodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                decodeTime = decodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP);

                // 计算编码吞吐量 (MB/s): 数据量(bytes) / 时间(s)
                // 原始数据大小: numbers.size() * 8 bytes (因为long是8字节)
                BigDecimal originalSizeBytes = numbersSizeBD.multiply(BigDecimal.valueOf(8));
                BigDecimal encodeTimeSeconds = encodeTime.divide(BigDecimal.valueOf(1_000_000_000), 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal encodeThroughput = originalSizeBytes.divide(encodeTimeSeconds, 10, BigDecimal.ROUND_HALF_UP)
                        .divide(BigDecimal.valueOf(1024 * 1024), 10, BigDecimal.ROUND_HALF_UP); // 转换为MB/s

                // 计算解码吞吐量
                BigDecimal decodeTimeSeconds = decodeTime.divide(BigDecimal.valueOf(1_000_000_000), 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal decodeThroughput = originalSizeBytes.divide(decodeTimeSeconds, 10, BigDecimal.ROUND_HALF_UP)
                        .divide(BigDecimal.valueOf(1024 * 1024), 10, BigDecimal.ROUND_HALF_UP); // 转换为MB/s

                String[] record = {
                        file.toString(),
                        "BP-DP-AllPackSize",
                        encodeThroughput.toPlainString(),
                        decodeThroughput.toPlainString(),
                        String.valueOf(numbers.size()),
                        modelCost.toPlainString(),
                        String.valueOf(pack_size),
                        model_ratio.toPlainString()
                };
                writer.writeRecord(record);
            }
            writer.close();
        }

    }


    @Test
    public void ZigzagVarPackSize() throws IOException {
        // 示例数据（实际应替换为真实时间序列）
        System.out.println("\nPerformance Testing with All Pack Sizes (1-16)...");
        String directory = "benchmark_data/ElfTestData_camel";
        String outputDirstr = "benchmark_data/output_DP_Zigzag_all_pack_size";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
            System.out.println(file.getName());
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Compression Throughput",
                    "Decompression Throughput",
                    "Points",
                    "Compressed Size",
                    "Pack Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file
            System.out.println("Processing " + file.getName() + "...");
            List<String> numbers = new ArrayList<>();
            List<Integer> decimalPlaces = new ArrayList<>();
            CsvReader csvReader = new CsvReader(file.getPath(), ',', StandardCharsets.UTF_8);
            while (csvReader.readRecord()) {
                for (String value : csvReader.getValues()) {
                    String numStr = value.trim();
                    if (!numStr.isEmpty()) {
                        numbers.add(numStr);
                        int decimal = 0;
                        if (numStr.contains(".")) {
                            String[] parts = numStr.split("\\.");
                            decimal = parts[1].length();
                        }
                        decimalPlaces.add(decimal);
                    }
                }
            }
            int time_of_repeat = all_time_of_repeat;

            for(int pack_size_exp = 0; pack_size_exp < all_max_pack_size; pack_size_exp++) {
                int pack_size = (int) Math.pow(2, pack_size_exp);

                BigDecimal modelCost = BigDecimal.ZERO;
                BigDecimal encodeTime = BigDecimal.ZERO;
                BigDecimal decodeTime = BigDecimal.ZERO;
                for (int j = 0; j < time_of_repeat; j++) {
                    for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {

                        List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                        if (chunkNumbers.size() == 1 || chunkNumbers.size() == 2)
                            continue;

                        int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                                .stream().max(Integer::compare).orElse(0);

                        long[] scaledInt = scaleNumbers(chunkNumbers, decimalMax);

                        long startEncodeTime = System.nanoTime();

                        long[] scaledInts = zigzag(scaledInt);
                        int remainder = scaledInts.length % pack_size;
                        int paddingLength = (remainder == 0) ? 0 : pack_size - remainder;

                        // 创建新数组，长度补齐为pack_size的倍数
                        long[] paddedArray = new long[scaledInts.length + paddingLength];
                        System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                        int actual_length = paddedArray.length;
                        int[] bitWidths = new int[actual_length / pack_size]; // 存储每pack_size个值的位宽结果

                        for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += pack_size) {
                                bitWidths[scaledInts_i / pack_size] =
                                        BP.groupBitWidthForPack(paddedArray, scaledInts_i, pack_size);
                        }
                        int[] encodePos = new int[1];
                        byte[] compressedData = compressWithOptimalPacking(paddedArray, bitWidths, pack_size, encodePos);
                        long encodeDuration = System.nanoTime() - startEncodeTime;

                        // 解压测试
                        long startDecodeTime = System.nanoTime();
                        long[] decompressedData = decompressWithOptimalPacking(compressedData, paddedArray.length, pack_size);
                        long decodeDuration = System.nanoTime() - startDecodeTime;

                        BigDecimal cur_cost = BigDecimal.valueOf(encodePos[0] * 8L);
                        encodeTime = encodeTime.add(BigDecimal.valueOf(encodeDuration));
                        decodeTime = decodeTime.add(BigDecimal.valueOf(decodeDuration));
                        modelCost = modelCost.add(cur_cost);
                    }

                }
                BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                encodeTime = encodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                decodeTime = decodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP);

                // 计算编码吞吐量 (MB/s)
                BigDecimal originalSizeBytes = numbersSizeBD.multiply(BigDecimal.valueOf(8));
                BigDecimal encodeTimeSeconds = encodeTime.divide(BigDecimal.valueOf(1_000_000_000), 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal encodeThroughput = originalSizeBytes.divide(encodeTimeSeconds, 10, BigDecimal.ROUND_HALF_UP)
                        .divide(BigDecimal.valueOf(1024 * 1024), 10, BigDecimal.ROUND_HALF_UP);

                // 计算解码吞吐量
                BigDecimal decodeTimeSeconds = decodeTime.divide(BigDecimal.valueOf(1_000_000_000), 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal decodeThroughput = originalSizeBytes.divide(decodeTimeSeconds, 10, BigDecimal.ROUND_HALF_UP)
                        .divide(BigDecimal.valueOf(1024 * 1024), 10, BigDecimal.ROUND_HALF_UP);

                String[] record = {
                        file.toString(),
                        "BP-DP-AllPackSize",
                        encodeThroughput.toPlainString(),
                        decodeThroughput.toPlainString(),
                        String.valueOf(numbers.size()),
                        modelCost.toPlainString(),
                        String.valueOf(pack_size),
                        model_ratio.toPlainString()
                };
                writer.writeRecord(record);
            }
            writer.close();
        }

    }

    @Test
    public void ZigzagVarChunkSize() throws IOException {
        System.out.println("\nPerformance Testing with Variable Chunk Sizes...");
        String directory = "benchmark_data/ElfTestData_camel";
        String outputDirstr = "benchmark_data/output_DP_Zigzag_vary_m";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);

        // 定义要测试的chunk sizes (m*8 where m is 16, 32, 64, 128, 256, 512, 1024)
        int[] chunkSizes = {16*8, 32*8, 64*8, 128*8, 256*8, 512*8}; //, 1024*8

        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
            System.out.println("Processing " + file.getName() + " with variable chunk sizes...");
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "m",
                    "Input Direction",
                    "Encoding Algorithm",
                    "Compression Throughput",
                    "Decompression Throughput",
                    "Points",
                    "Compressed Size",
//                    "Pack Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head);

            List<String> numbers = new ArrayList<>();
            List<Integer> decimalPlaces = new ArrayList<>();
            CsvReader csvReader = new CsvReader(file.getPath(), ',', StandardCharsets.UTF_8);
            while (csvReader.readRecord()) {
                for (String value : csvReader.getValues()) {
                    String numStr = value.trim();
                    if (!numStr.isEmpty()) {
                        numbers.add(numStr);
                        int decimal = 0;
                        if (numStr.contains(".")) {
                            String[] parts = numStr.split("\\.");
                            decimal = parts[1].length();
                        }
                        decimalPlaces.add(decimal);
                    }
                }
            }

            int time_of_repeat = 1;
            int decimalMax = decimalPlaces.stream().max(Integer::compare).orElse(0);

            int batchSize = 1024;
            List<long[]> batches = new ArrayList<>();

            for (int i = 0; i < numbers.size(); i += batchSize) {
                int end = Math.min(numbers.size(), i + batchSize);
                List<String> batch = numbers.subList(i, end);
                long[] scaledBatch = scaleNumbers(batch, decimalMax);
                batches.add(scaledBatch);
            }

            // 计算总长度并拼接所有批次的结果
            int totalLength = batches.stream().mapToInt(arr -> arr.length).sum();
            long[] scaledInts_all = new long[totalLength];

            int currentIndex = 0;
            for (long[] batch : batches) {
                System.arraycopy(batch, 0, scaledInts_all, currentIndex, batch.length);
                currentIndex += batch.length;
            }
            // 测试每个chunk size
            for (int chunkSize : chunkSizes) {
                System.out.println("Testing chunk size: " + chunkSize);

                for(int pack_size_exp = 0; pack_size_exp < 1; pack_size_exp++) {
                    int pack_size = (int) Math.pow(2, pack_size_exp);
                    BigDecimal modelCost = BigDecimal.ZERO;
                    BigDecimal modelTime = BigDecimal.ZERO;
                    BigDecimal modelDecodeTime = BigDecimal.ZERO;


                    for (int j = 0; j < time_of_repeat; j++) {
                        int totalCost = 0;
                        for (int i = 0; i < numbers.size(); i += chunkSize) {
                            int end = Math.min(i + chunkSize, numbers.size());
                            long[] scaledInt= new long[end-i];
                            if (end - i >= 0) System.arraycopy(scaledInts_all, i, scaledInt, 0, end - i);

                            long startTime = System.nanoTime();
                            long[] scaledInts = zigzag(scaledInt);
                            ExactOracleCompressionResult res = compressWithBestExactPacking(scaledInts);
                            pack_size = res.packSize;
                            BigDecimal cur_cost = BigDecimal.valueOf(res.encodedBits);
                            long duration = System.nanoTime() - startTime;
                            modelTime = modelTime.add(BigDecimal.valueOf(duration));
                            modelCost = modelCost.add(cur_cost);

                            long decodeStart = System.nanoTime();
                            long[] decoded = decompressExactOracle(res);
                            zigzagDecode(decoded);
                            long decodeDuration = System.nanoTime() - decodeStart;
                            modelDecodeTime = modelDecodeTime.add(BigDecimal.valueOf(decodeDuration));
                        }
                    }

                    BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                    modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                    modelTime = modelTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                    modelDecodeTime = modelDecodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                    BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                    BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP);
                    BigDecimal modelTime_throughput = numbersSizeBD.multiply(BigDecimal.valueOf(8000)).divide(modelTime, 10, BigDecimal.ROUND_HALF_UP);
                    BigDecimal decodeThroughput = BigDecimal.ZERO;
                    if (modelDecodeTime.compareTo(BigDecimal.ZERO) != 0) {
                        decodeThroughput =
                                numbersSizeBD.multiply(BigDecimal.valueOf(8000)).divide(modelDecodeTime, 10, BigDecimal.ROUND_HALF_UP);
                    }

                    String[] record = {
                            String.valueOf(chunkSize),
                            file.toString(),
                            "Zigzag-DP",
                            String.valueOf(modelTime_throughput.doubleValue()),
                            decodeThroughput.toPlainString(),
                            String.valueOf(numbers.size()),
                            String.valueOf(modelCost),
//                            String.valueOf(pack_size),
                            String.valueOf(model_ratio.doubleValue())
                    };
                    writer.writeRecord(record);
                }
            }
            writer.close();
//            break;
        }
    }

    @Test
    public void SprintzDP() throws IOException {
        // 示例数据（实际应替换为真实时间序列）
        System.out.println("\nPerformance Testing with All Pack Sizes (1-16)...");
        String directory = "benchmark_data/ElfTestData_camel";
        String outputDirstr = "benchmark_data/output_DP_Sprintz";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
            System.out.println(file.getName());
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Compression Throughput",
                    "Decompression Throughput",
                    "Points",
                    "Compressed Size",
                    "Pack Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file
            System.out.println("Processing " + file.getName() + "...");
            List<String> numbers = new ArrayList<>();
            List<Integer> decimalPlaces = new ArrayList<>();
            CsvReader csvReader = new CsvReader(file.getPath(), ',', StandardCharsets.UTF_8);
            while (csvReader.readRecord()) {
                for (String value : csvReader.getValues()) {
                    String numStr = value.trim();
                    if (!numStr.isEmpty()) {
                        numbers.add(numStr);
                        int decimal = 0;
                        if (numStr.contains(".")) {
                            String[] parts = numStr.split("\\.");
                            decimal = parts[1].length();
                        }
                        decimalPlaces.add(decimal);
                    }
                }
            }
            int time_of_repeat = 10;

            for(int pack_size_exp = 0; pack_size_exp < 1; pack_size_exp++) {
                int pack_size = (int) Math.pow(2, pack_size_exp);

                BigDecimal modelCost = BigDecimal.ZERO;
                BigDecimal encodeTime = BigDecimal.ZERO;
                BigDecimal decodeTime = BigDecimal.ZERO;
                for (int j = 0; j < time_of_repeat; j++) {
                    for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {

                        List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                        if (chunkNumbers.size() == 1 || chunkNumbers.size() == 2)
                            continue;

                        int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                                .stream().max(Integer::compare).orElse(0);

                        long[] scaledInt = scaleNumbers(chunkNumbers, decimalMax);

                        long startEncodeTime = System.nanoTime();
                        SprintzEncodedResult ser = sprintz(scaledInt);
                        long[] scaledInts = ser.getEncodedData();

                        int remainder = scaledInts.length % pack_size;
                        int paddingLength = (remainder == 0) ? 0 : pack_size - remainder;

                        // 创建新数组，长度补齐为pack_size的倍数
                        long[] paddedArray = new long[scaledInts.length + paddingLength];
                        System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                        int actual_length = paddedArray.length;
                        int[] bitWidths = new int[actual_length / pack_size]; // 存储每pack_size个值的位宽结果

                        for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += pack_size) {
                                bitWidths[scaledInts_i / pack_size] =
                                        BP.groupBitWidthForPack(paddedArray, scaledInts_i, pack_size);
                        }
                        int[] encodePos = new int[1];
                        byte[] compressedData = compressWithOptimalPacking(paddedArray, bitWidths, pack_size, encodePos);
                        long encodeDuration = System.nanoTime() - startEncodeTime;

                        // 解压测试
                        long startDecodeTime = System.nanoTime();
                        long[] decodedData = decompressWithOptimalPacking(compressedData, paddedArray.length, pack_size);
                        long[] originalData = sprintzDecode(decodedData,ser.getFirstValue());
                        long decodeDuration = System.nanoTime() - startDecodeTime;

                        // 验证解压数据的正确性（可选）
                        // for (int idx = 0; idx < scaledInts.length; idx++) {
                        //     if (paddedArray[idx] != decompressedData[idx]) {
                        //         System.err.println("Decompression error at index " + idx);
                        //     }
                        // }

                        BigDecimal cur_cost = BigDecimal.valueOf(encodePos[0] * 8L);
                        encodeTime = encodeTime.add(BigDecimal.valueOf(encodeDuration));
                        decodeTime = decodeTime.add(BigDecimal.valueOf(decodeDuration));
                        modelCost = modelCost.add(cur_cost);
                    }

                }
                BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                encodeTime = encodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                decodeTime = decodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP);

                // 计算编码吞吐量 (MB/s): 数据量(bytes) / 时间(s)
                // 原始数据大小: numbers.size() * 8 bytes (因为long是8字节)
                BigDecimal originalSizeBytes = numbersSizeBD.multiply(BigDecimal.valueOf(8));
                BigDecimal encodeTimeSeconds = encodeTime.divide(BigDecimal.valueOf(1_000_000_000), 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal encodeThroughput = originalSizeBytes.divide(encodeTimeSeconds, 10, BigDecimal.ROUND_HALF_UP)
                        .divide(BigDecimal.valueOf(1024 * 1024), 10, BigDecimal.ROUND_HALF_UP); // 转换为MB/s

                // 计算解码吞吐量
                BigDecimal decodeTimeSeconds = decodeTime.divide(BigDecimal.valueOf(1_000_000_000), 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal decodeThroughput = originalSizeBytes.divide(decodeTimeSeconds, 10, BigDecimal.ROUND_HALF_UP)
                        .divide(BigDecimal.valueOf(1024 * 1024), 10, BigDecimal.ROUND_HALF_UP); // 转换为MB/s

                String[] record = {
                        file.toString(),
                        "BP-DP-AllPackSize",
                        encodeThroughput.toPlainString(),
                        decodeThroughput.toPlainString(),
                        String.valueOf(numbers.size()),
                        modelCost.toPlainString(),
                        String.valueOf(pack_size),
                        model_ratio.toPlainString()
                };
                writer.writeRecord(record);
            }
            writer.close();
        }

    }

    @Test
    public void SprintzVarPackSize() throws IOException {
        // 示例数据（实际应替换为真实时间序列）
        System.out.println("\nPerformance Testing with All Pack Sizes (1-16)...");
        String directory = "benchmark_data/ElfTestData_camel";
        String outputDirstr = "benchmark_data/output_DP_Sprintz_vary_pack_size";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
            System.out.println(file.getName());
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Compression Throughput",
                    "Decompression Throughput",
                    "Points",
                    "Compressed Size",
                    "Pack Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file
            System.out.println("Processing " + file.getName() + "...");
            List<String> numbers = new ArrayList<>();
            List<Integer> decimalPlaces = new ArrayList<>();
            CsvReader csvReader = new CsvReader(file.getPath(), ',', StandardCharsets.UTF_8);
            while (csvReader.readRecord()) {
                for (String value : csvReader.getValues()) {
                    String numStr = value.trim();
                    if (!numStr.isEmpty()) {
                        numbers.add(numStr);
                        int decimal = 0;
                        if (numStr.contains(".")) {
                            String[] parts = numStr.split("\\.");
                            decimal = parts[1].length();
                        }
                        decimalPlaces.add(decimal);
                    }
                }
            }
            int time_of_repeat = all_time_of_repeat;

            for(int pack_size_exp = 0; pack_size_exp < all_max_pack_size; pack_size_exp++) {
                int pack_size = (int) Math.pow(2, pack_size_exp);

                BigDecimal modelCost = BigDecimal.ZERO;
                BigDecimal encodeTime = BigDecimal.ZERO;
                BigDecimal decodeTime = BigDecimal.ZERO;
                for (int j = 0; j < time_of_repeat; j++) {
                    for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {

                        List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                        if (chunkNumbers.size() == 1 || chunkNumbers.size() == 2)
                            continue;

                        int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                                .stream().max(Integer::compare).orElse(0);

                        long[] scaledInt = scaleNumbers(chunkNumbers, decimalMax);

                        long startEncodeTime = System.nanoTime();
                        SprintzEncodedResult ser = sprintz(scaledInt);
                        long[] scaledInts = ser.getEncodedData();
                        int remainder = scaledInts.length % pack_size;
                        int paddingLength = (remainder == 0) ? 0 : pack_size - remainder;

                        // 创建新数组，长度补齐为pack_size的倍数
                        long[] paddedArray = new long[scaledInts.length + paddingLength];
                        System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                        int actual_length = paddedArray.length;
                        int[] bitWidths = new int[actual_length / pack_size]; // 存储每pack_size个值的位宽结果

                        for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += pack_size) {
                                bitWidths[scaledInts_i / pack_size] =
                                        BP.groupBitWidthForPack(paddedArray, scaledInts_i, pack_size);
                        }
                        int[] encodePos = new int[1];
                        byte[] compressedData = compressWithOptimalPacking(paddedArray, bitWidths, pack_size, encodePos);
                        long encodeDuration = System.nanoTime() - startEncodeTime;

                        long startDecodeTime = System.nanoTime();
                        long[] decodedData = decompressWithOptimalPacking(compressedData, paddedArray.length, pack_size);
                        sprintzDecode(decodedData, ser.getFirstValue());
                        long decodeDuration = System.nanoTime() - startDecodeTime;

                        BigDecimal cur_cost = BigDecimal.valueOf(encodePos[0] * 8L);
                        encodeTime = encodeTime.add(BigDecimal.valueOf(encodeDuration));
                        decodeTime = decodeTime.add(BigDecimal.valueOf(decodeDuration));
                        modelCost = modelCost.add(cur_cost);
                    }

                }
                BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                encodeTime = encodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                decodeTime = decodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP);

                // 计算编码吞吐量 (MB/s)
                BigDecimal originalSizeBytes = numbersSizeBD.multiply(BigDecimal.valueOf(8));
                BigDecimal encodeTimeSeconds = encodeTime.divide(BigDecimal.valueOf(1_000_000_000), 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal encodeThroughput = originalSizeBytes.divide(encodeTimeSeconds, 10, BigDecimal.ROUND_HALF_UP)
                        .divide(BigDecimal.valueOf(1024 * 1024), 10, BigDecimal.ROUND_HALF_UP);

                BigDecimal decodeTimeSeconds = decodeTime.divide(BigDecimal.valueOf(1_000_000_000), 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal decodeThroughput = originalSizeBytes.divide(decodeTimeSeconds, 10, BigDecimal.ROUND_HALF_UP)
                        .divide(BigDecimal.valueOf(1024 * 1024), 10, BigDecimal.ROUND_HALF_UP);

                String[] record = {
                        file.toString(),
                        "BP-DP-AllPackSize",
                        encodeThroughput.toPlainString(),
                        decodeThroughput.toPlainString(),
                        String.valueOf(numbers.size()),
                        modelCost.toPlainString(),
                        String.valueOf(pack_size),
                        model_ratio.toPlainString()
                };
                writer.writeRecord(record);
            }
            writer.close();
        }

    }

    // 新增方法：测试不同chunk size的表现
    @Test
    public void SprintzVarChunkSize() throws IOException {
        System.out.println("\nPerformance Testing with Variable Chunk Sizes...");
        String directory = "benchmark_data/ElfTestData_camel";
        String outputDirstr = "benchmark_data/output_DP_Sprintz_vary_m";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);

        // 定义要测试的chunk sizes (m*8 where m is 16, 32, 64, 128, 256, 512, 1024)
        int[] chunkSizes = {16*8, 32*8, 64*8, 128*8, 256*8, 512*8}; // , 1024*8

        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
            System.out.println("Processing " + file.getName() + " with variable chunk sizes...");
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "m",
                    "Input Direction",
                    "Encoding Algorithm",
                    "Compression Throughput",
                    "Decompression Throughput",
                    "Points",
                    "Compressed Size",
                    "Pack Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head);

            List<String> numbers = new ArrayList<>();
            List<Integer> decimalPlaces = new ArrayList<>();
            CsvReader csvReader = new CsvReader(file.getPath(), ',', StandardCharsets.UTF_8);
            while (csvReader.readRecord()) {
                for (String value : csvReader.getValues()) {
                    String numStr = value.trim();
                    if (!numStr.isEmpty()) {
                        numbers.add(numStr);
                        int decimal = 0;
                        if (numStr.contains(".")) {
                            String[] parts = numStr.split("\\.");
                            decimal = parts[1].length();
                        }
                        decimalPlaces.add(decimal);
                    }
                }
            }

            int time_of_repeat = 10;
            int decimalMax = decimalPlaces.stream().max(Integer::compare).orElse(0);

            int batchSize = 1024;
            List<long[]> batches = new ArrayList<>();

            for (int i = 0; i < numbers.size(); i += batchSize) {
                int end = Math.min(numbers.size(), i + batchSize);
                List<String> batch = numbers.subList(i, end);
                long[] scaledBatch = scaleNumbers(batch, decimalMax);
                batches.add(scaledBatch);
            }

            // 计算总长度并拼接所有批次的结果
            int totalLength = batches.stream().mapToInt(arr -> arr.length).sum();
            long[] scaledInts_all = new long[totalLength];

            int currentIndex = 0;
            for (long[] batch : batches) {
                System.arraycopy(batch, 0, scaledInts_all, currentIndex, batch.length);
                currentIndex += batch.length;
            }
            // 测试每个chunk size
            for (int chunkSize : chunkSizes) {
                System.out.println("Testing chunk size: " + chunkSize);

                for(int pack_size_exp = 0; pack_size_exp < 1; pack_size_exp++) {
                    int pack_size = (int) Math.pow(2, pack_size_exp);
                    BigDecimal modelCost = BigDecimal.ZERO;
                    BigDecimal modelTime = BigDecimal.ZERO;
                    BigDecimal modelDecodeTime = BigDecimal.ZERO;

                    for (int j = 0; j < time_of_repeat; j++) {
                        int totalCost = 0;
                        for (int i = 0; i < numbers.size(); i += chunkSize) {
                            int end = Math.min(i + chunkSize, numbers.size());
                            long[] scaledInt= new long[end-i];
                            if (end - i >= 0) System.arraycopy(scaledInts_all, i, scaledInt, 0, end - i);

                            long startTime = System.nanoTime();
                            SprintzEncodedResult ser = sprintz(scaledInt);
                            long[] scaledInts = ser.getEncodedData();
                            ExactOracleCompressionResult res = compressWithBestExactPacking(scaledInts);
                            pack_size = res.packSize;
                            BigDecimal cur_cost = BigDecimal.valueOf(res.encodedBits);
                            long duration = System.nanoTime() - startTime;
                            modelTime = modelTime.add(BigDecimal.valueOf(duration));
                            modelCost = modelCost.add(cur_cost);

                            long decodeStart = System.nanoTime();
                            long[] decoded = decompressExactOracle(res);
                            sprintzDecode(decoded, ser.getFirstValue());
                            long decodeDuration = System.nanoTime() - decodeStart;
                            modelDecodeTime = modelDecodeTime.add(BigDecimal.valueOf(decodeDuration));
                        }
                    }

                    BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                    modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                    modelTime = modelTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                    modelDecodeTime = modelDecodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                    BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                    BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP);
                    BigDecimal modelTime_throughput = numbersSizeBD.multiply(BigDecimal.valueOf(8000)).divide(modelTime, 10, BigDecimal.ROUND_HALF_UP);
                    BigDecimal decodeThroughput = BigDecimal.ZERO;
                    if (modelDecodeTime.compareTo(BigDecimal.ZERO) != 0) {
                        decodeThroughput =
                                numbersSizeBD.multiply(BigDecimal.valueOf(8000)).divide(modelDecodeTime, 10, BigDecimal.ROUND_HALF_UP);
                    }

                    String[] record = {
                            String.valueOf(chunkSize),
                            file.toString(),
                            "Sprintz-DP",
                            String.valueOf(modelTime_throughput.doubleValue()),
                            decodeThroughput.toPlainString(),
                            String.valueOf(numbers.size()),
                            String.valueOf(modelCost),
                            String.valueOf(pack_size),
                            String.valueOf(model_ratio.doubleValue())
                    };
                    writer.writeRecord(record);
                }
            }
            writer.close();
        }
    }

    @Test
    public void TS2DIFFDP() throws IOException {
        // 示例数据（实际应替换为真实时间序列）
        System.out.println("\nPerformance Testing with All Pack Sizes (1-16)...");
        String directory = "benchmark_data/ElfTestData_camel";
        String outputDirstr = "benchmark_data/output_DP_TS2DIFF";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
            System.out.println(file.getName());
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Compression Throughput",
                    "Decompression Throughput",
                    "Points",
                    "Compressed Size",
                    "Pack Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file
            System.out.println("Processing " + file.getName() + "...");
            List<String> numbers = new ArrayList<>();
            List<Integer> decimalPlaces = new ArrayList<>();
            CsvReader csvReader = new CsvReader(file.getPath(), ',', StandardCharsets.UTF_8);
            while (csvReader.readRecord()) {
                for (String value : csvReader.getValues()) {
                    String numStr = value.trim();
                    if (!numStr.isEmpty()) {
                        numbers.add(numStr);
                        int decimal = 0;
                        if (numStr.contains(".")) {
                            String[] parts = numStr.split("\\.");
                            decimal = parts[1].length();
                        }
                        decimalPlaces.add(decimal);
                    }
                }
            }
            int time_of_repeat = 10;

            for(int pack_size_exp = 0; pack_size_exp < 1; pack_size_exp++) {
                int pack_size = (int) Math.pow(2, pack_size_exp);

                BigDecimal modelCost = BigDecimal.ZERO;
                BigDecimal encodeTime = BigDecimal.ZERO;
                BigDecimal decodeTime = BigDecimal.ZERO;
                for (int j = 0; j < time_of_repeat; j++) {
                    for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {

                        List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                        if (chunkNumbers.size() == 1 || chunkNumbers.size() == 2)
                            continue;

                        int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                                .stream().max(Integer::compare).orElse(0);

                        long[] scaledInt = scaleNumbers(chunkNumbers, decimalMax);

                        long startEncodeTime = System.nanoTime();
                        TSDIFFEncodedResult ter = ts2diff(scaledInt);
                        long[] scaledInts = ter.getEncodedData();

                        int remainder = scaledInts.length % pack_size;
                        int paddingLength = (remainder == 0) ? 0 : pack_size - remainder;

                        // 创建新数组，长度补齐为pack_size的倍数
                        long[] paddedArray = new long[scaledInts.length + paddingLength];
                        System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                        int actual_length = paddedArray.length;
                        int[] bitWidths = new int[actual_length / pack_size]; // 存储每pack_size个值的位宽结果

                        for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += pack_size) {
                                bitWidths[scaledInts_i / pack_size] =
                                        BP.groupBitWidthForPack(paddedArray, scaledInts_i, pack_size);
                        }
                        int[] encodePos = new int[1];
                        byte[] compressedData = compressWithOptimalPacking(paddedArray, bitWidths, pack_size, encodePos);
                        long encodeDuration = System.nanoTime() - startEncodeTime;

                        // 解压测试
                        long startDecodeTime = System.nanoTime();
                        long[] decodedData = decompressWithOptimalPacking(compressedData, paddedArray.length, pack_size);
                        long[] originalData = ts2diffDecode(decodedData,ter.getFirstValue(),ter.getMinDiff());
                        long decodeDuration = System.nanoTime() - startDecodeTime;

                        // 验证解压数据的正确性（可选）
                        // for (int idx = 0; idx < scaledInts.length; idx++) {
                        //     if (paddedArray[idx] != decompressedData[idx]) {
                        //         System.err.println("Decompression error at index " + idx);
                        //     }
                        // }

                        BigDecimal cur_cost = BigDecimal.valueOf(encodePos[0] * 8L);
                        encodeTime = encodeTime.add(BigDecimal.valueOf(encodeDuration));
                        decodeTime = decodeTime.add(BigDecimal.valueOf(decodeDuration));
                        modelCost = modelCost.add(cur_cost);
                    }

                }
                BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                encodeTime = encodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                decodeTime = decodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP);

                // 计算编码吞吐量 (MB/s): 数据量(bytes) / 时间(s)
                // 原始数据大小: numbers.size() * 8 bytes (因为long是8字节)
                BigDecimal originalSizeBytes = numbersSizeBD.multiply(BigDecimal.valueOf(8));
                BigDecimal encodeTimeSeconds = encodeTime.divide(BigDecimal.valueOf(1_000_000_000), 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal encodeThroughput = originalSizeBytes.divide(encodeTimeSeconds, 10, BigDecimal.ROUND_HALF_UP)
                        .divide(BigDecimal.valueOf(1024 * 1024), 10, BigDecimal.ROUND_HALF_UP); // 转换为MB/s

                // 计算解码吞吐量
                BigDecimal decodeTimeSeconds = decodeTime.divide(BigDecimal.valueOf(1_000_000_000), 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal decodeThroughput = originalSizeBytes.divide(decodeTimeSeconds, 10, BigDecimal.ROUND_HALF_UP)
                        .divide(BigDecimal.valueOf(1024 * 1024), 10, BigDecimal.ROUND_HALF_UP); // 转换为MB/s

                String[] record = {
                        file.toString(),
                        "TS2DIFF-DP",
                        encodeThroughput.toPlainString(),
                        decodeThroughput.toPlainString(),
                        String.valueOf(numbers.size()),
                        modelCost.toPlainString(),
                        String.valueOf(pack_size),
                        model_ratio.toPlainString()
                };
                writer.writeRecord(record);
            }
            writer.close();
        }

    }

    @Test
    public void TS2DIFFDPVarPackSize() throws IOException {
        // 示例数据（实际应替换为真实时间序列）
        System.out.println("\nPerformance Testing with All Pack Sizes (1-16)...");
        String directory = "benchmark_data/ElfTestData_camel";
        String outputDirstr = "benchmark_data/output_DP_TS2DIFF_vary_pack_size";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
            System.out.println(file.getName());
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Compression Throughput",
                    "Decompression Throughput",
                    "Points",
                    "Compressed Size",
                    "Pack Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file
            System.out.println("Processing " + file.getName() + "...");
            List<String> numbers = new ArrayList<>();
            List<Integer> decimalPlaces = new ArrayList<>();
            CsvReader csvReader = new CsvReader(file.getPath(), ',', StandardCharsets.UTF_8);
            while (csvReader.readRecord()) {
                for (String value : csvReader.getValues()) {
                    String numStr = value.trim();
                    if (!numStr.isEmpty()) {
                        numbers.add(numStr);
                        int decimal = 0;
                        if (numStr.contains(".")) {
                            String[] parts = numStr.split("\\.");
                            decimal = parts[1].length();
                        }
                        decimalPlaces.add(decimal);
                    }
                }
            }
            int time_of_repeat = all_time_of_repeat;

            for(int pack_size_exp = 0; pack_size_exp < all_max_pack_size; pack_size_exp++) {
                int pack_size = (int) Math.pow(2, pack_size_exp);

                BigDecimal modelCost = BigDecimal.ZERO;
                BigDecimal encodeTime = BigDecimal.ZERO;
                BigDecimal decodeTime = BigDecimal.ZERO;
                int chunksize = CHUNK_SIZE + 1;
                for (int j = 0; j < time_of_repeat; j++) {
                    for (int i = 0; i < numbers.size(); i += chunksize) {

                        List<String> chunkNumbers = numbers.subList(i, Math.min(i + chunksize, numbers.size()));
                        if (chunkNumbers.size() == 1 || chunkNumbers.size() == 2)
                            continue;

                        int decimalMax = decimalPlaces.subList(i, Math.min(i + chunksize, numbers.size()))
                                .stream().max(Integer::compare).orElse(0);

                        long[] scaledInt = scaleNumbers(chunkNumbers, decimalMax);

                        long startEncodeTime = System.nanoTime();
                        TSDIFFEncodedResult ter = ts2diff(scaledInt);
                        long[] scaledInts = ter.getEncodedData();
                        int remainder = scaledInts.length % pack_size;
                        int paddingLength = (remainder == 0) ? 0 : pack_size - remainder;

                        // 创建新数组，长度补齐为pack_size的倍数
                        long[] paddedArray = new long[scaledInts.length + paddingLength];
                        System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                        int actual_length = paddedArray.length;
                        int[] bitWidths = new int[actual_length / pack_size]; // 存储每pack_size个值的位宽结果

                        for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += pack_size) {
                                bitWidths[scaledInts_i / pack_size] =
                                        BP.groupBitWidthForPack(paddedArray, scaledInts_i, pack_size);
                        }
                        int[] encodePos = new int[1];
                        byte[] compressedData = compressWithOptimalPacking(paddedArray, bitWidths, pack_size, encodePos);
                        long encodeDuration = System.nanoTime() - startEncodeTime;

                        long startDecodeTime = System.nanoTime();
                        long[] decodedData = decompressWithOptimalPacking(compressedData, paddedArray.length, pack_size);
                        ts2diffDecode(decodedData, ter.getFirstValue(), ter.getMinDiff());
                        long decodeDuration = System.nanoTime() - startDecodeTime;

                        BigDecimal cur_cost = BigDecimal.valueOf(encodePos[0] * 8L);
                        encodeTime = encodeTime.add(BigDecimal.valueOf(encodeDuration));
                        decodeTime = decodeTime.add(BigDecimal.valueOf(decodeDuration));
                        modelCost = modelCost.add(cur_cost);
                    }

                }
                BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                encodeTime = encodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                decodeTime = decodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP);

                // 计算编码吞吐量 (MB/s)
                BigDecimal originalSizeBytes = numbersSizeBD.multiply(BigDecimal.valueOf(8));
                BigDecimal encodeTimeSeconds = encodeTime.divide(BigDecimal.valueOf(1_000_000_000), 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal encodeThroughput = originalSizeBytes.divide(encodeTimeSeconds, 10, BigDecimal.ROUND_HALF_UP)
                        .divide(BigDecimal.valueOf(1024 * 1024), 10, BigDecimal.ROUND_HALF_UP);

                BigDecimal decodeTimeSeconds = decodeTime.divide(BigDecimal.valueOf(1_000_000_000), 10, BigDecimal.ROUND_HALF_UP);
                BigDecimal decodeThroughput = originalSizeBytes.divide(decodeTimeSeconds, 10, BigDecimal.ROUND_HALF_UP)
                        .divide(BigDecimal.valueOf(1024 * 1024), 10, BigDecimal.ROUND_HALF_UP);

                String[] record = {
                        file.toString(),
                        "TS2DIFF-DP",
                        encodeThroughput.toPlainString(),
                        decodeThroughput.toPlainString(),
                        String.valueOf(numbers.size()),
                        modelCost.toPlainString(),
                        String.valueOf(pack_size),
                        model_ratio.toPlainString()
                };
                writer.writeRecord(record);
            }
            writer.close();
        }

    }

    // 新增方法：测试不同chunk size的表现
    @Test
    public void TS2DIFFDPVarChunkSize() throws IOException {
        System.out.println("\nPerformance Testing with Variable Chunk Sizes...");
        String directory = "benchmark_data/ElfTestData_camel";
        String outputDirstr = "benchmark_data/output_DP_TS2DIFF_vary_m";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);

        // 定义要测试的chunk sizes (m*8 where m is 16, 32, 64, 128, 256, 512, 1024)
        int[] chunkSizes = {16*8, 32*8, 64*8, 128*8, 256*8, 512*8}; //, 1024*8

        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
            System.out.println("Processing " + file.getName() + " with variable chunk sizes...");
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "m",
                    "Input Direction",
                    "Encoding Algorithm",
                    "Compression Throughput",
                    "Decompression Throughput",
                    "Points",
                    "Compressed Size",
                    "Pack Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head);

            List<String> numbers = new ArrayList<>();
            List<Integer> decimalPlaces = new ArrayList<>();
            CsvReader csvReader = new CsvReader(file.getPath(), ',', StandardCharsets.UTF_8);
            while (csvReader.readRecord()) {
                for (String value : csvReader.getValues()) {
                    String numStr = value.trim();
                    if (!numStr.isEmpty()) {
                        numbers.add(numStr);
                        int decimal = 0;
                        if (numStr.contains(".")) {
                            String[] parts = numStr.split("\\.");
                            decimal = parts[1].length();
                        }
                        decimalPlaces.add(decimal);
                    }
                }
            }

            int time_of_repeat = 1;
            int decimalMax = decimalPlaces.stream().max(Integer::compare).orElse(0);

            int batchSize = 1024;
            List<long[]> batches = new ArrayList<>();

            for (int i = 0; i < numbers.size(); i += batchSize) {
                int end = Math.min(numbers.size(), i + batchSize);
                List<String> batch = numbers.subList(i, end);
                long[] scaledBatch = scaleNumbers(batch, decimalMax);
                batches.add(scaledBatch);
            }

            // 计算总长度并拼接所有批次的结果
            int totalLength = batches.stream().mapToInt(arr -> arr.length).sum();
            long[] scaledInts_all = new long[totalLength];

            int currentIndex = 0;
            for (long[] batch : batches) {
                System.arraycopy(batch, 0, scaledInts_all, currentIndex, batch.length);
                currentIndex += batch.length;
            }
            // 测试每个chunk size
            for (int chunkSize : chunkSizes) {
                System.out.println("Testing chunk size: " + chunkSize);
                chunkSize += 1;

                for(int pack_size_exp = 0; pack_size_exp < 1; pack_size_exp++) {
                    int pack_size = (int) Math.pow(2, pack_size_exp);
                    BigDecimal modelCost = BigDecimal.ZERO;
                    BigDecimal modelTime = BigDecimal.ZERO;
                    BigDecimal modelDecodeTime = BigDecimal.ZERO;

                    for (int j = 0; j < time_of_repeat; j++) {
                        int totalCost = 0;
                        for (int i = 0; i < numbers.size(); i += chunkSize) {
                            int end = Math.min(i + chunkSize, numbers.size());
                            long[] scaledInt= new long[end-i];
                            if (end - i >= 0) System.arraycopy(scaledInts_all, i, scaledInt, 0, end - i);

                            long startTime = System.nanoTime();
                            TSDIFFEncodedResult ter = ts2diff(scaledInt);
                            long[] scaledInts = ter.getEncodedData();

                            ExactOracleCompressionResult res = compressWithBestExactPacking(scaledInts);
                            pack_size = res.packSize;
                            BigDecimal cur_cost = BigDecimal.valueOf(res.encodedBits);
                            long duration = System.nanoTime() - startTime;
                            modelTime = modelTime.add(BigDecimal.valueOf(duration));
                            modelCost = modelCost.add(cur_cost);

                            long decodeStart = System.nanoTime();
                            long[] decoded = decompressExactOracle(res);
                            ts2diffDecode(decoded, ter.getFirstValue(), ter.getMinDiff());
                            long decodeDuration = System.nanoTime() - decodeStart;
                            modelDecodeTime = modelDecodeTime.add(BigDecimal.valueOf(decodeDuration));
                        }
                    }

                    BigDecimal timeOfRepeatBD = BigDecimal.valueOf(time_of_repeat);
                    modelCost = modelCost.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                    modelTime = modelTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                    modelDecodeTime = modelDecodeTime.divide(timeOfRepeatBD, 10, BigDecimal.ROUND_HALF_UP);
                    BigDecimal numbersSizeBD = BigDecimal.valueOf(numbers.size());
                    BigDecimal model_ratio = modelCost.divide(numbersSizeBD.multiply(BigDecimal.valueOf(64)), 10, BigDecimal.ROUND_HALF_UP);
                    BigDecimal modelTime_throughput = numbersSizeBD.multiply(BigDecimal.valueOf(8000)).divide(modelTime, 10, BigDecimal.ROUND_HALF_UP);
                    BigDecimal decodeThroughput = BigDecimal.ZERO;
                    if (modelDecodeTime.compareTo(BigDecimal.ZERO) != 0) {
                        decodeThroughput =
                                numbersSizeBD.multiply(BigDecimal.valueOf(8000)).divide(modelDecodeTime, 10, BigDecimal.ROUND_HALF_UP);
                    }

                    String[] record = {
                            String.valueOf(chunkSize-1),
                            file.toString(),
                            "TS2DIFF-DP",
                            String.valueOf(modelTime_throughput.doubleValue()),
                            decodeThroughput.toPlainString(),
                            String.valueOf(numbers.size()),
                            String.valueOf(modelCost),
                            String.valueOf(pack_size),
                            String.valueOf(model_ratio.doubleValue())
                    };
                    writer.writeRecord(record);
                }
            }
            writer.close();
        }
    }





    @Test
    public void exportGroupSizes() throws IOException {
        System.out.println("\nExporting group sizes for packsize=1...");
        String directory = "benchmark_data/ElfTestData_camel";
        File dir = new File(directory);

        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;

            System.out.println("Processing " + file.getName() + " for packsize=1 group sizes...");

            // 读取数据
            List<String> numbers = new ArrayList<>();
            List<Integer> decimalPlaces = new ArrayList<>();
            CsvReader csvReader = new CsvReader(file.getPath(), ',', StandardCharsets.UTF_8);
            while (csvReader.readRecord()) {
                for (String value : csvReader.getValues()) {
                    String numStr = value.trim();
                    if (!numStr.isEmpty()) {
                        numbers.add(numStr);
                        int decimal = 0;
                        if (numStr.contains(".")) {
                            String[] parts = numStr.split("\\.");
                            decimal = parts[1].length();
                        }
                        decimalPlaces.add(decimal);
                    }
                }
            }
            csvReader.close();

            // 处理每个chunk
            List<int[]> allGroupSizes = new ArrayList<>();

            for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {
                List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                if (chunkNumbers.size() == 1 || chunkNumbers.size() == 2) continue;

                int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                        .stream().max(Integer::compare).orElse(0);

                long[] scaledInts = scaleNumbers(chunkNumbers, decimalMax);

                // packsize = 1
                int pack_size = 1;
                int[] bitWidths = new int[scaledInts.length];

                // 计算每个值的位宽（当packsize=1时，每个值单独计算位宽）
                for (int j = 0; j < scaledInts.length; j++) {
                    int bitWidth = 64 - Long.numberOfLeadingZeros(scaledInts[j]);
                    bitWidths[j] = bitWidth;
                }

                // 计算最优分组方案
                PackingPlan plan = computeOptimalPackingPlan(bitWidths, pack_size);

                // 保存该chunk的分组大小
                allGroupSizes.add(plan.groupSizes);
            }

            // 输出到CSV文件
            String outputPath = "benchmark_data/pack_sizes/" +
                    file.getName().replace(".csv", "") + "_group_sizes.csv";
            CsvWriter writer = new CsvWriter(outputPath, ',', StandardCharsets.UTF_8);

            // 写入表头
            String[] header = new String[allGroupSizes.size()];
            for (int i = 0; i < allGroupSizes.size(); i++) {
                header[i] = "Chunk_" + (i+1);
            }
            writer.writeRecord(header);

            // 找到最大分组数量
            int maxGroups = 0;
            for (int[] groups : allGroupSizes) {
                if (groups.length > maxGroups) {
                    maxGroups = groups.length;
                }
            }

            // 写入数据（每行是一个分组序号，每列是一个chunk）
            for (int row = 0; row < maxGroups; row++) {
                String[] record = new String[allGroupSizes.size()];
                for (int col = 0; col < allGroupSizes.size(); col++) {
                    int[] groups = allGroupSizes.get(col);
                    if (row < groups.length) {
                        record[col] = String.valueOf(groups[row]);
                    } else {
                        record[col] = "";
                    }
                }
                writer.writeRecord(record);
            }

            writer.close();
            System.out.println("Group sizes exported to: " + outputPath);

            // 同时输出汇总统计信息
            String statsPath = "benchmark_data/pack_sizes/" +
                    file.getName().replace(".csv", "") + "_group_stats.csv";
            CsvWriter statsWriter = new CsvWriter(statsPath, ',', StandardCharsets.UTF_8);

            String[] statsHeader = {"Chunk", "Total Groups", "Average Group Size", "Min Group Size", "Max Group Size"};
            statsWriter.writeRecord(statsHeader);

            for (int i = 0; i < allGroupSizes.size(); i++) {
                int[] groups = allGroupSizes.get(i);
                if (groups.length == 0) continue;

                int sum = 0;
                int min = Integer.MAX_VALUE;
                int max = Integer.MIN_VALUE;
                for (int group : groups) {
                    sum += group;
                    if (group < min) {
                        min = group;
                    }
                    if (group > max) {
                        max = group;
                    }
                }
                double avg = sum / (double) groups.length;

                String[] statsRecord = {
                        "Chunk_" + (i+1),
                        String.valueOf(groups.length),
                        String.format("%.2f", avg),
                        String.valueOf(min),
                        String.valueOf(max)
                };
                statsWriter.writeRecord(statsRecord);
            }

            statsWriter.close();
            System.out.println("Group statistics exported to: " + statsPath);
        }

        System.out.println("Export completed!");
    }
}
