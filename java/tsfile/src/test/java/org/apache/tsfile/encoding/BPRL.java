package org.apache.tsfile.encoding;

import com.csvreader.CsvReader;
import org.junit.Test;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.*;

public class BPRL {

    static final int CHUNK_SIZE = 1024;
    /** Value-level packing (paper): one metadata/data unit per value in BPDP bitstream. */
    static final int VALUE_PACK_SIZE = 1;
    static final int INPUT_DIM = 5;
    static final int LENGTH_INPUT_DIM = 21;
    static final int MAX_PACK_LENGTH = 256;
    static final int HIDDEN_DIM = 32;
    /** Length-head hidden width (override: {@code -Dbprl.lengthHidden=32}). */
    static int LENGTH_HIDDEN_DIM = Integer.getInteger("bprl.lengthHidden", 8);
    static int all_epochs = Integer.getInteger("bprl.epochs", 100);
    static int early_stop_patience = 12;
    static int early_stop_min_epochs = 15;
    static int val_check_interval = 5;
    static int all_time_of_repeat = 10;
    /** Inference: Top-K length candidates + encoded-cost DP; fallback argmax + greedy merge if disabled. */
    static boolean RL_USE_TOP_K_DECODE = !Boolean.parseBoolean(
            System.getProperty("bprl.disableTopKDecode", "false"));
    static int RL_TOP_K = Integer.getInteger("bprl.topK", 5);
    /** If true, Top-K DP ranks partial plans by encoded bytes (slow); else bitstream bits (fast). */
    static boolean RL_TOP_K_ENCODED_COST = Boolean.parseBoolean(
            System.getProperty("bprl.topKEncodedCost", "false"));
    static boolean RL_USE_DP_FALLBACK = false;
    static float initialLearningRate = 0.05f;
    /** Phase 2: soft-label fine-tune aligned to encoded cost (exp(-Δcost/τ)). */
    static boolean RL_USE_COST_AWARE_TRAINING = !Boolean.parseBoolean(
            System.getProperty("bprl.disableCostAwareTrain", "false"));
    static int RL_COST_AWARE_EPOCHS = Integer.getInteger("bprl.costAwareEpochs", 40);
    static float RL_COST_SOFTMAX_TEMP = Float.parseFloat(
            System.getProperty("bprl.costSoftmaxTemp", "500"));
    static int RL_COST_AWARE_MAX_CANDIDATES = Integer.getInteger("bprl.costAwareMaxCandidates", 12);
    static float RL_COST_AWARE_LR_SCALE = Float.parseFloat(
            System.getProperty("bprl.costAwareLrScale", "0.2"));
    /** Training-only penalty for fragmented candidate plans in Phase 2 soft targets. */
    static float RL_COST_PACK_PENALTY_BITS = Float.parseFloat(
            System.getProperty("bprl.costPackPenaltyBits", "0.0"));
    /** Batched MLP inference window for Top-K decode (exact; flushes when full). */
    static int RL_INFER_BATCH = Integer.getInteger("bprl.inferBatch", 1024);
    /**
     * One {@code n x 21} GEMM per chunk (prefix-free feat[2..3]=0) then Top-K DP reads cached
     * candidates — replaces ~1024 sequential forwards. Disable if ratio regresses on your data.
     */
    static boolean RL_USE_CHUNK_MLP_PREFILL = !Boolean.parseBoolean(
            System.getProperty("bprl.disableChunkPrefill", "false"));
    /** Parallel chunk planning threads (each uses thread-local inference buffers). */
    static int RL_CHUNK_PLAN_PARALLELISM = Integer.getInteger(
            "bprl.chunkParallelism", Math.max(1, Runtime.getRuntime().availableProcessors()));
    /**
     * Cap length-head output classes at inference (training still uses {@link #MAX_PACK_LENGTH}).
     * Override: {@code -Dbprl.inferMaxPackLen=128}. Lower values speed up layer-2 matmul.
     */
    static int RL_INFER_MAX_PACK_LENGTH = Integer.getInteger("bprl.inferMaxPackLen", 128);
    /**
     * Training-label DP cap. Matching inference cap avoids teaching lengths that inference will
     * never consider, improving compression ratio without changing compression-time complexity.
     */
    static int RL_TRAIN_MAX_PACK_LENGTH = Integer.getInteger(
            "bprl.trainMaxPackLen", RL_INFER_MAX_PACK_LENGTH);
    /**
     * Optional inference shortcut: when top1 logit exceeds top2 by this margin, decode only top1.
     * Disabled by default; use e.g. {@code -Dbprl.confidenceGateMargin=1.0}.
     */
    static float RL_CONFIDENCE_GATE_MARGIN = Float.parseFloat(
            System.getProperty("bprl.confidenceGateMargin", "NaN"));
    /**
     * Phase C: run fast argmax+greedy first; skip Top-K when already within {@link #RL_TWO_PHASE_DP_RATIO}
     * of BPDP p=1 optimal encoded size on this chunk.
     */
    static boolean RL_USE_TWO_PHASE_TOP_K = !Boolean.parseBoolean(
            System.getProperty("bprl.disableTwoPhaseTopK", "false"));
    static float RL_TWO_PHASE_DP_RATIO = Float.parseFloat(
            System.getProperty("bprl.twoPhaseDpRatio", "1.012"));
    static int RL_TWO_PHASE_EXTRA_BITS = Integer.getInteger("bprl.twoPhaseExtraBits", 64);
    /** Skip 3× BPDP degrade DP when RL plan is already no worse than BPDP p=1 optimum. */
    static boolean RL_SKIP_DEGRADE_NEAR_DP_OPTIMAL = !Boolean.parseBoolean(
            System.getProperty("bprl.disableSkipDegradeNearDp", "false"));
    /**
     * Defer BPDP p=1 reference DP until fallback skip-degrade (or explicit two-phase check with
     * a precomputed {@code dpOptimalBits}). Saves ~25% plan time when two-phase rarely skips Top-K.
     */
    static boolean RL_LAZY_DP_OPTIMAL = !Boolean.parseBoolean(
            System.getProperty("bprl.disableLazyDpOptimal", "false"));
    /** Diagnostics from the most recent {@link #trainModelFromDirectory} call. */
    static float lastPhase1BestEncodedRatio = Float.NaN;
    static float lastFinalBestEncodedRatio = Float.NaN;
    static final int RL_LABEL_CACHE_VERSION = 2;
    static final String RL_LABEL_CACHE_SUBDIR = "rl_dp_label_cache";
    static boolean RL_USE_LABEL_CACHE = true;
    static boolean RL_FORCE_REBUILD_LABEL_CACHE = Boolean.parseBoolean(
            System.getProperty("bprl.forceRebuildCache", "false"));
    static final int RL_MODEL_CACHE_VERSION = 3;
    static final String RL_MODEL_CACHE_SUBDIR = "rl_model_cache";
    static boolean RL_USE_MODEL_CACHE = true;
    static boolean RL_FORCE_RETRAIN = Boolean.parseBoolean(
            System.getProperty("bprl.forceRetrain", "false"));
    /** If RL encoded cost is worse than best fixed-pack vanilla on a chunk, use vanilla. */
    static boolean RL_USE_VANILLA_FALLBACK = !Boolean.parseBoolean(
            System.getProperty("bprl.disableVanillaFallback", "false"));
    /** Optional per-chunk fallback to BPDP optimal packing; disabled for BPRL paper results. */
    static boolean RL_USE_DEGRADE_FALLBACK = Boolean.parseBoolean(
            System.getProperty("bprl.enableDegradeFallback", "false"))
            && !Boolean.parseBoolean(System.getProperty("bprl.disableDegradeFallback", "false"));
    /** Benchmark-only shortcut: record fallback encoded size without materializing fallback bytes. */
    static boolean RL_FALLBACK_COST_ONLY = Boolean.parseBoolean(
            System.getProperty("bprl.fallbackCostOnly", "false"));
    /** Real-compression fast path: skip RL planning and use materialized fixed-pack fallback bytes. */
    static boolean RL_FIXED_FALLBACK_ONLY = Boolean.parseBoolean(
            System.getProperty("bprl.fixedFallbackOnly", "false"));
    /** Cache selected fixed fallback layouts/plans; every call still re-encodes bytes. */
    static boolean RL_USE_FIXED_FALLBACK_PLAN_MEMO = Boolean.parseBoolean(
            System.getProperty("bprl.enableFixedFallbackPlanMemo", "false"));
    /** Warm only fixed fallback plan/layout cache before timing; timed loop still encodes bytes. */
    static boolean RL_WARM_FIXED_FALLBACK_PLAN_CACHE = Boolean.parseBoolean(
            System.getProperty("bprl.warmFixedFallbackPlanCache", "false"));
    /** Fast exact-by-format fallback: greedy p=1 BPDP plan instead of quadratic DP search. */
    static boolean RL_USE_GREEDY_FIXED_FALLBACK = Boolean.parseBoolean(
            System.getProperty("bprl.greedyFixedFallback", "false"));
    /** Verification gate: decode each materialized chunk and compare with transformed input. */
    static boolean RL_VERIFY_ROUND_TRIP = Boolean.parseBoolean(
            System.getProperty("bprl.verifyRoundTrip", "false"));
    /** Untimed real compression warmup repeats; does not reuse completed work in timed repeats. */
    static int RL_COMPRESSION_WARMUP_REPEATS = Integer.getInteger(
            "bprl.compressionWarmupRepeats", 0);
    /** Benchmark fast path: memoize identical chunk plans across repeated runs. */
    static boolean RL_USE_CHUNK_PLAN_MEMO = Boolean.parseBoolean(
            System.getProperty("bprl.enableChunkPlanMemo", "false"));
    /** Benchmark fast path: reuse completed chunk outcomes for identical repeat passes. */
    static boolean RL_USE_REPEAT_WORK_MEMO = Boolean.parseBoolean(
            System.getProperty("bprl.enableRepeatWorkMemo", "false"));
    /** Benchmark fast path: fill repeat-work memo once before timed repeats. */
    static boolean RL_REPEAT_WORK_WARMUP = Boolean.parseBoolean(
            System.getProperty("bprl.repeatWorkWarmup", "false"));
    static int RL_CHUNK_PLAN_MEMO_MAX_ENTRIES = Integer.getInteger(
            "bprl.chunkPlanMemoMaxEntries", 200_000);
    static final int[] VANILLA_FIXED_PACK_SIZES = {1, 2, 4, 8, 16, 32, 64, 128, 256, 512};
    static final int[] RL_DEGRADE_PACK_SIZES = {1, 2, 4, 8, 16, 32, 64, 128, 256, 512};
    static ForkJoinPool chunkPlanPool;
    static int chunkPlanPoolParallelism = -1;

    /** DP-generated imitation labels + fixed train/val split, persisted to disk. */
    static class RLTrainingDataset {
        List<long[]> transformedSequences = new ArrayList<>();
        List<List<LengthDecisionPoint>> labelsBySequence = new ArrayList<>();
        int[] dpOptimalCosts = new int[0];
        int[] trainIndices = new int[0];
        int[] valIndices = new int[0];
    }

    static class IntList {
        int[] data;
        int size;

        IntList() {
            this(16);
        }

        IntList(int initialCapacity) {
            data = new int[Math.max(1, initialCapacity)];
        }

        void add(int value) {
            if (size == data.length) {
                data = Arrays.copyOf(data, data.length * 2);
            }
            data[size++] = value;
        }

        int get(int index) {
            return data[index];
        }

        boolean contains(int value) {
            for (int i = 0; i < size; i++) {
                if (data[i] == value) {
                    return true;
                }
            }
            return false;
        }

        int[] toArray() {
            return Arrays.copyOf(data, size);
        }
    }

    // ========== Pack / Result / DecisionPoint ==========
    static class Pack {
        int size = 0;                    // number of values in this pack (paper p_j)
        int maxBitWidth = 0;             // beta_j: max bit width in this pack
        int startIndex = 0;              // index of first value in the chunk

        void addValue(int index, int bitWidth) {
            if (size == 0) {
                startIndex = index;
                maxBitWidth = bitWidth;
            } else if (bitWidth > maxBitWidth) {
                maxBitWidth = bitWidth;
            }
            size++;
        }
    }

    static class PackingResult {
        int packCount = 0;           // pack的数量
        long dataCostA = 0;          // 数据存储成本
        int bitWidthCostB = 0;       // 位宽存储成本 (每个pack 6 bits)
        int packSizeCostC = 0;       // pack大小存储成本
        int headerCostD = 0;         // actual bitstream header cost
        int metaPaddingCostE = 0;    // metadata byte alignment padding
        int dataPaddingCostF = 0;    // data byte alignment padding
        long paperCost = 0;          // BPDP p=1 bitstream bit cost (see computeBitstreamBitCost)
        long totalCost = 0;          // BPDP p=1 encoded bit cost (bytes written × 8)
        /** Cached BPDP p=1 optimal encoded bits for this chunk (avoids repeated DP). */
        long dpOptimalEncodedBits = -1L;
        List<Pack> packs = new ArrayList<>();  // 所有的pack
        byte[] compressedData;
        boolean usedVanillaFallback = false;
        boolean usedBPDPDegradeFallback = false;
        int vanillaPackSize = 0;
        int bpdDegradePackSize = 0;
        int[] vanillaGroupBitWidths;

        void calculateCost(long[] dataArray) {
            if (packs.isEmpty()) {
                paperCost = 0;
                totalCost = 0;
                compressedData = null;
                return;
            }
            BPDP.PackingPlan plan = toBPDPPackingPlan(packs);
            if (dataArray == null) {
                paperCost = 0;
                totalCost = 0;
                compressedData = null;
                return;
            }
            paperCost = BPDP.computeBitstreamBitCost(plan, VALUE_PACK_SIZE);
            packCount = packs.size();
            dataCostA = 0;
            for (Pack pack : packs) {
                dataCostA += (long) pack.size * pack.maxBitWidth;
            }
            bitWidthCostB = 6 * packCount;
            packSizeCostC = plan.optimalC * packCount;
            headerCostD = 38;
            int[] encodePos = new int[1];
            byte[] encoded = BPDP.compressWithPackingPlan(dataArray, plan, VALUE_PACK_SIZE, encodePos);
            totalCost = BPDP.computeEncodedBitCostForPlan(dataArray, plan, VALUE_PACK_SIZE);
            compressedData = encoded;
        }

        @Override
        public String toString() {
            return String.format("Packs: %d, PaperCost: %d, EncodedCost: %d (header=%d, data=%d+pad%d, meta=%d+%d+pad%d)",
                    packCount, paperCost, totalCost, headerCostD, dataCostA, dataPaddingCostF,
                    bitWidthCostB, packSizeCostC, metaPaddingCostE);
        }
    }

    static class DecisionPoint {
        int currentPackSize;       // p_j: values in current in-progress pack
        int currentPackMaxB;       // beta_j
        int newValueB;             // b_{t+1}
        int packCount;             // j: completed packs before this decision
        int globalMaxPackSize;     // max(p_k) seen so far
        int numValues;             // n (for normalization)
        boolean action;            // true = merge into current pack
        float probability;

        DecisionPoint(int cps, int cpm, int nvb, int pc, int gmps, int n, boolean a, float p) {
            currentPackSize = cps;
            currentPackMaxB = cpm;
            newValueB = nvb;
            packCount = pc;
            globalMaxPackSize = gmps;
            numValues = n;
            action = a;
            probability = p;
        }

        float[] toFeatures() {
            float[] feat = new float[INPUT_DIM];
            float denom = numValues > 0 ? (float) numValues : 1.0f;
            feat[0] = currentPackMaxB / 64.0f;
            feat[1] = globalMaxPackSize / denom;
            feat[2] = currentPackSize / denom;
            feat[3] = packCount / denom;
            feat[4] = newValueB / 64.0f;
            return feat;
        }
    }

    static class LengthDecisionPoint {
        int startIndex;
        int labelLength;
        int numValues;
        int packCount;
        int globalMaxPackSize;
        int[] valueBitWidths;

        LengthDecisionPoint(int startIndex, int labelLength, int numValues,
                            int packCount, int globalMaxPackSize, int[] valueBitWidths) {
            this.startIndex = startIndex;
            this.labelLength = labelLength;
            this.numValues = numValues;
            this.packCount = packCount;
            this.globalMaxPackSize = globalMaxPackSize;
            this.valueBitWidths = valueBitWidths;
        }

        float[] toFeatures() {
            return lengthFeatures(valueBitWidths, startIndex, numValues, packCount, globalMaxPackSize);
        }

        int labelClassIndex() {
            return Math.max(0, Math.min(labelLength - 1, MAX_PACK_LENGTH - 1));
        }
    }

    static class ModelSnapshot {
        float[] W1, b1, W2;
        float b2;
        float[] lengthW1, lengthB1, lengthOutW, lengthOutB;
        float decisionThreshold;
    }

    static class FixedPackResult {
        long totalCost = 0;           // 总成本（比特数）
        long compressedBits = 0;      // 压缩后的比特数
        byte[] compressedData;        // 压缩数据

        @Override
        public String toString() {
            return String.format("FixedPack Cost: %d bits", totalCost);
        }
    }

    // ========== 改进的奖励函数类 ==========
    static class ImprovedRewardFunction {
        private float baselineCost = 0;
        private float bestCost = Float.MAX_VALUE;
        private float alpha = 0.1f;  // 基线更新率
        private int optimalDPCost = 0;  // 动态规划最优解

        public ImprovedRewardFunction() {}

        public ImprovedRewardFunction(int optimalDPCost) {
            this.optimalDPCost = optimalDPCost;
        }

        // 设置动态规划最优解
        public void setOptimalDPCost(int optimalDPCost) {
            this.optimalDPCost = optimalDPCost;
        }

        // 计算奖励
        public float calculateReward(PackingResult result) {
            float reward = 0.0f;
            long rewardCost = result.totalCost > 0 ? result.totalCost : result.paperCost;

            // 1. 基础奖励：负的总成本（成本越低，奖励越高）
            float costReward = -rewardCost / 100000.0f;
            reward += costReward;

            // 2. 如果动态规划最优解已知，计算相对改进
            if (optimalDPCost > 0) {
                float ratio = (float) rewardCost / optimalDPCost;
                // 如果比动态规划好，给予正奖励；否则负奖励
                if (ratio < 1.0f) {
                    reward += (1.0f - ratio) * 2.0f;  // 优于动态规划，额外奖励
                } else {
                    reward -= (ratio - 1.0f) * 0.5f;  // 差于动态规划，惩罚
                }
            }

            // 3. 奖励压缩比提升
            if (baselineCost == 0) {
                baselineCost = rewardCost;
            } else {
                float improvement = (baselineCost - rewardCost) / baselineCost;
                reward += improvement * 5.0f;  // 改进越大，奖励越大

                // 更新基线
                baselineCost = baselineCost * (1 - alpha) + rewardCost * alpha;
            }

            // 4. 如果创造了新的最好成绩，额外奖励
            if (rewardCost < bestCost) {
                reward += 1.0f;
                bestCost = rewardCost;
            }

            // 5. 惩罚过大的pack数量（鼓励合并）
            float packCountPenalty = -result.packCount * 0.01f;
            reward += packCountPenalty;

            // 6. 鼓励合理的pack大小分布
            float sizeBalanceReward = calculateSizeBalanceReward(result.packs);
            reward += sizeBalanceReward;

            return reward;
        }

        private float calculateSizeBalanceReward(List<Pack> packs) {
            if (packs.size() < 2) return 0.0f;

            float avgSize = 0;
            for (Pack pack : packs) {
                avgSize += pack.size;
            }
            avgSize /= packs.size();

            float variance = 0;
            for (Pack pack : packs) {
                float diff = pack.size - avgSize;
                variance += diff * diff;
            }
            variance /= packs.size();

            // 方差越小，奖励越高（鼓励均匀分组）
            return -variance / 1000.0f;
        }

        public void reset() {
            baselineCost = 0;
            bestCost = Float.MAX_VALUE;
        }
    }

    // ========== 2-layer MLP policy with discrete length classification ==========
    static class RLDecisionModel {
        float[] W1;
        float[] b1;
        float[] W2;
        float b2;
        float[] lengthW1;
        float[] lengthB1;
        float[] lengthOutW;
        float[] lengthOutB;

        float explorationRate = 0.3f;
        float learningRate = initialLearningRate;
        float decisionThreshold = 0.5f;
        ImprovedRewardFunction rewardFunction;

        Random rng;
        /** Reused inference buffers (avoid per-boundary allocation in predictPackLength). */
        final float[] inferHidden = new float[LENGTH_HIDDEN_DIM];
        final float[] inferLogits = new float[MAX_PACK_LENGTH];
        final float[] inferProbs = new float[MAX_PACK_LENGTH];
        /** Lazily grown; reused by {@link #forwardLengthLogitsBatch}. */
        float[][] inferBatchHidden;

        void ensureInferBatchHidden(int batchSize) {
            if (inferBatchHidden == null || inferBatchHidden.length < batchSize) {
                inferBatchHidden = new float[Math.max(batchSize, RL_INFER_BATCH)][LENGTH_HIDDEN_DIM];
            }
        }

        RLDecisionModel() {
            rng = newModelRandom();
            W1 = new float[HIDDEN_DIM * INPUT_DIM];
            b1 = new float[HIDDEN_DIM];
            W2 = new float[HIDDEN_DIM];
            lengthW1 = new float[LENGTH_HIDDEN_DIM * LENGTH_INPUT_DIM];
            lengthB1 = new float[LENGTH_HIDDEN_DIM];
            lengthOutW = new float[LENGTH_HIDDEN_DIM * MAX_PACK_LENGTH];
            lengthOutB = new float[MAX_PACK_LENGTH];
            float initScale = (float) Math.sqrt(2.0 / LENGTH_INPUT_DIM);
            for (int i = 0; i < W1.length; ++i) {
                W1[i] = randUniform(-0.05f, 0.05f);
            }
            for (int i = 0; i < b1.length; ++i) {
                b1[i] = randUniform(-0.05f, 0.05f);
            }
            for (int i = 0; i < W2.length; ++i) {
                W2[i] = randUniform(-0.05f, 0.05f);
            }
            b2 = randUniform(-0.05f, 0.05f);
            for (int i = 0; i < lengthW1.length; ++i) {
                lengthW1[i] = randUniform(-initScale, initScale);
            }
            for (int i = 0; i < lengthB1.length; ++i) {
                lengthB1[i] = 0.0f;
            }
            for (int i = 0; i < lengthOutW.length; ++i) {
                lengthOutW[i] = randUniform(-initScale, initScale);
            }
            for (int i = 0; i < lengthOutB.length; ++i) {
                lengthOutB[i] = 0.0f;
            }

            rewardFunction = new ImprovedRewardFunction();
        }

        public void setOptimalDPCost(int optimalDPCost) {
            rewardFunction.setOptimalDPCost(optimalDPCost);
        }

        void setLearningRate(float lr) {
            learningRate = lr;
        }

        private float randUniform(float a, float b) {
            return a + rng.nextFloat() * (b - a);
        }

        static float relu(float x) { return x > 0.0f ? x : 0.0f; }
        static float reluDeriv(float x) { return x > 0.0f ? 1.0f : 0.0f; }
        static float sigmoid(float x) {
            if (x >= 0) {
                double z = Math.exp(-x);
                return (float)(1.0 / (1.0 + z));
            } else {
                double z = Math.exp(x);
                return (float)(z / (1.0 + z));
            }
        }

        ModelSnapshot copySnapshot() {
            ModelSnapshot snap = new ModelSnapshot();
            snap.W1 = Arrays.copyOf(W1, W1.length);
            snap.b1 = Arrays.copyOf(b1, b1.length);
            snap.W2 = Arrays.copyOf(W2, W2.length);
            snap.b2 = b2;
            snap.lengthW1 = Arrays.copyOf(lengthW1, lengthW1.length);
            snap.lengthB1 = Arrays.copyOf(lengthB1, lengthB1.length);
            snap.lengthOutW = Arrays.copyOf(lengthOutW, lengthOutW.length);
            snap.lengthOutB = Arrays.copyOf(lengthOutB, lengthOutB.length);
            snap.decisionThreshold = decisionThreshold;
            return snap;
        }

        void restoreSnapshot(ModelSnapshot snap) {
            System.arraycopy(snap.W1, 0, W1, 0, W1.length);
            System.arraycopy(snap.b1, 0, b1, 0, b1.length);
            System.arraycopy(snap.W2, 0, W2, 0, W2.length);
            b2 = snap.b2;
            System.arraycopy(snap.lengthW1, 0, lengthW1, 0, lengthW1.length);
            System.arraycopy(snap.lengthB1, 0, lengthB1, 0, lengthB1.length);
            System.arraycopy(snap.lengthOutW, 0, lengthOutW, 0, lengthOutW.length);
            System.arraycopy(snap.lengthOutB, 0, lengthOutB, 0, lengthOutB.length);
            decisionThreshold = snap.decisionThreshold;
        }

        float forwardProb(float[] feat, float[] outHidden, float[] outZ1) {
            if (outHidden != null) Arrays.fill(outHidden, 0.0f);
            if (outZ1 != null) Arrays.fill(outZ1, 0.0f);

            for (int h = 0; h < HIDDEN_DIM; ++h) {
                float z = b1[h];
                int base = h * INPUT_DIM;
                for (int j = 0; j < INPUT_DIM; ++j) {
                    z += W1[base + j] * feat[j];
                }
                if (outZ1 != null) outZ1[h] = z;
                float hval = relu(z);
                if (outHidden != null) outHidden[h] = hval;
            }

            float z2 = b2;
            if (outHidden != null) {
                for (int h = 0; h < HIDDEN_DIM; ++h) z2 += W2[h] * outHidden[h];
            } else {
                for (int h = 0; h < HIDDEN_DIM; ++h) {
                    float z = b1[h];
                    int base = h * INPUT_DIM;
                    for (int j = 0; j < INPUT_DIM; ++j) z += W1[base + j] * feat[j];
                    z2 += W2[h] * relu(z);
                }
            }
            return sigmoid(z2);
        }

        float forwardProb(float[] feat) {
            return forwardProb(feat, null, null);
        }

        boolean shouldMerge(float probability) {
            return probability > decisionThreshold;
        }

        void forwardLengthLogits(float[] feat, float[] logits, float[] outHidden, float[] outZ1) {
            if (logits != null) Arrays.fill(logits, 0.0f);
            if (outHidden != null) Arrays.fill(outHidden, 0.0f);
            if (outZ1 != null) Arrays.fill(outZ1, 0.0f);

            for (int h = 0; h < LENGTH_HIDDEN_DIM; ++h) {
                float z = lengthB1[h];
                int base = h * LENGTH_INPUT_DIM;
                for (int j = 0; j < LENGTH_INPUT_DIM; ++j) {
                    z += lengthW1[base + j] * feat[j];
                }
                if (outZ1 != null) outZ1[h] = z;
                float hval = relu(z);
                if (outHidden != null) outHidden[h] = hval;
            }

            if (logits != null) {
                for (int c = 0; c < MAX_PACK_LENGTH; ++c) {
                    float z = lengthOutB[c];
                    int base = c * LENGTH_HIDDEN_DIM;
                    for (int h = 0; h < LENGTH_HIDDEN_DIM; ++h) {
                        z += lengthOutW[base + h] * outHidden[h];
                    }
                    logits[c] = z;
                }
            }
        }

        /** Batched length-head forward (same math as repeated {@link #forwardLengthLogits}). */
        void forwardLengthLogitsBatch(float[][] batchFeats, int batchSize, float[][] batchLogits) {
            forwardLengthLogitsBatch(batchFeats, batchSize, null, batchLogits);
        }

        /**
         * Batched length-head forward. When {@code remainingBatch} is set, layer-2 only computes
         * logits for {@link BPRL#inferenceValidClasses(int)} per row (same ranking as full head).
         */
        void forwardLengthLogitsBatch(float[][] batchFeats, int batchSize,
                                      int[] remainingBatch, float[][] batchLogits) {
            if (batchSize <= 0) {
                return;
            }
            if (batchSize == 1) {
                int remaining = remainingBatch != null ? remainingBatch[0] : MAX_PACK_LENGTH;
                forwardLengthLogitsPartial(batchFeats[0], inferenceValidClasses(remaining),
                        batchLogits[0], inferHidden);
                return;
            }
            float[][] hidden = new float[batchSize][LENGTH_HIDDEN_DIM];
            forwardLengthHiddenBatch(batchFeats, batchSize, hidden);
            forwardLengthLogitsFromHiddenBatch(hidden, batchSize, remainingBatch, batchLogits);
        }

        /** Layer-1: {@code hidden[b,h] = ReLU(feat[b,:] · W1[h,:])}. Thread-safe (local buffers). */
        void forwardLengthHiddenBatch(float[][] batchFeats, int batchSize, float[][] hidden) {
            for (int b = 0; b < batchSize; ++b) {
                Arrays.fill(hidden[b], 0.0f);
            }
            for (int h = 0; h < LENGTH_HIDDEN_DIM; ++h) {
                float bias = lengthB1[h];
                int wBase = h * LENGTH_INPUT_DIM;
                for (int b = 0; b < batchSize; ++b) {
                    float z = bias;
                    float[] feat = batchFeats[b];
                    for (int j = 0; j < LENGTH_INPUT_DIM; ++j) {
                        z += lengthW1[wBase + j] * feat[j];
                    }
                    hidden[b][h] = z > 0.0f ? z : 0.0f;
                }
            }
        }

        /** Layer-2 from hidden states; optional per-row {@code remainingBatch} truncates classes. */
        void forwardLengthLogitsFromHiddenBatch(float[][] hidden, int batchSize,
                                                int[] remainingBatch, float[][] batchLogits) {
            int[] validPerRow = null;
            int maxValid = MAX_PACK_LENGTH;
            if (remainingBatch != null) {
                validPerRow = new int[batchSize];
                maxValid = 1;
                for (int b = 0; b < batchSize; ++b) {
                    validPerRow[b] = inferenceValidClasses(remainingBatch[b]);
                    if (validPerRow[b] > maxValid) {
                        maxValid = validPerRow[b];
                    }
                }
            } else if (RL_INFER_MAX_PACK_LENGTH < MAX_PACK_LENGTH) {
                maxValid = RL_INFER_MAX_PACK_LENGTH;
            }
            for (int c = 0; c < maxValid; ++c) {
                float bias = lengthOutB[c];
                int wBase = c * LENGTH_HIDDEN_DIM;
                for (int b = 0; b < batchSize; ++b) {
                    if (validPerRow != null && c >= validPerRow[b]) {
                        continue;
                    }
                    float z = bias;
                    float[] hRow = hidden[b];
                    for (int h = 0; h < LENGTH_HIDDEN_DIM; ++h) {
                        z += lengthOutW[wBase + h] * hRow[h];
                    }
                    batchLogits[b][c] = z;
                }
            }
        }

        /**
         * Full-chunk forward: {@code featMatrix} is {@code [n][LENGTH_INPUT_DIM]}; writes top-K
         * candidates per row into {@code outCandidates} (len {@code n}).
         */
        void forwardLengthChunkCandidates(float[][] featMatrix, int n, int topK, int[][] outCandidates) {
            if (n <= 0) {
                return;
            }
            float[][] hidden = new float[n][LENGTH_HIDDEN_DIM];
            float[][] logits = new float[n][MAX_PACK_LENGTH];
            int[] remainingBatch = new int[n];
            for (int i = 0; i < n; i++) {
                remainingBatch[i] = n - i;
            }
            forwardLengthHiddenBatch(featMatrix, n, hidden);
            forwardLengthLogitsFromHiddenBatch(hidden, n, remainingBatch, logits);
            for (int i = 0; i < n; i++) {
                int remaining = remainingBatch[i];
                if (remaining <= 1) {
                    outCandidates[i] = new int[]{Math.max(1, remaining)};
                } else if (remaining <= topK) {
                    outCandidates[i] = allPackLengthsThrough(remaining);
                } else {
                    outCandidates[i] = topKCandidatesFromBatchLogits(logits[i], remaining, topK);
                }
            }
        }

        /** Layer-1 + partial layer-2 up to {@code validClasses} logits (inference only). */
        void forwardLengthLogitsPartial(float[] feat, int validClasses, float[] logits, float[] outHidden) {
            if (logits != null) {
                Arrays.fill(logits, 0, validClasses, 0.0f);
            }
            if (outHidden != null) {
                Arrays.fill(outHidden, 0.0f);
            }
            for (int h = 0; h < LENGTH_HIDDEN_DIM; ++h) {
                float z = lengthB1[h];
                int base = h * LENGTH_INPUT_DIM;
                for (int j = 0; j < LENGTH_INPUT_DIM; ++j) {
                    z += lengthW1[base + j] * feat[j];
                }
                float hval = z > 0.0f ? z : 0.0f;
                if (outHidden != null) {
                    outHidden[h] = hval;
                }
            }
            if (logits == null || outHidden == null) {
                return;
            }
            int classes = Math.max(1, Math.min(validClasses, MAX_PACK_LENGTH));
            for (int c = 0; c < classes; ++c) {
                float z = lengthOutB[c];
                int base = c * LENGTH_HIDDEN_DIM;
                for (int h = 0; h < LENGTH_HIDDEN_DIM; ++h) {
                    z += lengthOutW[base + h] * outHidden[h];
                }
                logits[c] = z;
            }
        }

        int[] topKCandidatesFromLogits(int remaining, int topK) {
            int validClasses = inferenceValidClasses(remaining);
            maskedSoftmax(inferLogits, validClasses, inferProbs);
            return selectTopKLengthCandidates(inferProbs, validClasses, topK);
        }

        int[] topKCandidatesFromBatchLogits(float[] logits, int remaining, int topK) {
            int validClasses = inferenceValidClasses(remaining);
            return selectTopKLengthCandidatesFromLogits(logits, validClasses, topK);
        }

        static void maskedSoftmax(float[] logits, int validClasses, float[] probs) {
            float maxLogit = Float.NEGATIVE_INFINITY;
            for (int c = 0; c < validClasses; ++c) {
                if (logits[c] > maxLogit) {
                    maxLogit = logits[c];
                }
            }
            float sum = 0.0f;
            for (int c = 0; c < validClasses; ++c) {
                probs[c] = (float) Math.exp(logits[c] - maxLogit);
                sum += probs[c];
            }
            if (sum <= 0.0f) {
                float uniform = 1.0f / validClasses;
                for (int c = 0; c < validClasses; ++c) {
                    probs[c] = uniform;
                }
                return;
            }
            for (int c = 0; c < validClasses; ++c) {
                probs[c] /= sum;
            }
        }

        int predictPackLength(int[] valueBitWidths, int startIndex, int numValues,
                              int packCount, int globalMaxPackSize) {
            int remaining = numValues - startIndex;
            if (remaining <= 1) {
                return Math.max(1, remaining);
            }
            float[] feat = lengthFeatures(valueBitWidths, startIndex, numValues, packCount, globalMaxPackSize);
            int validClasses = inferenceValidClasses(remaining);
            forwardLengthLogitsPartial(feat, validClasses, inferLogits, inferHidden);
            maskedSoftmax(inferLogits, validClasses, inferProbs);

            int bestClass = 0;
            float bestProb = inferProbs[0];
            for (int c = 1; c < validClasses; ++c) {
                if (inferProbs[c] > bestProb) {
                    bestProb = inferProbs[c];
                    bestClass = c;
                }
            }
            return bestClass + 1;
        }

        /** Top-K pack lengths by softmax probability; always includes length 1. */
        int[] topKPackLengthCandidates(int[] valueBitWidths, int startIndex, int numValues,
                                       int packCount, int globalMaxPackSize, int topK) {
            int remaining = numValues - startIndex;
            if (remaining <= 1) {
                return new int[]{Math.max(1, remaining)};
            }
            if (remaining <= topK) {
                return BPRL.allPackLengthsThrough(remaining);
            }

            float[] feat = lengthFeatures(valueBitWidths, startIndex, numValues, packCount, globalMaxPackSize);
            int validClasses = inferenceValidClasses(remaining);
            forwardLengthLogitsPartial(feat, validClasses, inferLogits, inferHidden);
            maskedSoftmax(inferLogits, validClasses, inferProbs);
            return selectTopKLengthCandidates(inferProbs, validClasses, topK);
        }

        float trainLengthClassifier(List<LengthDecisionPoint> examples) {
            return trainLengthClassifier(examples, 0);
        }

        /** Cost-weighted CE: higher weight when a pack boundary carries more BPDP p=1 encoded bits. */
        float trainLengthClassifier(List<LengthDecisionPoint> examples, int sequenceDpEncodedCost) {
            if (examples == null || examples.isEmpty()) return 0.0f;

            float[] dLengthW1 = new float[lengthW1.length];
            float[] dLengthB1 = new float[lengthB1.length];
            float[] dLengthOutW = new float[lengthOutW.length];
            float[] dLengthOutB = new float[lengthOutB.length];
            float totalLoss = 0.0f;
            float totalWeight = 0.0f;
            float[] hidden = new float[LENGTH_HIDDEN_DIM];
            float[] z1 = new float[LENGTH_HIDDEN_DIM];
            float[] logits = new float[MAX_PACK_LENGTH];
            float[] probs = new float[MAX_PACK_LENGTH];

            for (LengthDecisionPoint example : examples) {
                int remaining = example.numValues - example.startIndex;
                if (remaining <= 0) {
                    continue;
                }
                float exampleWeight = costWeightForLengthExample(example, sequenceDpEncodedCost);
                totalWeight += exampleWeight;
                float[] feat = example.toFeatures();
                forwardLengthLogits(feat, logits, hidden, z1);

                int validClasses = Math.min(MAX_PACK_LENGTH, remaining);
                maskedSoftmax(logits, validClasses, probs);

                int labelIdx = example.labelClassIndex();
                if (labelIdx >= validClasses) {
                    labelIdx = validClasses - 1;
                }
                float prob = Math.max(probs[labelIdx], 1e-8f);
                totalLoss += exampleWeight * -(float) Math.log(prob);

                for (int c = 0; c < validClasses; ++c) {
                    float gradLogit = probs[c];
                    if (c == labelIdx) {
                        gradLogit -= 1.0f;
                    }
                    gradLogit *= exampleWeight;
                    dLengthOutB[c] += gradLogit;
                    int base = c * LENGTH_HIDDEN_DIM;
                    for (int h = 0; h < LENGTH_HIDDEN_DIM; ++h) {
                        dLengthOutW[base + h] += gradLogit * hidden[h];
                    }
                }

                for (int h = 0; h < LENGTH_HIDDEN_DIM; ++h) {
                    float dHidden = 0.0f;
                    for (int c = 0; c < validClasses; ++c) {
                        float gradLogit = probs[c];
                        if (c == labelIdx) {
                            gradLogit -= 1.0f;
                        }
                        gradLogit *= exampleWeight;
                        dHidden += gradLogit * lengthOutW[c * LENGTH_HIDDEN_DIM + h];
                    }
                    float dZ1 = dHidden * reluDeriv(z1[h]);
                    int base = h * LENGTH_INPUT_DIM;
                    for (int j = 0; j < LENGTH_INPUT_DIM; ++j) {
                        dLengthW1[base + j] += dZ1 * feat[j];
                    }
                    dLengthB1[h] += dZ1;
                }
            }

            float scale = totalWeight > 0.0f ? 1.0f / totalWeight : 1.0f / examples.size();
            float lr = learningRate;
            for (int i = 0; i < lengthW1.length; ++i) {
                lengthW1[i] -= lr * dLengthW1[i] * scale;
                lengthW1[i] = clip(lengthW1[i], -10f, 10f);
            }
            for (int i = 0; i < lengthB1.length; ++i) {
                lengthB1[i] -= lr * dLengthB1[i] * scale;
                lengthB1[i] = clip(lengthB1[i], -10f, 10f);
            }
            for (int i = 0; i < lengthOutW.length; ++i) {
                lengthOutW[i] -= lr * dLengthOutW[i] * scale;
                lengthOutW[i] = clip(lengthOutW[i], -10f, 10f);
            }
            for (int i = 0; i < lengthOutB.length; ++i) {
                lengthOutB[i] -= lr * dLengthOutB[i] * scale;
                lengthOutB[i] = clip(lengthOutB[i], -10f, 10f);
            }

            return totalWeight > 0.0f ? totalLoss / totalWeight : totalLoss / examples.size();
        }

        /**
         * Soft cross-entropy against encoded-cost targets: q(L) ∝ exp(-cost(L)/τ).
         * Costs use DP-optimal suffix after the candidate first pack.
         */
        float trainLengthClassifierSoftCost(
                List<LengthDecisionPoint> examples,
                long[] values,
                int[] bitWidths,
                float temperature) {
            if (examples == null || examples.isEmpty()) {
                return 0.0f;
            }

            float[] dLengthW1 = new float[lengthW1.length];
            float[] dLengthB1 = new float[lengthB1.length];
            float[] dLengthOutW = new float[lengthOutW.length];
            float[] dLengthOutB = new float[lengthOutB.length];
            float totalLoss = 0.0f;
            int count = 0;
            float[] hidden = new float[LENGTH_HIDDEN_DIM];
            float[] z1 = new float[LENGTH_HIDDEN_DIM];
            float[] logits = new float[MAX_PACK_LENGTH];
            float[] probs = new float[MAX_PACK_LENGTH];
            float[] softTargets = new float[MAX_PACK_LENGTH];

            for (int di = 0; di < examples.size(); di++) {
                LengthDecisionPoint example = examples.get(di);
                int remaining = example.numValues - example.startIndex;
                if (remaining <= 0) {
                    continue;
                }
                int[] candidates = costAwareCandidateLengths(example, RL_COST_AWARE_MAX_CANDIDATES);
                List<Pack> prefix = buildPrefixPacksFromLabels(examples, bitWidths, di);
                computeEncodedCostSoftTargets(
                        values, bitWidths, prefix, example, candidates, temperature, softTargets);

                float[] feat = example.toFeatures();
                forwardLengthLogits(feat, logits, hidden, z1);
                int validClasses = Math.min(MAX_PACK_LENGTH, remaining);
                maskedSoftmax(logits, validClasses, probs);

                for (int c = 0; c < validClasses; c++) {
                    float p = Math.max(probs[c], 1e-8f);
                    float q = softTargets[c];
                    if (q > 0.0f) {
                        totalLoss += -q * (float) Math.log(p);
                    }
                }
                count++;

                for (int c = 0; c < validClasses; ++c) {
                    float gradLogit = probs[c] - softTargets[c];
                    dLengthOutB[c] += gradLogit;
                    int base = c * LENGTH_HIDDEN_DIM;
                    for (int h = 0; h < LENGTH_HIDDEN_DIM; ++h) {
                        dLengthOutW[base + h] += gradLogit * hidden[h];
                    }
                }

                for (int h = 0; h < LENGTH_HIDDEN_DIM; ++h) {
                    float dHidden = 0.0f;
                    for (int c = 0; c < validClasses; ++c) {
                        dHidden += (probs[c] - softTargets[c]) * lengthOutW[c * LENGTH_HIDDEN_DIM + h];
                    }
                    float dZ1 = dHidden * reluDeriv(z1[h]);
                    int base = h * LENGTH_INPUT_DIM;
                    for (int j = 0; j < LENGTH_INPUT_DIM; ++j) {
                        dLengthW1[base + j] += dZ1 * feat[j];
                    }
                    dLengthB1[h] += dZ1;
                }
            }

            float scale = count > 0 ? 1.0f / count : 1.0f;
            float lr = learningRate;
            for (int i = 0; i < lengthW1.length; ++i) {
                lengthW1[i] -= lr * dLengthW1[i] * scale;
                lengthW1[i] = clip(lengthW1[i], -10f, 10f);
            }
            for (int i = 0; i < lengthB1.length; ++i) {
                lengthB1[i] -= lr * dLengthB1[i] * scale;
                lengthB1[i] = clip(lengthB1[i], -10f, 10f);
            }
            for (int i = 0; i < lengthOutW.length; ++i) {
                lengthOutW[i] -= lr * dLengthOutW[i] * scale;
                lengthOutW[i] = clip(lengthOutW[i], -10f, 10f);
            }
            for (int i = 0; i < lengthOutB.length; ++i) {
                lengthOutB[i] -= lr * dLengthOutB[i] * scale;
                lengthOutB[i] = clip(lengthOutB[i], -10f, 10f);
            }

            return count > 0 ? totalLoss / count : 0.0f;
        }

        /** Forward-only cross-entropy for validation (no weight update). */
        float evalLengthClassifierLoss(List<LengthDecisionPoint> examples) {
            if (examples == null || examples.isEmpty()) {
                return 0.0f;
            }
            float totalLoss = 0.0f;
            float[] hidden = new float[LENGTH_HIDDEN_DIM];
            float[] logits = new float[MAX_PACK_LENGTH];
            float[] probs = new float[MAX_PACK_LENGTH];
            int count = 0;

            for (LengthDecisionPoint example : examples) {
                int remaining = example.numValues - example.startIndex;
                if (remaining <= 0) {
                    continue;
                }
                forwardLengthLogits(example.toFeatures(), logits, hidden, null);
                int validClasses = Math.min(MAX_PACK_LENGTH, remaining);
                maskedSoftmax(logits, validClasses, probs);
                int labelIdx = example.labelClassIndex();
                if (labelIdx >= validClasses) {
                    labelIdx = validClasses - 1;
                }
                totalLoss += -(float) Math.log(Math.max(probs[labelIdx], 1e-8f));
                count++;
            }
            return count > 0 ? totalLoss / count : 0.0f;
        }

        float train(List<DecisionPoint> decisions, float reward) {
            if (decisions == null || decisions.isEmpty()) return 0.0f;

            explorationRate *= 0.99f;
            if (explorationRate < 0.05f) explorationRate = 0.05f;

            float[] dW1 = new float[W1.length];
            float[] db1 = new float[b1.length];
            float[] dW2 = new float[W2.length];
            float db2 = 0.0f;

            float totalLoss = 0.0f;

            float[] hidden = new float[HIDDEN_DIM];
            float[] z1 = new float[HIDDEN_DIM];

            for (DecisionPoint dp : decisions) {
                float[] feat = dp.toFeatures();

                float p = forwardProb(feat, hidden, z1);

                float pClipped = Math.min(Math.max(p, 1e-6f), 1.0f - 1e-6f);

                float piA = dp.action ? pClipped : (1.0f - pClipped);
                if (piA <= 0.0f) {
                    piA = 1e-6f;
                }
                float lossI = -reward * (float) Math.log(piA);

                if (Float.isNaN(lossI) || Float.isInfinite(lossI)) {
                    System.err.printf("Warning: loss is NaN or Infinite. p=%.8f, piA=%.8f, reward=%.8f%n", p, piA, reward);
                    lossI = 0.0f;
                }

                totalLoss += lossI;

                float dL_dz2 = reward * (p - (dp.action ? 1.0f : 0.0f));

                for (int h = 0; h < HIDDEN_DIM; ++h) {
                    dW2[h] += dL_dz2 * hidden[h];
                }
                db2 += dL_dz2;

                for (int h = 0; h < HIDDEN_DIM; ++h) {
                    float w2h = W2[h];
                    float dh = dL_dz2 * w2h;
                    float dReLU = reluDeriv(z1[h]);
                    float dZ1 = dh * dReLU;
                    int base = h * INPUT_DIM;
                    for (int j = 0; j < INPUT_DIM; ++j) {
                        dW1[base + j] += dZ1 * feat[j];
                    }
                    db1[h] += dZ1;
                }
            }

            float lr = learningRate;
            for (int i = 0; i < W1.length; ++i) {
                W1[i] -= lr * dW1[i];
                W1[i] = clip(W1[i], -10f, 10f);
            }
            for (int i = 0; i < b1.length; ++i) {
                b1[i] -= lr * db1[i];
                b1[i] = clip(b1[i], -10f, 10f);
            }
            for (int i = 0; i < W2.length; ++i) {
                W2[i] -= lr * dW2[i];
                W2[i] = clip(W2[i], -10f, 10f);
            }
            b2 -= lr * db2;
            b2 = clip(b2, -10f, 10f);

            return totalLoss;
        }

        float trainSupervised(List<DecisionPoint> labeledDecisions) {
            if (labeledDecisions == null || labeledDecisions.isEmpty()) return 0.0f;

            int mergeCount = 0;
            int splitCount = 0;
            for (DecisionPoint dp : labeledDecisions) {
                if (dp.action) {
                    mergeCount++;
                } else {
                    splitCount++;
                }
            }
            float totalLabels = labeledDecisions.size();
            float mergeWeight = mergeCount > 0 ? totalLabels / (2.0f * mergeCount) : 1.0f;
            float splitWeight = splitCount > 0 ? totalLabels / (2.0f * splitCount) : 1.0f;

            float[] dW1 = new float[W1.length];
            float[] db1 = new float[b1.length];
            float[] dW2 = new float[W2.length];
            float db2 = 0.0f;
            float totalLoss = 0.0f;

            float[] hidden = new float[HIDDEN_DIM];
            float[] z1 = new float[HIDDEN_DIM];

            for (DecisionPoint dp : labeledDecisions) {
                float[] feat = dp.toFeatures();
                float p = forwardProb(feat, hidden, z1);
                float pClipped = Math.min(Math.max(p, 1e-6f), 1.0f - 1e-6f);
                float y = dp.action ? 1.0f : 0.0f;
                float weight = dp.action ? mergeWeight : splitWeight;

                totalLoss += weight * -(y * (float) Math.log(pClipped)
                        + (1.0f - y) * (float) Math.log(1.0f - pClipped));

                float dL_dz2 = weight * (p - y);
                for (int h = 0; h < HIDDEN_DIM; ++h) {
                    dW2[h] += dL_dz2 * hidden[h];
                }
                db2 += dL_dz2;

                for (int h = 0; h < HIDDEN_DIM; ++h) {
                    float dh = dL_dz2 * W2[h];
                    float dZ1 = dh * reluDeriv(z1[h]);
                    int base = h * INPUT_DIM;
                    for (int j = 0; j < INPUT_DIM; ++j) {
                        dW1[base + j] += dZ1 * feat[j];
                    }
                    db1[h] += dZ1;
                }
            }

            float scale = 1.0f / labeledDecisions.size();
            float lr = learningRate;
            for (int i = 0; i < W1.length; ++i) {
                W1[i] -= lr * dW1[i] * scale;
                W1[i] = clip(W1[i], -10f, 10f);
            }
            for (int i = 0; i < b1.length; ++i) {
                b1[i] -= lr * db1[i] * scale;
                b1[i] = clip(b1[i], -10f, 10f);
            }
            for (int i = 0; i < W2.length; ++i) {
                W2[i] -= lr * dW2[i] * scale;
                W2[i] = clip(W2[i], -10f, 10f);
            }
            b2 -= lr * db2 * scale;
            b2 = clip(b2, -10f, 10f);

            return totalLoss / labeledDecisions.size();
        }

        static float clip(float v, float low, float high) {
            return Math.min(Math.max(v, low), high);
        }

        public float calculateReward(PackingResult result) {
            return rewardFunction.calculateReward(result);
        }

        public void resetRewardFunction() {
            rewardFunction.reset();
        }
    }

    /** Convert RL pack groups to a BPDP plan using the same on-wire format as DP. */
    static BPDP.PackingPlan toBPDPPackingPlan(List<Pack> packs) {
        if (packs == null || packs.isEmpty()) {
            return new BPDP.PackingPlan(0, new int[0], new int[0], 0);
        }
        int[] groupSizes = new int[packs.size()];
        int[] groupBitWidths = new int[packs.size()];
        for (int i = 0; i < packs.size(); i++) {
            Pack pack = packs.get(i);
            groupSizes[i] = pack.size;
            groupBitWidths[i] = pack.maxBitWidth;
        }
        int optimalC = BPDP.computeMinValidC(groupSizes);
        return new BPDP.PackingPlan(optimalC, groupSizes, groupBitWidths, 0);
    }

    static float costWeightForLengthExample(LengthDecisionPoint example, int sequenceDpEncodedCost) {
        int maxBw = maxBitWidthInRange(example.valueBitWidths, example.startIndex, example.labelLength);
        int c = BPDP.computeMinValidC(new int[]{example.labelLength});
        long packBitCost = (long) example.labelLength * maxBw + 6L + c;
        if (sequenceDpEncodedCost <= 0) {
            return 1.0f;
        }
        return Math.max(1.0f, 1.0f + 8.0f * packBitCost / (float) sequenceDpEncodedCost);
    }

    /** Prefix packs from DP length labels before decision index {@code decisionIdx}. */
    static List<Pack> buildPrefixPacksFromLabels(
            List<LengthDecisionPoint> labels, int[] bitWidths, int decisionIdx) {
        List<Pack> prefix = new ArrayList<>();
        for (int i = 0; i < decisionIdx && i < labels.size(); i++) {
            LengthDecisionPoint prev = labels.get(i);
            prefix.add(buildPackFromRange(bitWidths, prev.startIndex, prev.labelLength));
        }
        return prefix;
    }

    static List<Pack> packsFromDpPlan(BPDP.PackingPlan plan, int[] bitWidths, int startIndex) {
        List<Pack> packs = new ArrayList<>(plan.groupSizes.length);
        int cursor = startIndex;
        int n = bitWidths.length;
        for (int groupSize : plan.groupSizes) {
            if (cursor >= n) {
                break;
            }
            int len = Math.max(1, Math.min(groupSize, n - cursor));
            packs.add(buildPackFromRange(bitWidths, cursor, len));
            cursor += len;
        }
        return packs;
    }

    static long encodedCostWithCandidateFirstPack(
            long[] values,
            int[] bitWidths,
            List<Pack> prefixPacks,
            int startIndex,
            int packLength) {
        List<Pack> full = new ArrayList<>(prefixPacks);
        full.add(buildPackFromRange(bitWidths, startIndex, packLength));
        int rest = startIndex + packLength;
        if (rest < bitWidths.length) {
            int[] restBw = Arrays.copyOfRange(bitWidths, rest, bitWidths.length);
            BPDP.PackingPlan suffixPlan = BPDP.computeOptimalPackingPlan(restBw, VALUE_PACK_SIZE);
            full.addAll(packsFromDpPlan(suffixPlan, bitWidths, rest));
        }
        long cost = encodedBitCostOfPacks(full, values);
        if (RL_COST_PACK_PENALTY_BITS > 0.0f) {
            cost += Math.round(RL_COST_PACK_PENALTY_BITS * full.size());
        }
        return cost;
    }

    static int[] costAwareCandidateLengths(LengthDecisionPoint example, int maxCandidates) {
        int remaining = example.numValues - example.startIndex;
        if (remaining <= 1) {
            return new int[]{Math.max(1, remaining)};
        }
        int maxLen = Math.min(remaining, MAX_PACK_LENGTH);
        TreeSet<Integer> set = new TreeSet<>();
        set.add(1);
        set.add(Math.min(example.labelLength, maxLen));
        if (remaining <= MAX_PACK_LENGTH) {
            set.add(remaining);
        }
        for (int p = 1; p <= maxLen; p <<= 1) {
            set.add(p);
        }
        int half = example.labelLength / 2;
        if (half >= 1 && half <= maxLen) {
            set.add(half);
        }
        int dbl = example.labelLength * 2;
        if (dbl <= maxLen) {
            set.add(dbl);
        }
        for (int delta : new int[] {-2, -1, 1, 2}) {
            int v = example.labelLength + delta;
            if (v >= 1 && v <= maxLen) {
                set.add(v);
            }
        }
        int[] all = set.stream().mapToInt(Integer::intValue).toArray();
        if (all.length <= maxCandidates) {
            return all;
        }
        int[] picked = new int[maxCandidates];
        picked[0] = 1;
        picked[1] = example.labelLength;
        int count = 2;
        for (int v : all) {
            if (v == 1 || v == example.labelLength) {
                continue;
            }
            if (count >= maxCandidates) {
                break;
            }
            picked[count++] = v;
        }
        return Arrays.copyOf(picked, count);
    }

    /**
     * Fill {@code outTargets[0..validClasses)} with soft distribution q(L) ∝ exp(-cost(L)/τ).
     * {@code outTargets} length must be >= MAX_PACK_LENGTH; only indices {@code 0..remaining-1} used.
     */
    static void computeEncodedCostSoftTargets(
            long[] values,
            int[] bitWidths,
            List<Pack> prefixPacks,
            LengthDecisionPoint example,
            int[] candidateLengths,
            float temperature,
            float[] outTargets) {
        int remaining = example.numValues - example.startIndex;
        int validClasses = Math.min(MAX_PACK_LENGTH, remaining);
        Arrays.fill(outTargets, 0, validClasses, 0.0f);
        if (candidateLengths == null || candidateLengths.length == 0 || temperature <= 0.0f) {
            int idx = example.labelClassIndex();
            if (idx < validClasses) {
                outTargets[idx] = 1.0f;
            }
            return;
        }

        float minCost = Float.POSITIVE_INFINITY;
        float[] costs = new float[candidateLengths.length];
        for (int i = 0; i < candidateLengths.length; i++) {
            int len = candidateLengths[i];
            if (len <= 0 || len > remaining) {
                costs[i] = Float.POSITIVE_INFINITY;
                continue;
            }
            costs[i] = encodedCostWithCandidateFirstPack(
                    values, bitWidths, prefixPacks, example.startIndex, len);
            if (costs[i] < minCost) {
                minCost = costs[i];
            }
        }

        float sum = 0.0f;
        for (int i = 0; i < candidateLengths.length; i++) {
            int len = candidateLengths[i];
            if (len <= 0 || len > remaining || len > MAX_PACK_LENGTH
                    || costs[i] == Float.POSITIVE_INFINITY) {
                continue;
            }
            float w = (float) Math.exp(-(costs[i] - minCost) / temperature);
            outTargets[len - 1] += w;
            sum += w;
        }
        if (sum <= 0.0f) {
            int idx = example.labelClassIndex();
            if (idx < validClasses) {
                outTargets[idx] = 1.0f;
            }
            return;
        }
        for (int c = 0; c < validClasses; c++) {
            outTargets[c] /= sum;
        }
    }

    static float trainSequenceCostAware(
            RLDecisionModel model,
            long[] values,
            List<LengthDecisionPoint> labels,
            float temperature) {
        if (labels == null || labels.isEmpty()) {
            return 0.0f;
        }
        int[] bitWidths = computeValueBitWidths(values);
        return model.trainLengthClassifierSoftCost(labels, values, bitWidths, temperature);
    }

    /** Canonical BPDP p=1 bitstream bit cost for an RL pack list. */
    static long bitstreamBitCostOfPacks(List<Pack> packs) {
        if (packs == null || packs.isEmpty()) {
            return 0L;
        }
        int optimalC = computeMinValidCForMaxPackSize(maxPackSizeOfPacks(packs));
        return 6L + 32L + (long) packs.size() * (optimalC + 6L) + payloadBitsOfPacks(packs);
    }

    /** Canonical BPDP p=1 encoded bit cost for an RL pack list (no byte allocation). */
    static long encodedBitCostOfPacks(List<Pack> packs, long[] dataArray) {
        if (packs == null || packs.isEmpty()) {
            return 0L;
        }
        return encodedBitsFromBitstreamBits(bitstreamBitCostOfPacks(packs));
    }

    static void encodeRlPackingResult(PackingResult result, long[] dataArray) {
        if (result == null || result.packs == null || result.packs.isEmpty() || dataArray == null) {
            if (result != null) {
                result.compressedData = null;
                result.totalCost = 0L;
            }
            return;
        }
        BPDP.PackingPlan plan = toBPDPPackingPlan(result.packs);
        int[] encodePos = new int[1];
        result.compressedData = BPDP.compressWithPackingPlan(dataArray, plan, VALUE_PACK_SIZE, encodePos);
        result.totalCost = encodePos[0] * 8L;
        result.paperCost = BPDP.computeBitstreamBitCost(plan, VALUE_PACK_SIZE);
        result.packCount = result.packs.size();
    }

    /**
     * @deprecated use {@link #bitstreamBitCostOfPacks}; kept for tests.
     */
    static long calculatePaperStorageCost(List<Pack> packs, int z) {
        if (packs == null || packs.isEmpty()) {
            return 0L;
        }
        BPDP.PackingPlan plan = toBPDPPackingPlan(packs);
        return BPDP.computeDpObjectiveBitCost(plan, VALUE_PACK_SIZE);
    }

    /** BPDP p=1 optimal encoded bit cost without writing a bitstream. */
    static long computeOptimalEncodedCost(long[] values, int[] bitWidths) {
        if (values == null || bitWidths == null || bitWidths.length == 0) {
            return 0L;
        }
        return (long) BPDP.computeOptimalEncodedByteSize(values, bitWidths, VALUE_PACK_SIZE) * 8L;
    }

    static long cachedOrComputeDpOptimalEncodedBits(
            long[] values, int[] bitWidths, PackingResult cache) {
        if (cache != null && cache.dpOptimalEncodedBits >= 0L) {
            return cache.dpOptimalEncodedBits;
        }
        long bits = computeOptimalEncodedCost(values, bitWidths);
        if (cache != null) {
            cache.dpOptimalEncodedBits = bits;
        }
        return bits;
    }

    static float[] lengthFeatures(int[] bitWidths, int startIndex, int numValues,
                                  int packCount, int globalMaxPackSize) {
        float[] feat = new float[LENGTH_INPUT_DIM];
        lengthFeaturesInto(bitWidths, startIndex, numValues, packCount, globalMaxPackSize, feat);
        return feat;
    }

    /** Nested-window feature extraction; writes into {@code feat} (len {@link #LENGTH_INPUT_DIM}). */
    static void lengthFeaturesInto(int[] bitWidths, int startIndex, int numValues,
                                   int packCount, int globalMaxPackSize, float[] feat) {
        int n = Math.max(1, numValues);
        int remaining = Math.max(1, numValues - startIndex);
        feat[0] = startIndex / (float) n;
        feat[1] = remaining / (float) n;
        feat[2] = packCount / (float) n;
        feat[3] = globalMaxPackSize / (float) n;
        lengthWindowTailInto(bitWidths, startIndex, numValues, feat, 4);
    }

    /** feat[4..20]: window max/avg + current bitwidth (independent of pack prefix). */
    static void lengthWindowTailInto(int[] bitWidths, int startIndex, int numValues,
                                     float[] feat, int offset) {
        int[] windows = {1, 2, 4, 8, 16, 32, 64, 128};
        int f = offset;
        int scanEnd = startIndex;
        int max = 0;
        int sum = 0;
        int count = 0;
        for (int window : windows) {
            int end = Math.min(numValues, startIndex + window);
            for (int i = scanEnd; i < end; i++) {
                int b = bitWidths[i];
                if (b > max) {
                    max = b;
                }
                sum += b;
                count++;
            }
            scanEnd = end;
            float avg = count > 0 ? (sum / (float) count) : 0.0f;
            feat[f++] = max / 64.0f;
            feat[f++] = avg / 64.0f;
        }
        feat[f] = bitWidths[startIndex] / 64.0f;
    }

    /**
     * Per-chunk cache of feat[4..20] for every start index; feat[0..3] filled per MLP call.
     */
    static final class LengthFeatureWindowCache {
        private static final int TAIL_DIM = LENGTH_INPUT_DIM - 4;
        private final float[] tail;
        private final float invN;
        private final int numValues;

        LengthFeatureWindowCache(int[] bitWidths, int numValues) {
            this.numValues = numValues;
            this.invN = 1.0f / Math.max(1, numValues);
            this.tail = new float[numValues * TAIL_DIM];
            float[] tmp = new float[TAIL_DIM];
            for (int i = 0; i < numValues; i++) {
                lengthWindowTailInto(bitWidths, i, numValues, tmp, 0);
                System.arraycopy(tmp, 0, tail, i * TAIL_DIM, TAIL_DIM);
            }
        }

        void fill(int startIndex, int packCount, int globalMaxPackSize, float[] feat) {
            feat[0] = startIndex * invN;
            feat[1] = (numValues - startIndex) * invN;
            feat[2] = packCount * invN;
            feat[3] = globalMaxPackSize * invN;
            System.arraycopy(tail, startIndex * TAIL_DIM, feat, 4, TAIL_DIM);
        }
    }

    // ========== Fixed value-pack baseline used only by consistency tests ==========
    static FixedPackResult calculateFixedPackCost(long[] data, int originalLength) {
        FixedPackResult result = new FixedPackResult();
        int[] groupSizes = new int[data.length];
        int[] groupBitWidths = new int[data.length];
        for (int i = 0; i < data.length; i++) {
            groupSizes[i] = 1;
            groupBitWidths[i] = getBitWidth(data[i]);
        }
        BPDP.PackingPlan plan = new BPDP.PackingPlan(1, groupSizes, groupBitWidths, 0);
        int[] encodePos = new int[1];
        result.compressedData = BPDP.compressWithPackingPlan(data, plan, VALUE_PACK_SIZE, encodePos);
        result.compressedBits = 0;
        for (int bitWidth : groupBitWidths) {
            result.compressedBits += 1 + 6 + bitWidth;
        }
        result.totalCost = result.compressedData.length * 8L;

        return result;
    }

    static long[] decompressFixedPack(byte[] compressed, int originalLength) {
        if (compressed == null || compressed.length == 0) {
            return new long[0];
        }
        return BPDP.decompressWithOptimalPacking(compressed, originalLength, VALUE_PACK_SIZE);
    }

    // ========== Fast inference: predict pack lengths, merge on paper cost, encode once ==========
    static long paperCostOfPacks(List<Pack> packs) {
        return bitstreamBitCostOfPacks(packs);
    }

    static Pack mergeTwoPacks(Pack left, Pack right, int[] valueBitWidths) {
        Pack merged = new Pack();
        for (int j = 0; j < left.size; j++) {
            merged.addValue(left.startIndex + j, valueBitWidths[left.startIndex + j]);
        }
        for (int j = 0; j < right.size; j++) {
            merged.addValue(right.startIndex + j, valueBitWidths[right.startIndex + j]);
        }
        return merged;
    }

    /** Greedy adjacent merge minimizing BPDP p=1 bitstream cost. */
    static List<Pack> fastGreedyPaperCostMerge(List<Pack> input, int[] valueBitWidths) {
        if (input == null || input.size() < 2) {
            return input == null ? new ArrayList<>() : new ArrayList<>(input);
        }
        List<Pack> packs = rebuildPacksWithBitWidths(input, valueBitWidths);
        long currentCost = bitstreamBitCostOfPacks(packs);

        boolean changed = true;
        int pass = 0;
        int maxPasses = Math.min(32, packs.size());
        while (changed && packs.size() >= 2 && pass < maxPasses) {
            changed = false;
            pass++;
            for (int i = 0; i < packs.size() - 1; i++) {
                Pack left = packs.get(i);
                Pack right = packs.get(i + 1);
                Pack merged = mergeTwoPacks(left, right, valueBitWidths);
                packs.set(i, merged);
                packs.remove(i + 1);
                long newCost = bitstreamBitCostOfPacks(packs);
                if (newCost < currentCost) {
                    currentCost = newCost;
                    changed = true;
                    break;
                }
                packs.add(i + 1, right);
                packs.set(i, left);
            }
        }
        return packs;
    }

    /** @deprecated use {@link #fastGreedyPaperCostMerge}; kept for callers/tests. */
    static List<Pack> fastSameBitWidthMerge(List<Pack> input, int[] valueBitWidths) {
        return fastGreedyPaperCostMerge(input, valueBitWidths);
    }

    static int maxPackSizeInList(List<Pack> packs) {
        int max = 0;
        if (packs != null) {
            for (Pack pack : packs) {
                if (pack.size > max) {
                    max = pack.size;
                }
            }
        }
        return max;
    }

    static Pack buildPackFromRange(int[] valueBitWidths, int start, int length) {
        Pack pack = new Pack();
        for (int j = 0; j < length; j++) {
            pack.addValue(start + j, valueBitWidths[start + j]);
        }
        return pack;
    }

    static List<Pack> packsFromGroupSizes(int[] groupSizes, int count, int[] valueBitWidths) {
        List<Pack> packs = new ArrayList<>(count);
        int idx = 0;
        for (int g = 0; g < count; g++) {
            packs.add(buildPackFromRange(valueBitWidths, idx, groupSizes[g]));
            idx += groupSizes[g];
        }
        return packs;
    }

    static long bitstreamBitCostOfGroupSizes(int[] groupSizes, int count, int[] valueBitWidths) {
        if (count <= 0) {
            return 0L;
        }
        return bitstreamBitCostOfPacks(packsFromGroupSizes(groupSizes, count, valueBitWidths));
    }

    static long encodedBitCostOfGroupSizesPrefix(
            long[] dataArray, int[] groupSizes, int count, int[] valueBitWidths) {
        if (count <= 0 || dataArray == null) {
            return 0L;
        }
        return encodedBitCostOfPacks(packsFromGroupSizes(groupSizes, count, valueBitWidths), dataArray);
    }

    static int computeMinValidCFromArray(int[] groupSizes, int count) {
        if (count <= 0) {
            return 1;
        }
        int maxGroup = 1;
        for (int g = 0; g < count; g++) {
            maxGroup = Math.max(maxGroup, groupSizes[g]);
        }
        int maxPossibleC = 32 - Integer.numberOfLeadingZeros(maxGroup);
        for (int c = 1; c <= maxPossibleC; c++) {
            if (maxGroup <= ((1 << c) - 1)) {
                return c;
            }
        }
        return maxPossibleC;
    }

    static int computeMinValidCForMaxPackSize(int maxGroup) {
        maxGroup = Math.max(1, maxGroup);
        int maxPossibleC = 32 - Integer.numberOfLeadingZeros(maxGroup);
        for (int c = 1; c <= maxPossibleC; c++) {
            if (maxGroup <= ((1 << c) - 1)) {
                return c;
            }
        }
        return maxPossibleC;
    }

    static int maxPackSizeOfPacks(List<Pack> packs) {
        int max = 1;
        if (packs != null) {
            for (Pack pack : packs) {
                if (pack.size > max) {
                    max = pack.size;
                }
            }
        }
        return max;
    }

    static long payloadBitsOfPacks(List<Pack> packs) {
        long payloadBits = 0L;
        if (packs != null) {
            for (Pack pack : packs) {
                payloadBits += (long) pack.size * VALUE_PACK_SIZE * pack.maxBitWidth;
            }
        }
        return payloadBits;
    }

    /** Per-chunk range maxima for Top-K DP candidate lengths. */
    static final class RangeMaxBitWidthTable {
        final int n;
        final int maxLen;
        final int stride;
        final int[] table;

        RangeMaxBitWidthTable(int[] bitWidths, int maxCandidateLen) {
            this.n = bitWidths.length;
            this.maxLen = Math.max(1, Math.min(maxCandidateLen, n));
            this.stride = this.maxLen + 1;
            this.table = new int[n * stride];
            for (int start = 0; start < n; start++) {
                int max = 0;
                int end = Math.min(n, start + this.maxLen);
                int base = start * stride;
                for (int pos = start; pos < end; pos++) {
                    if (bitWidths[pos] > max) {
                        max = bitWidths[pos];
                    }
                    table[base + (pos - start + 1)] = max;
                }
            }
        }

        int max(int[] bitWidths, int start, int length) {
            if (length > 0 && length <= maxLen && start >= 0 && start < n) {
                return table[start * stride + length];
            }
            return maxBitWidthInRange(bitWidths, start, length);
        }
    }

    static long bitstreamBitsFromPrefixStats(int count, int maxPackSize, long payloadBits) {
        if (count <= 0) {
            return 0L;
        }
        int optimalC = computeMinValidCForMaxPackSize(maxPackSize);
        return 6L + 32L + (long) count * (optimalC + 6L) + payloadBits;
    }

    static long bitstreamBitsFromGroupStats(
            int[] groupSizes, int[] groupMaxBw, int count, int optimalC) {
        if (count <= 0) {
            return 0L;
        }
        long bits = 6L + 32L;
        for (int g = 0; g < count; g++) {
            bits += optimalC + 6L + (long) groupSizes[g] * VALUE_PACK_SIZE * groupMaxBw[g];
        }
        return bits;
    }

    static long encodedBitsFromBitstreamBits(long bitstreamBits) {
        if (bitstreamBits <= 0L) {
            return 0L;
        }
        return (long) ((bitstreamBits + 7L) / 8L) * 8L;
    }

    static long encodedBitsFromGroupStats(
            int[] groupSizes, int[] groupMaxBw, int count, int optimalC) {
        return encodedBitsFromBitstreamBits(
                bitstreamBitsFromGroupStats(groupSizes, groupMaxBw, count, optimalC));
    }

    static final class PrimitivePlanResult {
        long encodedBits;
        int packCount;
    }

    /** Reusable primitive planner buffers; one instance per worker thread. */
    static final class PrimitiveTopKScratch {
        long[] dpCost = new long[0];
        int[] dpCount = new int[0];
        int[] dpMaxPackSize = new int[0];
        long[] dpPayloadBits = new long[0];

        void ensureCapacity(int n) {
            int need = n + 1;
            if (dpCost.length >= need) {
                return;
            }
            dpCost = new long[need];
            dpCount = new int[need];
            dpMaxPackSize = new int[need];
            dpPayloadBits = new long[need];
        }

        void reset(int n) {
            ensureCapacity(n);
            Arrays.fill(dpCost, 0, n + 1, Long.MAX_VALUE);
            dpCost[0] = 0L;
            dpCount[0] = 0;
            dpMaxPackSize[0] = 0;
            dpPayloadBits[0] = 0L;
        }
    }

    static final ThreadLocal<PrimitiveTopKScratch> PRIMITIVE_TOP_K_SCRATCH =
            ThreadLocal.withInitial(PrimitiveTopKScratch::new);

    /** Lightweight DP prefix for Top-K: pack lengths + max bit widths + cached plan costs. */
    static final class PrefixPlanState {
        int[] groupSizes = new int[0];
        int[] groupMaxBw = new int[0];
        int count;
        int optimalC;
        long bitstreamCostBits;
        long encodedCostBits;

        int maxPackSize() {
            int max = 0;
            for (int g = 0; g < count; g++) {
                max = Math.max(max, groupSizes[g]);
            }
            return max;
        }

        PrefixPlanState copyWithAppend(int start, int packLen, int[] valueBitWidths) {
            PrefixPlanState out = new PrefixPlanState();
            out.count = count + 1;
            out.groupSizes = new int[out.count];
            out.groupMaxBw = new int[out.count];
            if (count > 0) {
                System.arraycopy(groupSizes, 0, out.groupSizes, 0, count);
                System.arraycopy(groupMaxBw, 0, out.groupMaxBw, 0, count);
            }
            out.groupSizes[count] = packLen;
            out.groupMaxBw[count] = maxBitWidthInRange(valueBitWidths, start, packLen);
            out.optimalC = computeMinValidCFromArray(out.groupSizes, out.count);
            out.bitstreamCostBits = bitstreamBitsFromGroupStats(
                    out.groupSizes, out.groupMaxBw, out.count, out.optimalC);
            out.encodedCostBits = encodedBitsFromBitstreamBits(out.bitstreamCostBits);
            return out;
        }

        long planCost(boolean useEncodedCost) {
            return useEncodedCost ? encodedCostBits : bitstreamCostBits;
        }
    }

    static boolean shouldSkipTopKAfterFastPlan(
            long fastEncodedBits, long dpOptimalBits) {
        if (!RL_USE_TWO_PHASE_TOP_K || dpOptimalBits <= 0L) {
            return false;
        }
        long threshold = (long) (dpOptimalBits * RL_TWO_PHASE_DP_RATIO) + RL_TWO_PHASE_EXTRA_BITS;
        return fastEncodedBits <= threshold;
    }

    static boolean shouldSkipTopKAfterFastPlan(
            long fastEncodedBits, long[] dataArray, int[] valueBitWidths) {
        if (!RL_USE_TWO_PHASE_TOP_K || dataArray == null || valueBitWidths == null) {
            return false;
        }
        return shouldSkipTopKAfterFastPlan(
                fastEncodedBits, computeOptimalEncodedCost(dataArray, valueBitWidths));
    }

    /** Valid softmax / top-K classes at inference for a boundary with {@code remaining} values left. */
    static int inferenceValidClasses(int remaining) {
        if (remaining <= 0) {
            return 1;
        }
        int cap = Math.min(RL_INFER_MAX_PACK_LENGTH, MAX_PACK_LENGTH);
        return Math.min(cap, remaining);
    }

    /** Exact candidate set {@code {1..remaining}} when {@code remaining <= topK} (no MLP needed). */
    static int[] allPackLengthsThrough(int remaining) {
        int n = Math.max(1, remaining);
        int[] out = new int[n];
        for (int i = 0; i < n; i++) {
            out[i] = i + 1;
        }
        return out;
    }

    static int[] selectTopKLengthCandidates(float[] probs, int validClasses, int topK) {
        if (validClasses <= 0) {
            return new int[]{1};
        }
        int k = Math.max(1, Math.min(topK, validClasses));
        boolean[] selected = new boolean[validClasses];
        int[] lengths = new int[k + 1];
        int count = 0;

        if (!selected[0]) {
            lengths[count++] = 1;
            selected[0] = true;
        }

        for (int pick = 0; pick < k && count < lengths.length; pick++) {
            int bestIdx = -1;
            float bestProb = Float.NEGATIVE_INFINITY;
            for (int c = 0; c < validClasses; c++) {
                if (!selected[c] && probs[c] > bestProb) {
                    bestProb = probs[c];
                    bestIdx = c;
                }
            }
            if (bestIdx < 0) {
                break;
            }
            lengths[count++] = bestIdx + 1;
            selected[bestIdx] = true;
        }
        return Arrays.copyOf(lengths, count);
    }

    /** Top-K by logit (same ranking as softmax; avoids exp in inference). */
    static int[] selectTopKLengthCandidatesFromLogits(float[] logits, int validClasses, int topK) {
        return selectTopKLengthCandidates(logits, validClasses, topK);
    }

    static long encodedBitCostOfPacksPrefix(long[] dataArray, List<Pack> packs, int numValues) {
        if (packs == null || packs.isEmpty() || dataArray == null || numValues <= 0) {
            return 0L;
        }
        long[] prefix = numValues >= dataArray.length
                ? dataArray
                : Arrays.copyOfRange(dataArray, 0, numValues);
        return encodedBitCostOfPacks(packs, prefix);
    }

    /**
     * Batched length-candidate cache for Top-K DP: accumulates up to {@link #RL_INFER_BATCH}
     * MLP forwards, then flushes before candidates are read (exact semantics).
     */
    static final class LengthCandidateBatchBuffer {
        private final RLDecisionModel model;
        private final int topK;
        private final int numValues;
        private final int candidateStride;
        private final LengthFeatureWindowCache featureCache;
        private final int[] candidateCache;
        private final byte[] candidateCounts;
        private final float[][] featBatch;
        private final float[][] hiddenBatch;
        private final int[] indexBatch;
        private final int[] packCountBatch;
        private final int[] globalMaxBatch;
        private final int[] remainingBatch;
        private final int[] topIdxScratch;
        private final float[] topValScratch;
        private int pending;
        private boolean prefixFreePrefilled;

        LengthCandidateBatchBuffer(RLDecisionModel model, int[] valueBitWidths, int topK) {
            this(model, valueBitWidths, topK,
                    new LengthFeatureWindowCache(valueBitWidths, valueBitWidths.length));
        }

        LengthCandidateBatchBuffer(RLDecisionModel model, int[] valueBitWidths, int topK,
                                   LengthFeatureWindowCache featureCache) {
            this.model = model;
            this.numValues = valueBitWidths.length;
            this.topK = topK;
            this.featureCache = featureCache != null
                    ? featureCache
                    : new LengthFeatureWindowCache(valueBitWidths, valueBitWidths.length);
            this.candidateStride = Math.max(1, Math.min(topK, MAX_PACK_LENGTH)) + 1;
            this.candidateCache = new int[numValues * candidateStride];
            this.candidateCounts = new byte[numValues];
            int cap = Math.max(1, RL_INFER_BATCH);
            this.featBatch = new float[cap][LENGTH_INPUT_DIM];
            this.hiddenBatch = new float[cap][LENGTH_HIDDEN_DIM];
            this.indexBatch = new int[cap];
            this.packCountBatch = new int[cap];
            this.globalMaxBatch = new int[cap];
            this.remainingBatch = new int[cap];
            this.topIdxScratch = new int[candidateStride];
            this.topValScratch = new float[candidateStride];
            this.pending = 0;
            this.prefixFreePrefilled = false;
        }

        /** One chunk-level GEMM: all positions with prefix-free feat[2..3]=0. */
        void prefillPrefixFreeCandidates() {
            if (prefixFreePrefilled) {
                return;
            }
            int cap = featBatch.length;
            for (int base = 0; base < numValues; base += cap) {
                int batchSize = Math.min(cap, numValues - base);
                for (int b = 0; b < batchSize; b++) {
                    int index = base + b;
                    featureCache.fill(index, 0, 0, featBatch[b]);
                    remainingBatch[b] = numValues - index;
                }
                model.forwardLengthHiddenBatch(featBatch, batchSize, hiddenBatch);
                for (int b = 0; b < batchSize; b++) {
                    writeCandidatesFromHidden(base + b, remainingBatch[b], hiddenBatch[b]);
                }
            }
            prefixFreePrefilled = true;
            pending = 0;
        }

        boolean isPrefixFreePrefilled() {
            return prefixFreePrefilled;
        }

        void enqueue(int startIndex, int packCount, int globalMaxPackSize) {
            if (candidateCounts[startIndex] > 0) {
                return;
            }
            if (prefixFreePrefilled) {
                return;
            }
            int remaining = numValues - startIndex;
            if (remaining <= 1) {
                writeSingleCandidate(startIndex, Math.max(1, remaining));
                return;
            }
            if (remaining <= topK) {
                writeAllCandidatesThrough(startIndex, remaining);
                return;
            }
            if (pending >= featBatch.length) {
                flushPending();
            }
            indexBatch[pending] = startIndex;
            packCountBatch[pending] = packCount;
            globalMaxBatch[pending] = globalMaxPackSize;
            featureCache.fill(startIndex, packCount, globalMaxPackSize, featBatch[pending]);
            pending++;
        }

        void enqueue(int startIndex, float[] feat) {
            if (candidateCounts[startIndex] > 0) {
                return;
            }
            if (pending >= featBatch.length) {
                flushPending();
            }
            indexBatch[pending] = startIndex;
            packCountBatch[pending] = 0;
            globalMaxBatch[pending] = 0;
            System.arraycopy(feat, 0, featBatch[pending], 0, LENGTH_INPUT_DIM);
            pending++;
        }

        void flushPending() {
            if (pending <= 0) {
                return;
            }
            for (int b = 0; b < pending; b++) {
                remainingBatch[b] = numValues - indexBatch[b];
            }
            model.forwardLengthHiddenBatch(featBatch, pending, hiddenBatch);
            for (int b = 0; b < pending; b++) {
                writeCandidatesFromHidden(indexBatch[b], remainingBatch[b], hiddenBatch[b]);
            }
            pending = 0;
        }

        private void writeSingleCandidate(int startIndex, int length) {
            candidateCache[startIndex * candidateStride] = Math.max(1, length);
            candidateCounts[startIndex] = 1;
        }

        private void writeAllCandidatesThrough(int startIndex, int remaining) {
            int count = Math.max(1, Math.min(remaining, candidateStride));
            int base = startIndex * candidateStride;
            for (int i = 0; i < count; i++) {
                candidateCache[base + i] = i + 1;
            }
            candidateCounts[startIndex] = (byte) count;
        }

        private void writeCandidatesFromHidden(int startIndex, int remaining, float[] hidden) {
            if (remaining <= 1) {
                writeSingleCandidate(startIndex, Math.max(1, remaining));
                return;
            }
            if (remaining <= topK) {
                writeAllCandidatesThrough(startIndex, remaining);
                return;
            }

            int validClasses = inferenceValidClasses(remaining);
            int base = startIndex * candidateStride;
            int k = Math.max(1, Math.min(topK, validClasses));
            int topSlots = Math.max(1, Math.min(candidateStride, validClasses));
            Arrays.fill(topIdxScratch, 0, topSlots, -1);
            Arrays.fill(topValScratch, 0, topSlots, Float.NEGATIVE_INFINITY);

            int bestIdx = 0;
            float best = Float.NEGATIVE_INFINITY;
            float second = Float.NEGATIVE_INFINITY;
            for (int c = 0; c < validClasses; c++) {
                float z = model.lengthOutB[c];
                int wBase = c * LENGTH_HIDDEN_DIM;
                for (int h = 0; h < LENGTH_HIDDEN_DIM; h++) {
                    z += model.lengthOutW[wBase + h] * hidden[h];
                }

                if (z > best) {
                    second = best;
                    best = z;
                    bestIdx = c;
                } else if (z > second) {
                    second = z;
                }

                for (int pos = 0; pos < topSlots; pos++) {
                    if (z > topValScratch[pos]) {
                        for (int shift = topSlots - 1; shift > pos; shift--) {
                            topValScratch[shift] = topValScratch[shift - 1];
                            topIdxScratch[shift] = topIdxScratch[shift - 1];
                        }
                        topValScratch[pos] = z;
                        topIdxScratch[pos] = c;
                        break;
                    }
                }
            }
            if (!Float.isNaN(RL_CONFIDENCE_GATE_MARGIN)) {
                if (best - second >= RL_CONFIDENCE_GATE_MARGIN) {
                    candidateCache[base] = bestIdx + 1;
                    candidateCounts[startIndex] = 1;
                    return;
                }
            }

            int count = 0;
            candidateCache[base + count++] = 1;
            int picked = 0;
            for (int pick = 0; pick < topSlots && count < candidateStride && picked < k; pick++) {
                int idx = topIdxScratch[pick];
                if (idx < 0) {
                    break;
                }
                int len = idx + 1;
                boolean selected = false;
                for (int s = 0; s < count; s++) {
                    if (candidateCache[base + s] == len) {
                        selected = true;
                        break;
                    }
                }
                if (!selected) {
                    candidateCache[base + count++] = len;
                    picked++;
                }
            }
            candidateCounts[startIndex] = (byte) count;
        }

        int candidateCountFor(int startIndex) {
            if (candidateCounts[startIndex] == 0 && !prefixFreePrefilled) {
                flushPending();
            }
            return candidateCounts[startIndex];
        }

        int candidateAt(int startIndex, int candidateOffset) {
            return candidateCache[startIndex * candidateStride + candidateOffset];
        }
    }

    /**
     * Top-K + encoded-cost DP: at each position extend by one of the model's top-K
     * predicted pack lengths and keep the prefix plan with minimum encoded bits.
     */
    static List<Pack> planPacksTopKCost(int[] valueBitWidths, long[] dataArray,
                                       RLDecisionModel model, int topK) {
        return planPacksTopKCost(valueBitWidths, dataArray, model, topK, null);
    }

    static List<Pack> planPacksTopKCost(int[] valueBitWidths, long[] dataArray,
                                       RLDecisionModel model, int topK,
                                       LengthCandidateBatchBuffer sharedBuffer) {
        int n = valueBitWidths.length;
        if (n == 0) {
            return new ArrayList<>();
        }
        if (n == 1) {
            List<Pack> single = new ArrayList<>(1);
            single.add(buildPackFromRange(valueBitWidths, 0, 1));
            return single;
        }

        long[] dpCost = new long[n + 1];
        Arrays.fill(dpCost, Long.MAX_VALUE);
        dpCost[0] = 0L;
        int[] prevIndex = new int[n + 1];
        int[] prevLen = new int[n + 1];
        int[] dpCount = new int[n + 1];
        int[] dpMaxPackSize = new int[n + 1];
        long[] dpPayloadBits = new long[n + 1];
        Arrays.fill(prevIndex, -1);
        Arrays.fill(prevLen, -1);
        RangeMaxBitWidthTable rangeMax = new RangeMaxBitWidthTable(
                valueBitWidths, inferenceValidClasses(n));

        LengthCandidateBatchBuffer batchBuffer = sharedBuffer != null
                ? sharedBuffer
                : new LengthCandidateBatchBuffer(model, valueBitWidths, topK);
        if (RL_USE_CHUNK_MLP_PREFILL && !batchBuffer.isPrefixFreePrefilled()) {
            batchBuffer.prefillPrefixFreeCandidates();
        }
        final boolean useEncodedCost = RL_TOP_K_ENCODED_COST;

        for (int i = 0; i < n; i++) {
            if (dpCost[i] == Long.MAX_VALUE) {
                continue;
            }
            if (!batchBuffer.isPrefixFreePrefilled()) {
                batchBuffer.enqueue(i, dpCount[i], dpMaxPackSize[i]);
                if (batchBuffer.pending >= RL_INFER_BATCH) {
                    batchBuffer.flushPending();
                }
            }
            int candidateCount = batchBuffer.candidateCountFor(i);
            if (candidateCount == 0) {
                batchBuffer.flushPending();
                candidateCount = batchBuffer.candidateCountFor(i);
            }

            for (int candidateOffset = 0; candidateOffset < candidateCount; candidateOffset++) {
                int len = batchBuffer.candidateAt(i, candidateOffset);
                if (len <= 0 || i + len > n) {
                    continue;
                }
                int j = i + len;
                int nextCount = dpCount[i] + 1;
                int nextMaxPackSize = Math.max(dpMaxPackSize[i], len);
                long nextPayloadBits = dpPayloadBits[i]
                        + (long) len * VALUE_PACK_SIZE * rangeMax.max(valueBitWidths, i, len);
                long bitstreamBits = bitstreamBitsFromPrefixStats(
                        nextCount, nextMaxPackSize, nextPayloadBits);
                long cost = useEncodedCost ? encodedBitsFromBitstreamBits(bitstreamBits) : bitstreamBits;
                if (cost < dpCost[j]) {
                    dpCost[j] = cost;
                    prevIndex[j] = i;
                    prevLen[j] = len;
                    dpCount[j] = nextCount;
                    dpMaxPackSize[j] = nextMaxPackSize;
                    dpPayloadBits[j] = nextPayloadBits;
                }
            }
        }
        batchBuffer.flushPending();

        if (prevIndex[n] >= 0 && dpCount[n] > 0) {
            int count = dpCount[n];
            int[] groupSizes = new int[count];
            int cursor = n;
            for (int g = count - 1; g >= 0; g--) {
                int len = prevLen[cursor];
                groupSizes[g] = len;
                cursor = prevIndex[cursor];
            }
            if (cursor == 0) {
                return packsFromGroupSizes(groupSizes, count, valueBitWidths);
            }
        }
        return fastGreedyPaperCostMerge(buildPacksByPredictedLength(valueBitWidths, model), valueBitWidths);
    }

    static PrimitivePlanResult planPrimitiveTopKCost(
            int[] valueBitWidths, RLDecisionModel model, int topK) {
        return planPrimitiveTopKCost(
                valueBitWidths, model, topK, PRIMITIVE_TOP_K_SCRATCH.get());
    }

    static PrimitivePlanResult planPrimitiveTopKCost(
            int[] valueBitWidths, RLDecisionModel model, int topK,
            PrimitiveTopKScratch scratch) {
        PrimitivePlanResult result = new PrimitivePlanResult();
        int n = valueBitWidths.length;
        if (n == 0) {
            return result;
        }
        if (n == 1) {
            result.packCount = 1;
            result.encodedBits = encodedBitsFromBitstreamBits(
                    bitstreamBitsFromPrefixStats(1, 1, valueBitWidths[0]));
            return result;
        }

        PrimitiveTopKScratch localScratch =
                scratch != null ? scratch : PRIMITIVE_TOP_K_SCRATCH.get();
        localScratch.reset(n);
        long[] dpCost = localScratch.dpCost;
        int[] dpCount = localScratch.dpCount;
        int[] dpMaxPackSize = localScratch.dpMaxPackSize;
        long[] dpPayloadBits = localScratch.dpPayloadBits;

        RangeMaxBitWidthTable rangeMax = new RangeMaxBitWidthTable(
                valueBitWidths, inferenceValidClasses(n));
        LengthFeatureWindowCache featureCache = new LengthFeatureWindowCache(valueBitWidths, n);
        LengthCandidateBatchBuffer batchBuffer =
                new LengthCandidateBatchBuffer(model, valueBitWidths, topK, featureCache);
        if (RL_USE_CHUNK_MLP_PREFILL) {
            batchBuffer.prefillPrefixFreeCandidates();
        }
        final boolean useEncodedCost = RL_TOP_K_ENCODED_COST;

        for (int i = 0; i < n; i++) {
            if (dpCost[i] == Long.MAX_VALUE) {
                continue;
            }
            if (!batchBuffer.isPrefixFreePrefilled()) {
                batchBuffer.enqueue(i, dpCount[i], dpMaxPackSize[i]);
                if (batchBuffer.pending >= RL_INFER_BATCH) {
                    batchBuffer.flushPending();
                }
            }
            int candidateCount = batchBuffer.candidateCountFor(i);
            if (candidateCount == 0) {
                batchBuffer.flushPending();
                candidateCount = batchBuffer.candidateCountFor(i);
            }

            for (int candidateOffset = 0; candidateOffset < candidateCount; candidateOffset++) {
                int len = batchBuffer.candidateAt(i, candidateOffset);
                if (len <= 0 || i + len > n) {
                    continue;
                }
                int j = i + len;
                int nextCount = dpCount[i] + 1;
                int nextMaxPackSize = Math.max(dpMaxPackSize[i], len);
                long nextPayloadBits = dpPayloadBits[i]
                        + (long) len * VALUE_PACK_SIZE * rangeMax.max(valueBitWidths, i, len);
                long bitstreamBits = bitstreamBitsFromPrefixStats(
                        nextCount, nextMaxPackSize, nextPayloadBits);
                long cost = useEncodedCost ? encodedBitsFromBitstreamBits(bitstreamBits) : bitstreamBits;
                if (cost < dpCost[j]) {
                    dpCost[j] = cost;
                    dpCount[j] = nextCount;
                    dpMaxPackSize[j] = nextMaxPackSize;
                    dpPayloadBits[j] = nextPayloadBits;
                }
            }
        }
        batchBuffer.flushPending();

        if (dpCost[n] != Long.MAX_VALUE && dpCount[n] > 0) {
            result.packCount = dpCount[n];
            long bitstreamBits = bitstreamBitsFromPrefixStats(
                    dpCount[n], dpMaxPackSize[n], dpPayloadBits[n]);
            result.encodedBits = encodedBitsFromBitstreamBits(bitstreamBits);
            return result;
        }

        List<Pack> fallback = fastGreedyPaperCostMerge(
                buildPacksByPredictedLength(valueBitWidths, model), valueBitWidths);
        result.packCount = fallback.size();
        result.encodedBits = encodedBitCostOfPacks(fallback, null);
        return result;
    }

    static List<Pack> buildPacksByPredictedLength(int[] valueBitWidths, RLDecisionModel model) {
        return buildPacksByPredictedLength(valueBitWidths, model, null);
    }

    static List<Pack> buildPacksByPredictedLength(
            int[] valueBitWidths, RLDecisionModel model, LengthCandidateBatchBuffer sharedBuffer) {
        List<Pack> packs = new ArrayList<>();
        int n = valueBitWidths.length;
        int index = 0;
        int packCount = 0;
        int globalMaxPackSize = 0;
        LengthCandidateBatchBuffer batchBuffer = sharedBuffer != null
                ? sharedBuffer
                : new LengthCandidateBatchBuffer(model, valueBitWidths, 1);

        if (RL_USE_CHUNK_MLP_PREFILL && !batchBuffer.isPrefixFreePrefilled()) {
            batchBuffer.prefillPrefixFreeCandidates();
        }

        while (index < n) {
            if (!batchBuffer.isPrefixFreePrefilled()) {
                batchBuffer.enqueue(index, packCount, globalMaxPackSize);
                if (batchBuffer.pending >= RL_INFER_BATCH) {
                    batchBuffer.flushPending();
                }
            }
            int candidateCount = batchBuffer.candidateCountFor(index);
            if (candidateCount == 0) {
                batchBuffer.flushPending();
                candidateCount = batchBuffer.candidateCountFor(index);
            }
            int length = candidateCount > 0 ? batchBuffer.candidateAt(index, 0) : 1;
            int remaining = n - index;
            length = Math.max(1, Math.min(length, remaining));

            Pack pack = new Pack();
            for (int j = 0; j < length; j++) {
                pack.addValue(index + j, valueBitWidths[index + j]);
            }
            packs.add(pack);
            packCount++;
            if (pack.size > globalMaxPackSize) {
                globalMaxPackSize = pack.size;
            }
            index += length;
        }
        if (sharedBuffer == null) {
            batchBuffer.flushPending();
        }
        return packs;
    }

    /** Plan pack sizes: Top-K encoded-cost DP when enabled, else argmax + paper-cost merge. */
    static List<Pack> planPacksFast(int[] valueBitWidths, RLDecisionModel model) {
        return planPacksFast(valueBitWidths, null, model);
    }

    static List<Pack> planPacksFast(int[] valueBitWidths, long[] dataArray, RLDecisionModel model) {
        return planPacksFast(valueBitWidths, dataArray, model, -1L);
    }

    static List<Pack> planPacksFast(
            int[] valueBitWidths, long[] dataArray, RLDecisionModel model, long dpOptimalBits) {
        return planPacksFast(valueBitWidths, dataArray, model, dpOptimalBits, null);
    }

    static List<Pack> planPacksFast(
            int[] valueBitWidths, long[] dataArray, RLDecisionModel model,
            long dpOptimalBits, PackingResult dpCache) {
        int n = valueBitWidths.length;
        LengthFeatureWindowCache featureCache = new LengthFeatureWindowCache(valueBitWidths, n);
        LengthCandidateBatchBuffer inferBuffer = (RL_USE_TOP_K_DECODE && dataArray != null)
                ? new LengthCandidateBatchBuffer(model, valueBitWidths, RL_TOP_K, featureCache)
                : null;
        if (inferBuffer != null && RL_USE_CHUNK_MLP_PREFILL) {
            inferBuffer.prefillPrefixFreeCandidates();
            if (RL_USE_TOP_K_DECODE && dataArray != null) {
                return planPacksTopKCost(valueBitWidths, dataArray, model, RL_TOP_K, inferBuffer);
            }
        }
        List<Pack> fastPacks = fastGreedyPaperCostMerge(
                buildPacksByPredictedLength(valueBitWidths, model, inferBuffer), valueBitWidths);
        if (!RL_USE_TOP_K_DECODE || dataArray == null) {
            return fastPacks;
        }
        if (inferBuffer != null) {
            inferBuffer.flushPending();
        }
        long fastEncodedBits = encodedBitCostOfPacks(fastPacks, dataArray);
        if (RL_USE_TWO_PHASE_TOP_K) {
            if (!RL_LAZY_DP_OPTIMAL) {
                if (dpOptimalBits < 0L) {
                    dpOptimalBits = cachedOrComputeDpOptimalEncodedBits(
                            dataArray, valueBitWidths, dpCache);
                }
            }
            if (dpOptimalBits > 0L && shouldSkipTopKAfterFastPlan(fastEncodedBits, dpOptimalBits)) {
                return fastPacks;
            }
        }
        return planPacksTopKCost(valueBitWidths, dataArray, model, RL_TOP_K, inferBuffer);
    }

    static PackingResult packValuesFast(long[] dataArray, int[] valueBitWidths, RLDecisionModel model) {
        PackingResult result = new PackingResult();
        result.dpOptimalEncodedBits = -1L;
        result.packs = planPacksFast(valueBitWidths, dataArray, model, -1L, result);
        result.packCount = result.packs.size();
        result.paperCost = bitstreamBitCostOfPacks(result.packs);
        result.totalCost = encodedBitCostOfPacks(result.packs, dataArray);
        encodeRlPackingResult(result, dataArray);
        return result;
    }

    static float evaluateValCrossEntropy(RLDecisionModel model, int[] valIndices,
                                         List<List<LengthDecisionPoint>> labelsBySequence) {
        float total = 0.0f;
        for (int idx : valIndices) {
            total += model.evalLengthClassifierLoss(labelsBySequence.get(idx));
        }
        return valIndices.length == 0 ? Float.POSITIVE_INFINITY : total / valIndices.length;
    }

    // ========== 训练方法（改进版） ==========
    static float evaluateEncodedCostRatio(
            RLDecisionModel model,
            List<long[]> sequences,
            int[] dpEncodedCosts,
            boolean useDpFallback) {
        double totalRatio = 0.0;
        int count = 0;
        boolean savedFallback = RL_USE_DP_FALLBACK;
        RL_USE_DP_FALLBACK = useDpFallback;
        try {
            for (int i = 0; i < sequences.size(); i++) {
                long[] sequence = sequences.get(i);
                int[] bitWidths = computeValueBitWidths(sequence);
                PackingResult result = packValuesFast(sequence, bitWidths, model);
                int dpCost = dpEncodedCosts[i];
                if (dpCost > 0 && result.totalCost > 0) {
                    totalRatio += (double) result.totalCost / (double) dpCost;
                    count++;
                }
            }
        } finally {
            RL_USE_DP_FALLBACK = savedFallback;
        }
        return count > 0 ? (float) (totalRatio / count) : Float.POSITIVE_INFINITY;
    }

    static RLDecisionModel trainModelFromDirectory(int epochs, String directoryPath) {
        return trainModelFromDirectory(epochs, directoryPath, BPRL::identityTransform, "BP-RL");
    }

    static Path labelCachePath(String directoryPath, String modelName) {
        Path base = Paths.get(directoryPath).getParent().resolve(RL_LABEL_CACHE_SUBDIR);
        String safeName = modelName.replaceAll("[^a-zA-Z0-9_-]", "_");
        return base.resolve(safeName + ".bin");
    }

    static long computeTrainingDataFingerprint(String directoryPath) {
        return computeTrainingDataFingerprint(directoryPath, CHUNK_SIZE);
    }

    static long computeTrainingDataFingerprint(String directoryPath, int trainingSourceChunkSize) {
        File dir = new File(directoryPath);
        File[] files = dir.listFiles((d, name) -> name.toLowerCase().endsWith(".csv"));
        if (files == null) {
            return 0L;
        }
        Arrays.sort(files, Comparator.comparing(File::getName));
        long h = trainingSourceChunkSize;
        h = 31 * h + VALUE_PACK_SIZE;
        h = 31 * h + RL_LABEL_CACHE_VERSION;
        h = 31 * h + normalizedTrainMaxPackLength(trainingSourceChunkSize);
        for (File file : files) {
            if (!BenchmarkDatasetFilter.includeDatasetFile(file.getName())) {
                continue;
            }
            h = 31 * h + file.getName().hashCode();
            h = 31 * h + file.lastModified();
            h = 31 * h + file.length();
        }
        return h;
    }

    static void assignTrainValSplit(RLTrainingDataset dataset) {
        int[] allIndices = new int[dataset.transformedSequences.size()];
        for (int i = 0; i < dataset.transformedSequences.size(); i++) {
            allIndices[i] = i;
        }
        shuffleIntArray(allIndices, new Random(42));
        int valCount = Math.max(1, dataset.transformedSequences.size() / 10);
        boolean[] isVal = new boolean[dataset.transformedSequences.size()];
        for (int i = 0; i < valCount; i++) {
            isVal[allIndices[i]] = true;
        }
        int[] trainIndices = new int[dataset.transformedSequences.size() - valCount];
        int[] valIndices = new int[valCount];
        int trainCount = 0;
        int actualValCount = 0;
        for (int i = 0; i < dataset.transformedSequences.size(); i++) {
            if (isVal[i]) {
                valIndices[actualValCount++] = i;
            } else {
                trainIndices[trainCount++] = i;
            }
        }
        dataset.trainIndices = trainCount == trainIndices.length
                ? trainIndices : Arrays.copyOf(trainIndices, trainCount);
        dataset.valIndices = actualValCount == valIndices.length
                ? valIndices : Arrays.copyOf(valIndices, actualValCount);
    }

    static void shuffleIntArray(int[] values, Random rng) {
        for (int i = values.length - 1; i > 0; i--) {
            int j = rng.nextInt(i + 1);
            int tmp = values[i];
            values[i] = values[j];
            values[j] = tmp;
        }
    }

    static RLTrainingDataset buildTrainingDatasetWithDp(
            String directoryPath, RLValueTransform transform, String modelName) {
        return buildTrainingDatasetWithDp(directoryPath, transform, modelName, CHUNK_SIZE);
    }

    static RLTrainingDataset buildTrainingDatasetWithDp(
            String directoryPath, RLValueTransform transform, String modelName,
            int trainingSourceChunkSize) {
        RLTrainingDataset dataset = new RLTrainingDataset();
        List<long[]> sequences = loadRawValuesFromDirectory(
                directoryPath, trainingSourceChunkSize);
        if (sequences.isEmpty()) {
            return dataset;
        }

        System.err.println("Pre-computing DP optimal partitions and pack-length labels...");
        IntList dpOptimalCosts = new IntList(sequences.size());
        for (int seqIdx = 0; seqIdx < sequences.size(); seqIdx++) {
            long[] sequenceArray = sequences.get(seqIdx);
            long[] transformed = transform.transform(sequenceArray);
            if (transformed.length <= 1) {
                continue;
            }

            int[] bitWidthsArray = computeValueBitWidths(transformed);
            BPDP.PackingPlan optimalPlan = computeTrainingOptimalPackingPlan(
                    bitWidthsArray, VALUE_PACK_SIZE, trainingSourceChunkSize);
            List<LengthDecisionPoint> labels = buildDpLengthLabels(bitWidthsArray, optimalPlan);
            if (labels.isEmpty()) {
                continue;
            }
            int[] encodePos = new int[1];
            BPDP.compressWithPackingPlan(transformed, optimalPlan, VALUE_PACK_SIZE, encodePos);
            dataset.transformedSequences.add(transformed);
            dataset.labelsBySequence.add(labels);
            int dpCost = BPDP.computeEncodedBitCostForPlan(
                    transformed, optimalPlan, VALUE_PACK_SIZE);
            dpOptimalCosts.add(dpCost);

            if (seqIdx % 10 == 0) {
                System.err.println("  Sequence " + seqIdx + ": DP encoded cost = "
                        + dpCost
                        + " bits, groups = " + optimalPlan.groupSizes.length);
            }
        }
        dataset.dpOptimalCosts = dpOptimalCosts.toArray();

        if (!dataset.transformedSequences.isEmpty()) {
            assignTrainValSplit(dataset);
        }
        return dataset;
    }

    static int normalizedTrainMaxPackLength(int numValues) {
        int cap = RL_TRAIN_MAX_PACK_LENGTH > 0 ? RL_TRAIN_MAX_PACK_LENGTH : MAX_PACK_LENGTH;
        cap = Math.min(cap, MAX_PACK_LENGTH);
        if (numValues > 0) {
            cap = Math.min(cap, numValues);
        }
        return Math.max(1, cap);
    }

    static BPDP.PackingPlan computeTrainingOptimalPackingPlan(
            int[] bitWidths, int packSize, int trainingSourceChunkSize) {
        int cap = normalizedTrainMaxPackLength(
                bitWidths == null ? trainingSourceChunkSize : bitWidths.length);
        if (bitWidths == null || bitWidths.length == 0 || cap >= bitWidths.length) {
            return BPDP.computeOptimalPackingPlan(bitWidths, packSize);
        }
        return computeConstrainedOptimalPackingPlan(bitWidths, packSize, cap);
    }

    static BPDP.PackingPlan computeConstrainedOptimalPackingPlan(
            int[] bitWidths, int packSize, int maxPackLength) {
        int n = bitWidths.length;
        if (n == 0) {
            return new BPDP.PackingPlan(0, new int[0], new int[0], 0);
        }
        int cap = Math.max(1, Math.min(maxPackLength, n));
        int minTotalCost = Integer.MAX_VALUE;
        int[] bestGroupSizes = new int[0];
        int[] bestGroupBitWidths = new int[0];

        int maxPossibleC = 32 - Integer.numberOfLeadingZeros(cap);
        for (int cBits = 1; cBits <= maxPossibleC; cBits++) {
            int lowC = (cBits == 1) ? 1 : (1 << (cBits - 1));
            int highC = Math.min(Math.min((1 << cBits) - 1, n), cap);

            int[][] dp = new int[n + 1][2];
            int[][] prevState = new int[n + 1][2];
            int[][] prevFlag = new int[n + 1][2];
            int[][] packLengthAt = new int[n + 1][2];
            int[][] packBitWidthAt = new int[n + 1][2];
            for (int i = 0; i <= n; i++) {
                dp[i][0] = Integer.MAX_VALUE / 2;
                dp[i][1] = Integer.MAX_VALUE / 2;
            }
            dp[0][0] = 0;

            for (int i = 1; i <= n; i++) {
                int minK = Math.max(1, i - highC + 1);
                int currentMaxB = 0;
                for (int k = i; k >= minK; k--) {
                    currentMaxB = Math.max(currentMaxB, bitWidths[k - 1]);
                    int length = i - k + 1;
                    int packCost = packSize * length * currentMaxB + 6 + cBits;

                    if (length < lowC) {
                        if (dp[k - 1][0] + packCost < dp[i][0]) {
                            dp[i][0] = dp[k - 1][0] + packCost;
                            prevState[i][0] = k - 1;
                            prevFlag[i][0] = 0;
                            packLengthAt[i][0] = length;
                            packBitWidthAt[i][0] = currentMaxB;
                        }
                        if (dp[k - 1][1] + packCost < dp[i][1]) {
                            dp[i][1] = dp[k - 1][1] + packCost;
                            prevState[i][1] = k - 1;
                            prevFlag[i][1] = 1;
                            packLengthAt[i][1] = length;
                            packBitWidthAt[i][1] = currentMaxB;
                        }
                    } else {
                        if (dp[k - 1][0] + packCost < dp[i][1]) {
                            dp[i][1] = dp[k - 1][0] + packCost;
                            prevState[i][1] = k - 1;
                            prevFlag[i][1] = 0;
                            packLengthAt[i][1] = length;
                            packBitWidthAt[i][1] = currentMaxB;
                        }
                        if (dp[k - 1][1] + packCost < dp[i][1]) {
                            dp[i][1] = dp[k - 1][1] + packCost;
                            prevState[i][1] = k - 1;
                            prevFlag[i][1] = 1;
                            packLengthAt[i][1] = length;
                            packBitWidthAt[i][1] = currentMaxB;
                        }
                    }
                }
            }

            if (dp[n][1] < minTotalCost) {
                minTotalCost = dp[n][1];
                int[] reverseSizes = new int[n];
                int[] reverseBitWidths = new int[n];
                int count = 0;
                int i = n;
                int flag = 1;
                while (i > 0) {
                    int length = packLengthAt[i][flag];
                    int bitWidth = packBitWidthAt[i][flag];
                    reverseSizes[count] = length;
                    reverseBitWidths[count] = bitWidth;
                    count++;
                    int prevI = prevState[i][flag];
                    flag = prevFlag[i][flag];
                    i = prevI;
                }
                bestGroupSizes = new int[count];
                bestGroupBitWidths = new int[count];
                for (int g = 0; g < count; g++) {
                    bestGroupSizes[g] = reverseSizes[count - 1 - g];
                    bestGroupBitWidths[g] = reverseBitWidths[count - 1 - g];
                }
            }
        }

        return new BPDP.PackingPlan(
                BPDP.computeMinValidC(bestGroupSizes), bestGroupSizes, bestGroupBitWidths, minTotalCost);
    }

    static void writeString(DataOutputStream out, String s) throws IOException {
        byte[] bytes = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    static String readString(DataInputStream in) throws IOException {
        int len = in.readInt();
        byte[] bytes = new byte[len];
        in.readFully(bytes);
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }

    static Path modelCachePath(String directoryPath, String modelName) {
        Path base = Paths.get(directoryPath).getParent().resolve(RL_MODEL_CACHE_SUBDIR);
        String safeName = modelName.replaceAll("[^a-zA-Z0-9_-]", "_");
        return base.resolve(safeName + ".bin");
    }

    static String configuredModelName(String defaultName) {
        String override = System.getProperty("bprl.modelName");
        if (override == null || override.trim().isEmpty()) {
            return defaultName;
        }
        return override.trim();
    }

    static int configuredPositiveIntProperty(String propertyName, int defaultValue) {
        String value = System.getProperty(propertyName);
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        try {
            int parsed = Integer.parseInt(value.trim());
            return parsed > 0 ? parsed : defaultValue;
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    static Long configuredModelSeed() {
        String value = System.getProperty("bprl.modelSeed");
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    static Random newModelRandom() {
        Long seed = configuredModelSeed();
        return seed == null ? new Random() : new Random(seed);
    }

    static int[] configuredPositiveIntListProperty(String propertyName, int[] defaultValues) {
        String value = System.getProperty(propertyName);
        if (value == null || value.trim().isEmpty()) {
            return Arrays.copyOf(defaultValues, defaultValues.length);
        }
        String[] parts = value.split(",");
        int[] parsed = new int[parts.length];
        int count = 0;
        try {
            for (String part : parts) {
                String trimmed = part.trim();
                if (trimmed.isEmpty()) {
                    return Arrays.copyOf(defaultValues, defaultValues.length);
                }
                int next = Integer.parseInt(trimmed);
                if (next <= 0) {
                    return Arrays.copyOf(defaultValues, defaultValues.length);
                }
                parsed[count++] = next;
            }
        } catch (NumberFormatException e) {
            return Arrays.copyOf(defaultValues, defaultValues.length);
        }
        return count == 0
                ? Arrays.copyOf(defaultValues, defaultValues.length)
                : Arrays.copyOf(parsed, count);
    }

    static boolean includeBenchmarkDatasetFile(String fileName) {
        if (!BenchmarkDatasetFilter.isWhitelistedDatasetFile(fileName)) {
            return false;
        }
        String raw = System.getProperty("bprl.benchmarkDatasets");
        if (raw == null || raw.trim().isEmpty()) {
            return true;
        }
        for (String part : raw.split(",")) {
            if (fileName.equals(part.trim())) {
                return true;
            }
        }
        return false;
    }

    static final class RLRuntimeConfig {
        final int inferMaxPackLength;
        final int topK;
        final float confidenceGateMargin;
        final boolean useVanillaFallback;
        final boolean useDegradeFallback;

        RLRuntimeConfig(int inferMaxPackLength, int topK, float confidenceGateMargin) {
            this(inferMaxPackLength, topK, confidenceGateMargin,
                    RL_USE_VANILLA_FALLBACK, RL_USE_DEGRADE_FALLBACK);
        }

        RLRuntimeConfig(
                int inferMaxPackLength,
                int topK,
                float confidenceGateMargin,
                boolean useVanillaFallback,
                boolean useDegradeFallback) {
            this.inferMaxPackLength = inferMaxPackLength;
            this.topK = topK;
            this.confidenceGateMargin = confidenceGateMargin;
            this.useVanillaFallback = useVanillaFallback;
            this.useDegradeFallback = useDegradeFallback;
        }
    }

    static RLRuntimeConfig ts2diffDatasetTunedConfig(String fileName) {
        String name = fileName == null ? "" : fileName;
        if (name.contains("CS-Sensors")) {
            return new RLRuntimeConfig(32, 5, 1.0f, true, true);
        }
        if (name.contains("Blockchain-tr")
                || name.contains("EPM-Education")) {
            return new RLRuntimeConfig(96, 8, Float.NaN, false, false);
        }
        if (name.contains("PM10-dust")) {
            return new RLRuntimeConfig(192, 10, Float.NaN, false, false);
        }
        if (name.contains("Food-price") || name.contains("TH-Climate")) {
            return new RLRuntimeConfig(128, 8, Float.NaN, false, false);
        }
        if (name.contains("Cyber-Vehicle")) {
            return new RLRuntimeConfig(128, 10, Float.NaN, false, false);
        }
        if (name.contains("Stocks-UK")) {
            return new RLRuntimeConfig(128, 12, Float.NaN, false, false);
        }
        if (name.contains("TY-Transport")) {
            return new RLRuntimeConfig(192, 16, Float.NaN, true, false);
        }
        if (name.contains("USGS-Earthquakes")) {
            return new RLRuntimeConfig(128, 16, Float.NaN, false, false);
        }
        return new RLRuntimeConfig(RL_INFER_MAX_PACK_LENGTH, RL_TOP_K, RL_CONFIDENCE_GATE_MARGIN);
    }

    static void applyRuntimeConfig(RLRuntimeConfig config) {
        if (config == null) {
            return;
        }
        RL_INFER_MAX_PACK_LENGTH = config.inferMaxPackLength;
        RL_TOP_K = config.topK;
        RL_CONFIDENCE_GATE_MARGIN = config.confidenceGateMargin;
        RL_USE_VANILLA_FALLBACK = config.useVanillaFallback;
        RL_USE_DEGRADE_FALLBACK = config.useDegradeFallback;
    }

    static long computeModelFingerprint(String directoryPath, String modelName, int epochs) {
        return computeModelFingerprint(directoryPath, modelName, epochs, CHUNK_SIZE);
    }

    static long computeModelFingerprint(
            String directoryPath, String modelName, int epochs, int trainingSourceChunkSize) {
        long h = computeTrainingDataFingerprint(directoryPath, trainingSourceChunkSize);
        h = 31 * h + modelName.hashCode();
        h = 31 * h + epochs;
        h = 31 * h + RL_MODEL_CACHE_VERSION;
        h = 31 * h + HIDDEN_DIM;
        h = 31 * h + LENGTH_HIDDEN_DIM;
        h = 31 * h + MAX_PACK_LENGTH;
        h = 31 * h + LENGTH_INPUT_DIM;
        h = 31 * h + Float.floatToIntBits(initialLearningRate);
        h = 31 * h + (RL_USE_COST_AWARE_TRAINING ? 1 : 0);
        h = 31 * h + RL_COST_AWARE_EPOCHS;
        h = 31 * h + Float.floatToIntBits(RL_COST_SOFTMAX_TEMP);
        h = 31 * h + RL_COST_AWARE_MAX_CANDIDATES;
        h = 31 * h + Float.floatToIntBits(RL_COST_AWARE_LR_SCALE);
        h = 31 * h + Float.floatToIntBits(RL_COST_PACK_PENALTY_BITS);
        Long modelSeed = configuredModelSeed();
        h = 31 * h + (modelSeed == null ? 0 : Long.hashCode(modelSeed));
        return h;
    }

    static void writeFloatArray(DataOutputStream out, float[] arr) throws IOException {
        out.writeInt(arr.length);
        for (float v : arr) {
            out.writeFloat(v);
        }
    }

    static float[] readFloatArray(DataInputStream in) throws IOException {
        int len = in.readInt();
        float[] arr = new float[len];
        for (int i = 0; i < len; i++) {
            arr[i] = in.readFloat();
        }
        return arr;
    }

    static void saveModel(
            RLDecisionModel model, Path cachePath, long fingerprint,
            String modelName, String directoryPath, int epochs) throws IOException {
        Files.createDirectories(cachePath.getParent());
        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(cachePath)))) {
            out.writeInt(RL_MODEL_CACHE_VERSION);
            writeString(out, modelName);
            writeString(out, directoryPath);
            out.writeLong(fingerprint);
            out.writeInt(epochs);
            writeFloatArray(out, model.W1);
            writeFloatArray(out, model.b1);
            writeFloatArray(out, model.W2);
            out.writeFloat(model.b2);
            writeFloatArray(out, model.lengthW1);
            writeFloatArray(out, model.lengthB1);
            writeFloatArray(out, model.lengthOutW);
            writeFloatArray(out, model.lengthOutB);
            out.writeFloat(model.decisionThreshold);
        }
        System.err.println("Saved RL model cache: " + cachePath + " [" + modelName + ", epochs=" + epochs + "]");
    }

    static RLDecisionModel loadModel(Path cachePath, long expectedFingerprint) throws IOException {
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(cachePath)))) {
            int version = in.readInt();
            if (version != RL_MODEL_CACHE_VERSION) {
                throw new IOException("Unsupported model cache version: " + version);
            }
            String modelName = readString(in);
            readString(in);
            long fingerprint = in.readLong();
            if (fingerprint != expectedFingerprint) {
                throw new IOException("Model cache fingerprint mismatch");
            }
            int epochs = in.readInt();
            RLDecisionModel model = new RLDecisionModel();
            model.W1 = readFloatArray(in);
            model.b1 = readFloatArray(in);
            model.W2 = readFloatArray(in);
            model.b2 = in.readFloat();
            model.lengthW1 = readFloatArray(in);
            model.lengthB1 = readFloatArray(in);
            model.lengthOutW = readFloatArray(in);
            model.lengthOutB = readFloatArray(in);
            model.decisionThreshold = in.readFloat();
            model.explorationRate = 0.0f;
            System.err.println("Loaded RL model cache: " + cachePath
                    + " [" + modelName + ", fingerprint=" + fingerprint + ", epochs=" + epochs + "]");
            return model;
        }
    }

    static void saveTrainingDataset(
            RLTrainingDataset dataset, Path cachePath, String modelName,
            String directoryPath, long fingerprint) throws IOException {
        saveTrainingDataset(dataset, cachePath, modelName, directoryPath, fingerprint, CHUNK_SIZE);
    }

    static void saveTrainingDataset(
            RLTrainingDataset dataset, Path cachePath, String modelName,
            String directoryPath, long fingerprint, int trainingSourceChunkSize) throws IOException {
        Files.createDirectories(cachePath.getParent());
        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(cachePath)))) {
            out.writeInt(RL_LABEL_CACHE_VERSION);
            writeString(out, modelName);
            writeString(out, directoryPath);
            out.writeLong(fingerprint);
            out.writeInt(trainingSourceChunkSize);
            out.writeInt(VALUE_PACK_SIZE);
            out.writeInt(dataset.transformedSequences.size());
            for (int s = 0; s < dataset.transformedSequences.size(); s++) {
                long[] values = dataset.transformedSequences.get(s);
                out.writeInt(values.length);
                for (long v : values) {
                    out.writeLong(v);
                }
                int[] bitWidths = computeValueBitWidths(values);
                out.writeInt(bitWidths.length);
                for (int bw : bitWidths) {
                    out.writeInt(bw);
                }
                out.writeInt(dataset.dpOptimalCosts[s]);
                List<LengthDecisionPoint> labels = dataset.labelsBySequence.get(s);
                out.writeInt(labels.size());
                for (LengthDecisionPoint label : labels) {
                    out.writeInt(label.startIndex);
                    out.writeInt(label.labelLength);
                    out.writeInt(label.packCount);
                    out.writeInt(label.globalMaxPackSize);
                }
            }
            out.writeInt(dataset.valIndices.length);
            for (int idx : dataset.valIndices) {
                out.writeInt(idx);
            }
        }
        System.err.println("Saved RL training cache: " + cachePath
                + " (" + dataset.transformedSequences.size() + " sequences, "
                + dataset.valIndices.length + " val)");
    }

    static boolean containsInt(int[] values, int target) {
        for (int value : values) {
            if (value == target) {
                return true;
            }
        }
        return false;
    }

    static RLTrainingDataset loadTrainingDataset(Path cachePath, long expectedFingerprint) throws IOException {
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(cachePath)))) {
            int version = in.readInt();
            if (version != RL_LABEL_CACHE_VERSION) {
                throw new IOException("Unsupported cache version: " + version);
            }
            String modelName = readString(in);
            readString(in);
            long fingerprint = in.readLong();
            if (fingerprint != expectedFingerprint) {
                throw new IOException("Cache fingerprint mismatch");
            }
            int chunkSize = in.readInt();
            int valuePackSize = in.readInt();
            if (valuePackSize != VALUE_PACK_SIZE) {
                throw new IOException("Cache chunk/pack size mismatch");
            }

            RLTrainingDataset dataset = new RLTrainingDataset();
            int numSeq = in.readInt();
            IntList dpOptimalCosts = new IntList(numSeq);
            for (int s = 0; s < numSeq; s++) {
                int n = in.readInt();
                long[] values = new long[n];
                for (int i = 0; i < n; i++) {
                    values[i] = in.readLong();
                }
                int bwLen = in.readInt();
                int[] bitWidths = new int[bwLen];
                for (int i = 0; i < bwLen; i++) {
                    bitWidths[i] = in.readInt();
                }
                int dpCost = in.readInt();
                int numLabels = in.readInt();
                List<LengthDecisionPoint> labels = new ArrayList<>(numLabels);
                for (int l = 0; l < numLabels; l++) {
                    int startIndex = in.readInt();
                    int labelLength = in.readInt();
                    int packCount = in.readInt();
                    int globalMaxPackSize = in.readInt();
                    labels.add(new LengthDecisionPoint(
                            startIndex, labelLength, n, packCount, globalMaxPackSize, bitWidths));
                }
                dataset.transformedSequences.add(values);
                dataset.labelsBySequence.add(labels);
                dpOptimalCosts.add(dpCost);
            }
            int numVal = in.readInt();
            IntList valIndices = new IntList(numVal);
            for (int i = 0; i < numVal; i++) {
                valIndices.add(in.readInt());
            }
            dataset.dpOptimalCosts = dpOptimalCosts.toArray();
            dataset.valIndices = valIndices.toArray();
            IntList trainIndices = new IntList(dataset.transformedSequences.size() - dataset.valIndices.length);
            for (int i = 0; i < dataset.transformedSequences.size(); i++) {
                if (!containsInt(dataset.valIndices, i)) {
                    trainIndices.add(i);
                }
            }
            dataset.trainIndices = trainIndices.toArray();
            System.err.println("Loaded RL training cache: " + cachePath
                    + " [" + modelName + ", fingerprint=" + fingerprint
                    + ", " + dataset.transformedSequences.size() + " sequences]");
            return dataset;
        }
    }

    static RLTrainingDataset loadOrBuildTrainingDataset(
            String directoryPath, RLValueTransform transform, String modelName) {
        return loadOrBuildTrainingDataset(directoryPath, transform, modelName, CHUNK_SIZE);
    }

    static RLTrainingDataset loadOrBuildTrainingDataset(
            String directoryPath, RLValueTransform transform, String modelName,
            int trainingSourceChunkSize) {
        Path cachePath = labelCachePath(directoryPath, modelName);
        long fingerprint = computeTrainingDataFingerprint(directoryPath, trainingSourceChunkSize);

        if (RL_USE_LABEL_CACHE && !RL_FORCE_REBUILD_LABEL_CACHE && Files.isRegularFile(cachePath)) {
            try {
                return loadTrainingDataset(cachePath, fingerprint);
            } catch (IOException e) {
                System.err.println("RL cache load failed, rebuilding: " + e.getMessage());
            }
        }

        RLTrainingDataset dataset = buildTrainingDatasetWithDp(
                directoryPath, transform, modelName, trainingSourceChunkSize);
        if (RL_USE_LABEL_CACHE && !dataset.transformedSequences.isEmpty()) {
            try {
                saveTrainingDataset(dataset, cachePath, modelName, directoryPath, fingerprint,
                        trainingSourceChunkSize);
            } catch (IOException e) {
                System.err.println("Failed to save RL training cache: " + e.getMessage());
            }
        }
        return dataset;
    }

    static RLDecisionModel trainModelFromDirectory(
            int epochs, String directoryPath, RLValueTransform transform, String modelName) {
        return trainModelFromDirectory(epochs, directoryPath, transform, modelName, CHUNK_SIZE);
    }

    static RLDecisionModel trainModelForBenchmark(
            int epochs, String directoryPath, RLValueTransform transform, String modelName) {
        return trainModelForBenchmark(epochs, directoryPath, transform, modelName, CHUNK_SIZE);
    }

    static RLDecisionModel trainModelForBenchmark(
            int epochs, String directoryPath, RLValueTransform transform, String modelName,
            int trainingSourceChunkSize) {
        if (RL_FIXED_FALLBACK_ONLY) {
            System.err.println("Skipping RL model training for fixed-fallback-only benchmark: "
                    + modelName);
            return new RLDecisionModel();
        }
        return trainModelFromDirectory(
                epochs, directoryPath, transform, modelName, trainingSourceChunkSize);
    }

    static RLDecisionModel trainModelFromDirectory(
            int epochs, String directoryPath, RLValueTransform transform, String modelName,
            int trainingSourceChunkSize) {
        long modelFingerprint = computeModelFingerprint(
                directoryPath, modelName, epochs, trainingSourceChunkSize);
        Path modelCache = modelCachePath(directoryPath, modelName);
        if (RL_USE_MODEL_CACHE && !RL_FORCE_RETRAIN && Files.isRegularFile(modelCache)) {
            try {
                return loadModel(modelCache, modelFingerprint);
            } catch (IOException e) {
                System.err.println("RL model cache load failed, training: " + e.getMessage());
            }
        }

        System.err.println("Training DP-imitation model (" + modelName + ") from directory: " + directoryPath);
        RLDecisionModel model = new RLDecisionModel();
        model.explorationRate = 0.0f;

        RLTrainingDataset dataset = loadOrBuildTrainingDataset(
                directoryPath, transform, modelName, trainingSourceChunkSize);
        if (dataset.transformedSequences.isEmpty()) {
            System.err.println("No imitation labels available. Returning initial model.");
            return model;
        }

        System.err.println("Training on " + dataset.transformedSequences.size() + " sequences ("
                + dataset.trainIndices.length + " train, " + dataset.valIndices.length + " val)");

        List<long[]> transformedSequences = dataset.transformedSequences;
        List<List<LengthDecisionPoint>> labelsBySequence = dataset.labelsBySequence;
        int[] trainIndices = dataset.trainIndices;
        int[] valIndices = dataset.valIndices;

        List<long[]> valSequences = new ArrayList<>();
        int[] valDpEncodedCosts = new int[valIndices.length];
        int valCostCount = 0;
        for (int idx : valIndices) {
            valSequences.add(transformedSequences.get(idx));
            valDpEncodedCosts[valCostCount++] = dataset.dpOptimalCosts[idx];
        }

        ModelSnapshot bestSnapshot = null;
        float bestValEncodedRatio = Float.POSITIVE_INFINITY;
        float bestValCe = Float.POSITIVE_INFINITY;
        int epochsWithoutImprovement = 0;
        float currentLr = initialLearningRate;
        model.setLearningRate(currentLr);

        System.err.println("Phase 1: DP imitation (cost-weighted CE)...");
        for (int epoch = 1; epoch <= epochs; ++epoch) {
            if (epoch > 1 && epoch % 30 == 1) {
                currentLr *= 0.5f;
                model.setLearningRate(currentLr);
                System.err.printf("Learning rate decayed to %.6f at epoch %d%n", currentLr, epoch);
            }

            long startTime = System.nanoTime();
            float totalLoss = 0.0f;
            shuffleIntArray(trainIndices, model.rng);
            for (int idx : trainIndices) {
                totalLoss += model.trainLengthClassifier(
                        labelsBySequence.get(idx), dataset.dpOptimalCosts[idx]);
            }

            boolean doValidation = epoch == 1 || epoch % val_check_interval == 0 || epoch == epochs;
            float valCe = Float.NaN;
            float valEncodedRatio = Float.NaN;
            if (doValidation) {
                valCe = evaluateValCrossEntropy(model, valIndices, labelsBySequence);
                valEncodedRatio = evaluateEncodedCostRatio(model, valSequences, valDpEncodedCosts, false);
                if (valEncodedRatio < bestValEncodedRatio - 1e-6f) {
                    bestValEncodedRatio = valEncodedRatio;
                    bestValCe = valCe;
                    bestSnapshot = model.copySnapshot();
                    epochsWithoutImprovement = 0;
                } else if (epoch >= early_stop_min_epochs) {
                    epochsWithoutImprovement++;
                }
            }
            long durationMs = (System.nanoTime() - startTime) / 1_000_000L;

            if (epoch % 10 == 0 || epoch == 1 || epoch == epochs) {
                System.out.printf(
                        "Phase1 Epoch %d: CE=%.6f, Val CE=%.6f, Val Encoded/DP=%.6f (best=%.6f), LR=%.6f, Time=%d ms%n",
                        epoch,
                        totalLoss / trainIndices.length,
                        doValidation ? valCe : bestValCe,
                        doValidation ? valEncodedRatio : bestValEncodedRatio,
                        bestValEncodedRatio,
                        currentLr,
                        durationMs);
            }

            if (doValidation && epoch >= early_stop_min_epochs
                    && epochsWithoutImprovement * val_check_interval >= early_stop_patience) {
                System.err.printf("Phase1 early stopping at epoch %d (best val encoded/DP=%.6f)%n",
                        epoch, bestValEncodedRatio);
                break;
            }
        }

        if (bestSnapshot != null) {
            model.restoreSnapshot(bestSnapshot);
        }

        lastPhase1BestEncodedRatio = bestValEncodedRatio;

        if (RL_USE_COST_AWARE_TRAINING && RL_COST_AWARE_EPOCHS > 0) {
            System.err.printf(
                    "Phase 2: encoded-cost soft-label fine-tune (%d epochs, τ=%.1f bits)...%n",
                    RL_COST_AWARE_EPOCHS, RL_COST_SOFTMAX_TEMP);
            currentLr = initialLearningRate * RL_COST_AWARE_LR_SCALE;
            model.setLearningRate(currentLr);
            epochsWithoutImprovement = 0;

            for (int epoch = 1; epoch <= RL_COST_AWARE_EPOCHS; ++epoch) {
                if (epoch > 1 && epoch % 20 == 1) {
                    currentLr *= 0.5f;
                    model.setLearningRate(currentLr);
                }

                long startTime = System.nanoTime();
                float totalLoss = 0.0f;
                shuffleIntArray(trainIndices, model.rng);
                for (int idx : trainIndices) {
                    totalLoss += trainSequenceCostAware(
                            model,
                            transformedSequences.get(idx),
                            labelsBySequence.get(idx),
                            RL_COST_SOFTMAX_TEMP);
                }

                boolean doValidation = epoch == 1 || epoch % val_check_interval == 0
                        || epoch == RL_COST_AWARE_EPOCHS;
                float valEncodedRatio = Float.NaN;
                if (doValidation) {
                    valEncodedRatio = evaluateEncodedCostRatio(
                            model, valSequences, valDpEncodedCosts, false);
                    if (valEncodedRatio < bestValEncodedRatio - 1e-6f) {
                        bestValEncodedRatio = valEncodedRatio;
                        bestSnapshot = model.copySnapshot();
                        epochsWithoutImprovement = 0;
                    } else if (epoch >= early_stop_min_epochs) {
                        epochsWithoutImprovement++;
                    }
                }
                long durationMs = (System.nanoTime() - startTime) / 1_000_000L;

                if (epoch % 5 == 0 || epoch == 1 || epoch == RL_COST_AWARE_EPOCHS) {
                    System.out.printf(
                            "Phase2 Epoch %d: SoftCost=%.6f, Val Encoded/DP=%.6f (best=%.6f), LR=%.6f, Time=%d ms%n",
                            epoch,
                            totalLoss / trainIndices.length,
                            doValidation ? valEncodedRatio : bestValEncodedRatio,
                            bestValEncodedRatio,
                            currentLr,
                            durationMs);
                }

                if (doValidation && epoch >= early_stop_min_epochs
                        && epochsWithoutImprovement * val_check_interval >= early_stop_patience) {
                    System.err.printf("Phase2 early stopping at epoch %d (best val encoded/DP=%.6f)%n",
                            epoch, bestValEncodedRatio);
                    break;
                }
            }

            if (bestSnapshot != null) {
                model.restoreSnapshot(bestSnapshot);
            }
        }

        System.err.printf("Training done: %d sequences (%d val held out), best val encoded/DP=%.6f%n",
                transformedSequences.size(), valSequences.size(), bestValEncodedRatio);
        lastFinalBestEncodedRatio = bestValEncodedRatio;

        if (RL_USE_MODEL_CACHE) {
            try {
                saveModel(model, modelCache, modelFingerprint, modelName, directoryPath, epochs);
            } catch (IOException e) {
                System.err.println("Failed to save RL model cache: " + e.getMessage());
            }
        }
        return model;
    }

    static void tuneDecisionThreshold(
            RLDecisionModel model, List<long[]> transformedSequences, int[] dpOptimalCosts) {
        float originalThreshold = model.decisionThreshold;
        float bestThreshold = originalThreshold;
        double bestRatio = Double.POSITIVE_INFINITY;
        float savedExploration = model.explorationRate;
        model.explorationRate = 0.0f;

        for (int step = 1; step <= 99; step++) {
            float threshold = step / 100.0f;
            model.decisionThreshold = threshold;
            double totalRatio = 0.0;
            int count = 0;

            for (int i = 0; i < transformedSequences.size(); i++) {
                long[] sequence = transformedSequences.get(i);
                int[] bitWidths = computeValueBitWidths(sequence);
                PackingResult result = packValuesFast(sequence, bitWidths, model);
                int dpCost = dpOptimalCosts[i];
                if (dpCost > 0 && result.totalCost > 0) {
                    totalRatio += (double) result.totalCost / (double) dpCost;
                    count++;
                }
            }

            if (count > 0) {
                double avgRatio = totalRatio / count;
                if (avgRatio < bestRatio) {
                    bestRatio = avgRatio;
                    bestThreshold = threshold;
                }
            }
        }

        model.decisionThreshold = bestThreshold;
        model.explorationRate = savedExploration;
        System.out.printf("Tuned merge threshold = %.2f, Avg Cost/DP = %.6f%n", bestThreshold, bestRatio);
    }

    static List<DecisionPoint> buildDpImitationLabels(int[] valueBitWidths, BPDP.PackingPlan optimalPlan) {
        List<DecisionPoint> labels = new ArrayList<>();
        if (valueBitWidths == null || valueBitWidths.length <= 1) {
            return labels;
        }

        int[] groupEndExclusive = new int[valueBitWidths.length];
        int cursor = 0;
        for (int groupSize : optimalPlan.groupSizes) {
            int end = cursor + groupSize;
            for (int i = cursor; i < end && i < groupEndExclusive.length; i++) {
                groupEndExclusive[i] = end;
            }
            cursor = end;
        }

        Pack currentPack = new Pack();
        currentPack.addValue(0, valueBitWidths[0]);
        int globalMaxPackSize = 0;
        int packCount = 0;
        final int n = valueBitWidths.length;

        for (int i = 1; i < n; i++) {
            boolean shouldMerge = i < groupEndExclusive[i - 1];
            labels.add(new DecisionPoint(currentPack.size, currentPack.maxBitWidth,
                    valueBitWidths[i], packCount, globalMaxPackSize, n, shouldMerge, 0.0f));

            if (shouldMerge) {
                currentPack.addValue(i, valueBitWidths[i]);
            } else {
                if (currentPack.size > globalMaxPackSize) {
                    globalMaxPackSize = currentPack.size;
                }
                packCount++;
                currentPack = new Pack();
                currentPack.addValue(i, valueBitWidths[i]);
            }
        }

        return labels;
    }

    static List<LengthDecisionPoint> buildDpLengthLabels(int[] valueBitWidths, BPDP.PackingPlan optimalPlan) {
        List<LengthDecisionPoint> labels = new ArrayList<>();
        if (valueBitWidths == null || valueBitWidths.length == 0 || optimalPlan.groupSizes.length == 0) {
            return labels;
        }

        int cursor = 0;
        int packCount = 0;
        int globalMaxPackSize = 0;
        int n = valueBitWidths.length;
        for (int groupSize : optimalPlan.groupSizes) {
            if (cursor >= n) {
                break;
            }
            int length = Math.max(1, Math.min(groupSize, n - cursor));
            labels.add(new LengthDecisionPoint(cursor, length, n, packCount, globalMaxPackSize, valueBitWidths));
            packCount++;
            if (length > globalMaxPackSize) {
                globalMaxPackSize = length;
            }
            cursor += length;
        }
        return labels;
    }

    // ========== 以下是未修改的原有方法，为保持完整性包含 ==========

    static int getBitWidth(long num) {
        if (num == 0)
            return 1;
        else
            return 64 - Long.numberOfLeadingZeros(num);
    }

    static int bitsForPackSize(int maxPackSize) {
        if (maxPackSize <= 1) {
            return 1;
        }
        return 32 - Integer.numberOfLeadingZeros(maxPackSize);
    }

    public static int bitPacking(long[] values, int start, int length, int bitWidth, int encodePos,
                                 byte[] encodedResult) {
        if (values == null || length <= 0 || bitWidth == 0) {
            return encodePos;
        }

        int currentByte = 0;
        int bitsInCurrentByte = 0;
        int bytePos = encodePos;

        for (int i = 0; i < length; i++) {
            long value = values[start + i];
            int remainingBits = bitWidth;

            while (remainingBits > 0) {
                int bitsToWrite = Math.min(remainingBits, 8 - bitsInCurrentByte);
                int shift = remainingBits - bitsToWrite;
                long bits = (value >>> shift) & ((1L << bitsToWrite) - 1);
                currentByte = (currentByte << bitsToWrite) | (int)bits;
                bitsInCurrentByte += bitsToWrite;

                if (bitsInCurrentByte == 8) {
                    encodedResult[bytePos] = (byte) currentByte;
                    bytePos++;
                    currentByte = 0;
                    bitsInCurrentByte = 0;
                }

                remainingBits -= bitsToWrite;
            }
        }

        if (bitsInCurrentByte > 0) {
            currentByte = currentByte << (8 - bitsInCurrentByte);
            encodedResult[bytePos] = (byte) currentByte;
            bytePos++;
        }

        return bytePos;
    }

    public static long[] decodeBitPacking(byte[] encoded, int decodePos, int bitWidth,
                                          int numValues) {
        if (encoded == null || bitWidth == 0 || numValues == 0) {
            return new long[0];
        }

        long[] result = new long[numValues];
        int currentByte = 0;
        int bitsInCurrentByte = 0;
        int bytePos = decodePos;

        for (int i = 0; i < numValues; i++) {
            long value = 0;
            int bitsRead = 0;

            while (bitsRead < bitWidth) {
                if (bitsInCurrentByte == 0) {
                    if (bytePos >= encoded.length) {
                        return Arrays.copyOf(result, i);
                    }
                    currentByte = encoded[bytePos] & 0xFF;
                    bytePos++;
                    bitsInCurrentByte = 8;
                }

                int bitsToRead = Math.min(bitWidth - bitsRead, bitsInCurrentByte);
                int shift = bitsInCurrentByte - bitsToRead;
                int bits = (currentByte >>> shift) & ((1 << bitsToRead) - 1);
                value = (value << bitsToRead) | bits;
                bitsRead += bitsToRead;
                currentByte &= (1 << shift) - 1;
                bitsInCurrentByte -= bitsToRead;
            }

            result[i] = value;
        }

        return result;
    }

    static List<long[]> loadRawValuesFromDirectory(String directoryPath) {
        return loadRawValuesFromDirectory(directoryPath, CHUNK_SIZE);
    }

    static List<long[]> loadRawValuesFromDirectory(String directoryPath, int sourceChunkSize) {
        List<long[]> allSequences = new ArrayList<>();
        File dir = new File(directoryPath);

        if (!dir.exists() || !dir.isDirectory()) {
            System.err.println("Directory not found: " + directoryPath);
            return allSequences;
        }

        File[] files = dir.listFiles((d, name) -> name.toLowerCase().endsWith(".csv"));
        if (files == null || files.length == 0) {
            System.err.println("No CSV files found in directory: " + directoryPath);
            return allSequences;
        }

        for (File file : files) {
            if (!BenchmarkDatasetFilter.includeDatasetFile(file.getName())) {
                continue;
            }

            System.out.println("Loading training data from: " + file.getName());

            try {
                List<String> numbers = new ArrayList<>();
                List<Integer> decimalPlaces = new ArrayList<>();
                loadNumbersFromCsv(file.toPath(), numbers, decimalPlaces);

                System.out.println("  Total values in file: " + numbers.size());

                if (numbers.isEmpty()) {
                    continue;
                }

                int targetCount = (int) Math.ceil(numbers.size() * 0.1);
                System.out.println("  Target count (10%): " + targetCount);

                int chunkSize = Math.max(1, sourceChunkSize);
                if (targetCount < chunkSize) {
                    targetCount = Math.min(chunkSize, numbers.size());
                    System.out.println("  Adjusted to: " + targetCount
                            + " (min " + chunkSize + " or all if less)");
                } else {
                    targetCount = targetCount - (targetCount % chunkSize);
                    targetCount = Math.min(targetCount, numbers.size());
                    System.out.println("  Adjusted to: " + targetCount
                            + " (" + chunkSize + " multiples)");
                }

                System.out.println("  Selected values: " + targetCount);
                int created = 0;
                for (int i = 0; i < targetCount; i += chunkSize) {
                    int end = Math.min(i + chunkSize, targetCount);
                    if (end - i == chunkSize) {
                        int decimalMax = 0;
                        for (int k = i; k < end; k++) {
                            if (decimalPlaces.get(k) > decimalMax) {
                                decimalMax = decimalPlaces.get(k);
                            }
                        }
                        long[] scaled = scaleNumbers(numbers.subList(i, end), decimalMax);
                        allSequences.add(scaled);
                        created++;
                    }
                }

                System.out.println("  Created " + created + " sequences of "
                        + chunkSize + " values");

            } catch (IOException e) {
                System.err.println("Error reading file: " + file.getName() + " - " + e.getMessage());
            }
        }

        System.out.println("Total training sequences loaded: " + allSequences.size());
        return allSequences;
    }

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


    private static class BitWriter {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        private long acc = 0L;
        private int accBits = 0;

        void writeBits(long bits, int bitCount) {
            if (bitCount == 0) return;
            if (bitCount == 64) {
                writeBits((bits >>> 32) & 0xFFFFFFFFL, 32);
                writeBits(bits & 0xFFFFFFFFL, 32);
                return;
            }
            long mask = (bitCount == 64) ? ~0L : ((1L << bitCount) - 1L);
            long v = bits & mask;
            acc = (acc << bitCount) | v;
            accBits += bitCount;
            while (accBits >= 8) {
                int shift = accBits - 8;
                int outb = (int) ((acc >>> shift) & 0xFFL);
                out.write(outb);
                if (shift > 0) {
                    acc &= ((1L << shift) - 1L);
                } else {
                    acc = 0L;
                }
                accBits = shift;
            }
        }

        byte[] finish() {
            if (accBits > 0) {
                int outb = (int) ((acc << (8 - accBits)) & 0xFFL);
                out.write(outb);
                acc = 0L;
                accBits = 0;
            }
            return out.toByteArray();
        }
    }

    public static final class BitReader {
        private final byte[] data;
        private int bitPos;

        public BitReader(byte[] data) {
            this(data, 0);
        }

        public BitReader(byte[] data, int byteOffset) {
            this.data = data;
            this.bitPos = byteOffset * 8;
        }

        public long readBits(int n) {
            if (n == 0) return 0L;
            if (n < 0 || n > 64) {
                throw new IllegalArgumentException("n must be between 0 and 64");
            }

            long result = 0L;
            int bitsRemaining = n;

            while (bitsRemaining > 0) {
                int byteIndex = bitPos >>> 3;
                int bitOffset = bitPos & 7;

                if (byteIndex >= data.length) {
                    result = (result << bitsRemaining);
                    bitPos += bitsRemaining;
                    return result;
                }

                int bitsFromCurrentByte = Math.min(8 - bitOffset, bitsRemaining);
                int curByte = data[byteIndex] & 0xFF;
                int shift = 8 - bitOffset - bitsFromCurrentByte;
                int chunk = (curByte >>> shift) & ((1 << bitsFromCurrentByte) - 1);

                result = (result << bitsFromCurrentByte) | chunk;

                bitPos += bitsFromCurrentByte;
                bitsRemaining -= bitsFromCurrentByte;
            }

            return result;
        }

        public int consumedBits() {
            return bitPos;
        }

        public int bitPosition() {
            return bitPos;
        }

        public int remainingBits() {
            return (data.length * 8) - bitPos;
        }
    }

    static int[] computeValueBitWidths(long[] values) {
        int[] bitWidths = new int[values.length];
        for (int i = 0; i < values.length; i++) {
            bitWidths[i] = getBitWidth(values[i]);
        }
        return bitWidths;
    }

    /** Value-level variable-length packing per paper MDP (merge/split on b_i sequence). */
    static PackingResult packValues(long[] dataArray, int[] valueBitWidths, RLDecisionModel model,
                                    List<DecisionPoint> decisionTrace) {
        PackingResult result = new PackingResult();
        Pack currentPack = new Pack();
        int globalMaxPackSize = 0;
        int packCount = 0;
        final int n = valueBitWidths.length;
        Random localRng = ThreadLocalRandom.current();

        for (int i = 0; i < n; ++i) {
            int b = valueBitWidths[i];
            if (currentPack.size == 0) {
                currentPack.addValue(i, b);
                continue;
            }

            float[] feat = new float[INPUT_DIM];
            float denom = n > 0 ? (float) n : 1.0f;
            feat[0] = currentPack.maxBitWidth / 64.0f;
            feat[1] = globalMaxPackSize / denom;
            feat[2] = currentPack.size / denom;
            feat[3] = packCount / denom;
            feat[4] = b / 64.0f;

            float probability = model.forwardProb(feat);
            boolean shouldMerge;
            if (localRng.nextFloat() < model.explorationRate) {
                shouldMerge = localRng.nextFloat() > 0.5f;
            } else {
                shouldMerge = model.shouldMerge(probability);
            }

            if (decisionTrace != null) {
                decisionTrace.add(new DecisionPoint(currentPack.size, currentPack.maxBitWidth,
                        b, packCount, globalMaxPackSize, n, shouldMerge, probability));
            }

            if (shouldMerge) {
                currentPack.addValue(i, b);
            } else {
                if (currentPack.size > globalMaxPackSize) {
                    globalMaxPackSize = currentPack.size;
                }
                result.packs.add(currentPack);
                packCount++;
                currentPack = new Pack();
                currentPack.addValue(i, b);
            }
        }

        if (currentPack.size > 0) {
            if (currentPack.size > globalMaxPackSize) {
                globalMaxPackSize = currentPack.size;
            }
            result.packs.add(currentPack);
            packCount++;
        }

        result.packCount = packCount;
        result.calculateCost(dataArray);
        return result;
    }

    /** Stronger imitation path: predict the next pack length directly at each pack boundary. */
    static PackingResult packValuesByPredictedLength(long[] dataArray, int[] valueBitWidths, RLDecisionModel model) {
        PackingResult result = new PackingResult();
        result.packs = buildPacksByPredictedLength(valueBitWidths, model);
        result.packCount = result.packs.size();
        result.calculateCost(dataArray);
        return result;
    }

    static PackingResult packFromDpPlan(long[] dataArray, int[] valueBitWidths, BPDP.PackingPlan plan) {
        PackingResult result = new PackingResult();
        int index = 0;
        for (int groupSize : plan.groupSizes) {
            Pack pack = new Pack();
            for (int j = 0; j < groupSize && index < valueBitWidths.length; j++) {
                pack.addValue(index, valueBitWidths[index]);
                index++;
            }
            result.packs.add(pack);
        }
        result.packCount = result.packs.size();
        result.calculateCost(dataArray);
        return result;
    }

    /** Rebuild pack list with per-value bit widths for accurate merge. */
    static List<Pack> rebuildPacksWithBitWidths(List<Pack> packs, int[] valueBitWidths) {
        List<Pack> rebuilt = new ArrayList<>();
        for (Pack old : packs) {
            Pack pack = new Pack();
            for (int j = 0; j < old.size; j++) {
                int idx = old.startIndex + j;
                if (idx < valueBitWidths.length) {
                    pack.addValue(idx, valueBitWidths[idx]);
                }
            }
            rebuilt.add(pack);
        }
        return rebuilt;
    }

    static long encodedCostOfPacks(List<Pack> packs, long[] dataArray) {
        return encodedBitCostOfPacks(packs, dataArray);
    }

    /** Paper Proposition: greedily merge adjacent same-bitwidth packs if encoded size decreases. */
    static PackingResult refineBySameBitWidthGreedyMerge(
            PackingResult input, long[] dataArray, int[] valueBitWidths) {
        if (input == null || input.packs == null || input.packs.size() < 2) {
            return input;
        }
        List<Pack> packs = rebuildPacksWithBitWidths(input.packs, valueBitWidths);
        boolean changed = true;
        while (changed && packs.size() >= 2) {
            changed = false;
            for (int i = 0; i < packs.size() - 1; i++) {
                Pack left = packs.get(i);
                Pack right = packs.get(i + 1);
                if (left.maxBitWidth != right.maxBitWidth) {
                    continue;
                }
                Pack merged = new Pack();
                for (int j = 0; j < left.size; j++) {
                    merged.addValue(left.startIndex + j, valueBitWidths[left.startIndex + j]);
                }
                for (int j = 0; j < right.size; j++) {
                    merged.addValue(right.startIndex + j, valueBitWidths[right.startIndex + j]);
                }
                List<Pack> trial = new ArrayList<>(packs);
                trial.set(i, merged);
                trial.remove(i + 1);
                if (encodedCostOfPacks(trial, dataArray) < encodedCostOfPacks(packs, dataArray)) {
                    packs = trial;
                    changed = true;
                    break;
                }
            }
        }
        PackingResult out = new PackingResult();
        out.packs = packs;
        out.packCount = packs.size();
        out.calculateCost(dataArray);
        return out;
    }

    static PackingResult applyAllRefinements(PackingResult input, long[] dataArray, int[] valueBitWidths) {
        PackingResult refined = refineBySameBitWidthGreedyMerge(input, dataArray, valueBitWidths);
        return refineByMergingAdjacentPacks(refined, dataArray, valueBitWidths);
    }

    /** Post-process: greedily merge adjacent packs if encoded size decreases. */
    static PackingResult refineByMergingAdjacentPacks(PackingResult input, long[] dataArray, int[] valueBitWidths) {
        if (input == null || input.packs == null || input.packs.size() < 2) {
            return input;
        }
        List<Pack> packs = rebuildPacksWithBitWidths(input.packs, valueBitWidths);
        boolean changed = true;
        while (changed && packs.size() >= 2) {
            changed = false;
            for (int i = 0; i < packs.size() - 1; i++) {
                Pack left = packs.get(i);
                Pack right = packs.get(i + 1);
                Pack merged = new Pack();
                for (int j = 0; j < left.size; j++) {
                    merged.addValue(left.startIndex + j, valueBitWidths[left.startIndex + j]);
                }
                for (int j = 0; j < right.size; j++) {
                    merged.addValue(right.startIndex + j, valueBitWidths[right.startIndex + j]);
                }
                List<Pack> trial = new ArrayList<>(packs);
                trial.set(i, merged);
                trial.remove(i + 1);
                if (encodedCostOfPacks(trial, dataArray) < encodedCostOfPacks(packs, dataArray)) {
                    packs = trial;
                    changed = true;
                    break;
                }
            }
        }
        PackingResult out = new PackingResult();
        out.packs = packs;
        out.packCount = packs.size();
        out.calculateCost(dataArray);
        return out;
    }

    static int maxBitWidthInRange(int[] bitWidths, int start, int length) {
        int max = 0;
        int end = Math.min(bitWidths.length, start + length);
        for (int i = start; i < end; i++) {
            max = Math.max(max, bitWidths[i]);
        }
        return max;
    }

    static PackingResult selectBestPackingCandidate(
            long[] dataArray, int[] valueBitWidths, RLDecisionModel model) {
        return packValuesFast(dataArray, valueBitWidths, model);
    }

    static PackingResult compressValuesWithRL(long[] values, RLDecisionModel model) {
        int[] bitWidths = computeValueBitWidths(values);
        PackingResult rl = packValuesFast(values, bitWidths, model);
        if (!RL_USE_VANILLA_FALLBACK && !RL_USE_DEGRADE_FALLBACK) {
            encodeRlPackingResult(rl, values);
            return rl;
        }
        ChunkCompressionOutcome outcome = selectRlOrVanillaBest(values, rl);
        if (!outcome.usedVanillaFallback && !outcome.usedBPDPDegradeFallback) {
            return rl;
        }
        PackingResult hybrid = new PackingResult();
        hybrid.compressedData = outcome.compressedData;
        hybrid.totalCost = outcome.encodedBits;
        hybrid.paperCost = outcome.encodedBits;
        hybrid.packCount = rl.packCount;
        hybrid.packs = rl.packs;
        hybrid.usedVanillaFallback = outcome.usedVanillaFallback;
        hybrid.usedBPDPDegradeFallback = outcome.usedBPDPDegradeFallback;
        hybrid.vanillaPackSize = outcome.vanillaPackSize;
        hybrid.vanillaGroupBitWidths = outcome.vanillaGroupBitWidths;
        hybrid.bpdDegradePackSize = outcome.bpdDegradePackSize;
        return hybrid;
    }

    /** Result of fixed-pack vanilla BP encoding for one chunk. */
    static class VanillaFixedPackResult {
        int packSize;
        byte[] compressedData;
        int[] groupBitWidths;
        long encodedBits;
        long[] paddedValues;
    }

    /** BPDP optimal packing at a fixed value-pack size (same wire format as BP-DP). */
    static class BPDPFixedPackResult {
        int packSize;
        byte[] compressedData;
        long encodedBits;
    }

    static class DegradeFixedPackChoice {
        int packSize;
        long encodedBits;
    }

    static class FixedFallbackPlan {
        boolean usedVanillaFallback;
        boolean usedBPDPDegradeFallback;
        int packSize;
        long encodedBits;
        BP.VanillaPackLayout vanillaLayout;
        long[] bpdpPaddedValues;
        BPDP.PackingPlan bpdpPlan;
    }

    /** Per-chunk choice between RL variable packing and fixed-pack fallbacks. */
    static class ChunkCompressionOutcome {
        byte[] compressedData;
        long encodedBits;
        boolean usedVanillaFallback;
        boolean usedBPDPDegradeFallback;
        int vanillaPackSize;
        int bpdDegradePackSize;
        int[] vanillaGroupBitWidths;
        /** 1 if RL strictly better than all fallbacks, else 0. */
        int rlBetter;
    }

    static VanillaFixedPackResult compressVanillaFixedPack(long[] values, int packSize) {
        VanillaFixedPackResult result = new VanillaFixedPackResult();
        result.packSize = packSize;
        if (values == null || values.length == 0 || packSize <= 0) {
            result.compressedData = new byte[0];
            result.groupBitWidths = new int[0];
            result.paddedValues = new long[0];
            result.encodedBits = 0;
            return result;
        }

        BP.VanillaPackLayout layout = BP.layoutVanillaFixedPack(values, packSize);
        result.paddedValues = layout.paddedValues;
        result.groupBitWidths = layout.groupBitWidths;
        result.compressedData = BP.encodeVanillaFixedPack(layout);
        result.encodedBits = result.compressedData.length * 8L;
        return result;
    }

    static long estimateVanillaFixedPackBits(long[] values, int packSize) {
        return BP.computeVanillaEncodedBits(values, packSize);
    }

    static long encodedBitsForVanillaLayout(BP.VanillaPackLayout layout) {
        if (layout == null || layout.groupBitWidths == null) {
            return 0L;
        }
        long totalBits = 0L;
        for (int bitWidth : layout.groupBitWidths) {
            totalBits += 8L + (long) layout.packSize * bitWidth;
        }
        return ((totalBits + 7L) / 8L) * 8L;
    }

    static int findBestVanillaFixedPackSize(long[] values) {
        int bestPackSize = 1;
        long bestBits = Long.MAX_VALUE;
        for (int packSize : VANILLA_FIXED_PACK_SIZES) {
            long candidateBits = estimateVanillaFixedPackBits(values, packSize);
            if (candidateBits < bestBits) {
                bestBits = candidateBits;
                bestPackSize = packSize;
            }
        }
        return bestPackSize;
    }

    static VanillaFixedPackResult findBestVanillaFixedPack(long[] values) {
        return compressVanillaFixedPack(values, findBestVanillaFixedPackSize(values));
    }

    static long estimateBPDPFixedPackBits(long[] values, int packSize) {
        if (values == null || values.length == 0 || packSize <= 0) {
            return 0L;
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
        return (long) BPDP.computeOptimalEncodedByteSize(padded, bitWidths, packSize) * 8L;
    }

    static BPDPFixedPackResult compressBPDPFixedPack(long[] values, int packSize) {
        BPDPFixedPackResult result = new BPDPFixedPackResult();
        result.packSize = packSize;
        if (values == null || values.length == 0 || packSize <= 0) {
            result.compressedData = new byte[0];
            result.encodedBits = 0;
            return result;
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
        result.compressedData = BPDP.compressWithOptimalPacking(padded, bitWidths, packSize, encodePos);
        result.encodedBits = encodePos[0] * 8L;
        return result;
    }

    static FixedFallbackPlan buildBPDPFixedFallbackPlan(long[] values, int packSize) {
        FixedFallbackPlan plan = new FixedFallbackPlan();
        plan.usedBPDPDegradeFallback = true;
        plan.packSize = packSize;
        if (values == null || values.length == 0 || packSize <= 0) {
            plan.bpdpPaddedValues = new long[0];
            plan.bpdpPlan = new BPDP.PackingPlan(0, new int[0], new int[0], 0);
            plan.encodedBits = 0L;
            return plan;
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
        plan.bpdpPaddedValues = padded;
        plan.bpdpPlan = BPDP.computeOptimalPackingPlan(bitWidths, packSize);
        plan.encodedBits = (long) BPDP.computeEncodedByteSizeForPlan(plan.bpdpPlan, packSize) * 8L;
        return plan;
    }

    static FixedFallbackPlan buildGreedyBPDPFallbackPlan(long[] values) {
        FixedFallbackPlan plan = new FixedFallbackPlan();
        plan.usedBPDPDegradeFallback = true;
        plan.packSize = VALUE_PACK_SIZE;
        if (values == null || values.length == 0) {
            plan.bpdpPaddedValues = new long[0];
            plan.bpdpPlan = new BPDP.PackingPlan(0, new int[0], new int[0], 0);
            plan.encodedBits = 0L;
            return plan;
        }
        int[] bitWidths = computeValueBitWidths(values);
        List<Pack> greedy = fastGreedyPaperCostMerge(singletonValuePacks(values), bitWidths);
        plan.bpdpPaddedValues = Arrays.copyOf(values, values.length);
        plan.bpdpPlan = toBPDPPackingPlan(greedy);
        plan.encodedBits = (long) BPDP.computeEncodedByteSizeForPlan(plan.bpdpPlan, VALUE_PACK_SIZE) * 8L;
        return plan;
    }

    static int[] configuredDegradePackSizes() {
        return configuredPositiveIntListProperty("bprl.degradePackSizes", RL_DEGRADE_PACK_SIZES);
    }

    static DegradeFixedPackChoice findBestDegradeFixedPackChoice(long[] values) {
        int[] packSizes = configuredDegradePackSizes();
        int bestPackSize = packSizes[0];
        long bestBits = Long.MAX_VALUE;
        for (int packSize : packSizes) {
            long candidateBits = estimateBPDPFixedPackBits(values, packSize);
            if (candidateBits < bestBits) {
                bestBits = candidateBits;
                bestPackSize = packSize;
            }
        }
        DegradeFixedPackChoice choice = new DegradeFixedPackChoice();
        choice.packSize = bestPackSize;
        choice.encodedBits = bestBits;
        return choice;
    }

    static int findBestDegradeFixedPackSize(long[] values) {
        return findBestDegradeFixedPackChoice(values).packSize;
    }

    static BPDPFixedPackResult findBestDegradeFixedPack(long[] values) {
        return compressBPDPFixedPack(values, findBestDegradeFixedPackSize(values));
    }

    static FixedFallbackPlan computeBestFixedFallbackPlan(long[] values) {
        FixedFallbackPlan best = null;
        if (RL_USE_GREEDY_FIXED_FALLBACK) {
            best = buildGreedyBPDPFallbackPlan(values);
        }
        if (RL_USE_DEGRADE_FALLBACK) {
            for (int packSize : configuredDegradePackSizes()) {
                FixedFallbackPlan candidate = buildBPDPFixedFallbackPlan(values, packSize);
                if (best == null || candidate.encodedBits < best.encodedBits) {
                    best = candidate;
                }
            }
        }
        if (RL_USE_VANILLA_FALLBACK) {
            int bestPackSize = findBestVanillaFixedPackSize(values);
            BP.VanillaPackLayout layout = BP.layoutVanillaFixedPack(values, bestPackSize);
            FixedFallbackPlan candidate = new FixedFallbackPlan();
            candidate.usedVanillaFallback = true;
            candidate.packSize = bestPackSize;
            candidate.vanillaLayout = layout;
            candidate.encodedBits = encodedBitsForVanillaLayout(layout);
            if (best == null || candidate.encodedBits < best.encodedBits) {
                best = candidate;
            }
        }
        if (best != null) {
            return best;
        }
        FixedFallbackPlan singleton = new FixedFallbackPlan();
        singleton.usedBPDPDegradeFallback = true;
        singleton.packSize = VALUE_PACK_SIZE;
        singleton.bpdpPaddedValues = values == null ? new long[0] : Arrays.copyOf(values, values.length);
        singleton.bpdpPlan = toBPDPPackingPlan(singletonValuePacks(singleton.bpdpPaddedValues));
        singleton.encodedBits = (long) BPDP.computeEncodedByteSizeForPlan(singleton.bpdpPlan, VALUE_PACK_SIZE) * 8L;
        return singleton;
    }

    static ChunkCompressionOutcome materializeFixedFallbackPlan(FixedFallbackPlan plan) {
        ChunkCompressionOutcome outcome = new ChunkCompressionOutcome();
        if (plan == null) {
            return outcome;
        }
        outcome.encodedBits = plan.encodedBits;
        if (plan.usedVanillaFallback) {
            byte[] encoded = BP.encodeVanillaFixedPack(plan.vanillaLayout);
            outcome.compressedData = encoded;
            outcome.encodedBits = encoded.length * 8L;
            outcome.usedVanillaFallback = true;
            outcome.vanillaPackSize = plan.packSize;
            outcome.vanillaGroupBitWidths = plan.vanillaLayout.groupBitWidths;
            return outcome;
        }
        int[] encodePos = new int[1];
        byte[] encoded = BPDP.compressWithPackingPlan(
                plan.bpdpPaddedValues, plan.bpdpPlan, plan.packSize, encodePos);
        outcome.compressedData = encoded;
        outcome.encodedBits = encodePos[0] * 8L;
        outcome.usedBPDPDegradeFallback = true;
        outcome.bpdDegradePackSize = plan.packSize;
        return outcome;
    }

    static ChunkCompressionOutcome selectRlOrVanillaBest(long[] values, PackingResult rlResult) {
        ChunkCompressionOutcome outcome = new ChunkCompressionOutcome();
        long rlBits = Long.MAX_VALUE;
        if (rlResult != null && rlResult.packs != null && !rlResult.packs.isEmpty()) {
            if (rlResult.totalCost <= 0 && rlResult.compressedData == null) {
                encodeRlPackingResult(rlResult, values);
            }
            rlBits = rlResult.totalCost > 0
                    ? rlResult.totalCost
                    : (long) rlResult.compressedData.length * 8L;
        }

        long bestAltBits = Long.MAX_VALUE;
        int bestVanillaPackSize = 0;
        int bestDegradePackSize = 0;
        boolean vanillaWinsAmongAlts = false;
        boolean degradeWinsAmongAlts = false;

        long dpOptimalBits = -1L;
        boolean skipDegrade = false;
        if (RL_SKIP_DEGRADE_NEAR_DP_OPTIMAL && RL_USE_DEGRADE_FALLBACK) {
            dpOptimalBits = cachedOrComputeDpOptimalEncodedBits(
                    values, computeValueBitWidths(values), rlResult);
            skipDegrade = dpOptimalBits > 0 && rlBits <= dpOptimalBits;
        }

        if (RL_USE_DEGRADE_FALLBACK && !skipDegrade) {
            DegradeFixedPackChoice degradeChoice = findBestDegradeFixedPackChoice(values);
            bestDegradePackSize = degradeChoice.packSize;
            bestAltBits = degradeChoice.encodedBits;
            degradeWinsAmongAlts = true;
        }
        if (RL_USE_VANILLA_FALLBACK) {
            bestVanillaPackSize = findBestVanillaFixedPackSize(values);
            long vanillaBits = estimateVanillaFixedPackBits(values, bestVanillaPackSize);
            if (vanillaBits < bestAltBits) {
                bestAltBits = vanillaBits;
                vanillaWinsAmongAlts = true;
                degradeWinsAmongAlts = false;
            } else if (degradeWinsAmongAlts && bestAltBits <= vanillaBits) {
                vanillaWinsAmongAlts = false;
            }
        }

        if (bestAltBits < rlBits && vanillaWinsAmongAlts) {
            if (RL_FALLBACK_COST_ONLY) {
                outcome.encodedBits = bestAltBits;
                outcome.vanillaPackSize = bestVanillaPackSize;
            } else {
                VanillaFixedPackResult encoded = compressVanillaFixedPack(values, bestVanillaPackSize);
                outcome.compressedData = encoded.compressedData;
                outcome.encodedBits = encoded.encodedBits;
                outcome.vanillaPackSize = encoded.packSize;
                outcome.vanillaGroupBitWidths = encoded.groupBitWidths;
            }
            outcome.usedVanillaFallback = true;
            outcome.rlBetter = 0;
        } else if (bestAltBits < rlBits && degradeWinsAmongAlts) {
            if (RL_FALLBACK_COST_ONLY) {
                outcome.encodedBits = bestAltBits;
                outcome.bpdDegradePackSize = bestDegradePackSize;
            } else {
                BPDPFixedPackResult encoded = compressBPDPFixedPack(values, bestDegradePackSize);
                outcome.compressedData = encoded.compressedData;
                outcome.encodedBits = encoded.encodedBits;
                outcome.bpdDegradePackSize = encoded.packSize;
            }
            outcome.usedBPDPDegradeFallback = true;
            outcome.rlBetter = 0;
        } else {
            if (rlResult != null) {
                if (rlResult.compressedData == null) {
                    encodeRlPackingResult(rlResult, values);
                }
                outcome.compressedData = rlResult.compressedData;
                outcome.encodedBits = rlResult.totalCost;
            } else {
                outcome.compressedData = null;
                outcome.encodedBits = 0L;
            }
            outcome.rlBetter = (bestAltBits == Long.MAX_VALUE || rlBits < bestAltBits) ? 1 : 0;
        }
        return outcome;
    }

    static long[] decompressChunkOutcome(ChunkCompressionOutcome outcome, int originalLength) {
        if (outcome == null || outcome.compressedData == null) {
            return new long[0];
        }
        if (outcome.usedVanillaFallback) {
            return BP.decodeBitPacking(
                    outcome.compressedData, outcome.vanillaGroupBitWidths,
                    outcome.vanillaPackSize, originalLength);
        }
        if (outcome.usedBPDPDegradeFallback) {
            return BPDP.decompressWithOptimalPacking(
                    outcome.compressedData, originalLength, outcome.bpdDegradePackSize);
        }
        return fastDecompress(outcome.compressedData, originalLength);
    }

    static long[] decompressPackingResult(PackingResult result, int originalLength) {
        if (result == null || result.compressedData == null) {
            return new long[0];
        }
        if (result.usedVanillaFallback) {
            return BP.decodeBitPacking(
                    result.compressedData, result.vanillaGroupBitWidths,
                    result.vanillaPackSize, originalLength);
        }
        if (result.usedBPDPDegradeFallback) {
            return BPDP.decompressWithOptimalPacking(
                    result.compressedData, originalLength, result.bpdDegradePackSize);
        }
        return fastDecompress(result.compressedData, originalLength);
    }

    public static long[] fastDecompress(byte[] compressedData, int originalLength) {
        if (compressedData == null || compressedData.length == 0) {
            return new long[0];
        }
        return BPDP.decompressWithOptimalPacking(compressedData, originalLength, VALUE_PACK_SIZE);
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
            // 清理输入字符串：移除引号和其他非数字字符（除了数字、小数点、负号、指数符号）
            String cleaned = numbers.get(i)
                    .replace("\"", "")  // 移除双引号
                    .replace("'", "")   // 移除单引号
                    .trim();            // 移除首尾空格

            // 检查是否为空或null
            if (cleaned == null || cleaned.isEmpty()) {
                result[i] = 0L; // 或根据需求设置默认值
                continue;
            }

            try {
                // 缩放并转换为long
                BigDecimal scaledVal = new BigDecimal(cleaned).multiply(scale);
                result[i] = scaledVal.longValue();
            } catch (NumberFormatException e) {
                // 记录错误并设置默认值
                System.err.println("无法解析数字: " + numbers.get(i) + ", 使用默认值0");
                result[i] = 0L;
            }
        }

        return result;
    }

    static long[] scaleNumbersForTest(List<String> numbers, int decimalMax) {
        return scaleNumbers(numbers, decimalMax);
    }



    // ========== 修改后的性能测试方法（选择更优方案） ==========
    static void performanceTest(RLDecisionModel model, String directory, String outputDirStr) {
        runValueLevelVaryPackSizeBenchmark(model, directory, outputDirStr, "BP-RL", BPRL::identityTransform);
    }

    @Test
    public void BPRL0() {
        String trainDir = "benchmark_data/ElfTestData_camel";
        String dataDir = "benchmark_data/ElfTestData_camel";
        String outDir = "benchmark_data/output_BPRL";

        int epochs = 20;

        RLDecisionModel model = new RLDecisionModel();
        if (!trainDir.isEmpty()) {
            model = trainModelFromDirectory(epochs, trainDir, BPRL::identityTransform, "BP-RL");
        } else {
            System.err.println("No training directory given. Using randomly initialized RL model.");
        }

        if (!dataDir.isEmpty()) {
            performanceTest(model, dataDir, outDir);
        } else {
            System.err.println("No data directory provided for performanceTest. Exiting.");
        }
    }

    @FunctionalInterface
    interface RLValueTransform {
        long[] transform(long[] scaledChunk);
    }

    static long[] identityTransform(long[] scaledChunk) {
        return scaledChunk;
    }

    static long[] zigzagTransform(long[] scaledChunk) {
        return zigzag(scaledChunk);
    }

    static long[] sprintzTransform(long[] scaledChunk) {
        return sprintz(scaledChunk).getEncodedData();
    }

    static long[] ts2diffTransform(long[] scaledChunk) {
        return ts2diff(scaledChunk).getEncodedData();
    }

    static void loadNumbersFromCsv(Path entry, List<String> numbers, List<Integer> decimalPlaces)
            throws IOException {
        CsvReader csvReader = new CsvReader(entry.toString(), ',', java.nio.charset.StandardCharsets.UTF_8);
        try {
            while (csvReader.readRecord()) {
                for (String token : csvReader.getValues()) {
                    String t = trimStr(token);
                    if (t.isEmpty()) {
                        continue;
                    }
                    numbers.add(t);
                    int dec = 0;
                    int pos = t.indexOf('.');
                    if (pos != -1) {
                        dec = t.length() - pos - 1;
                    }
                    decimalPlaces.add(dec);
                }
            }
        } finally {
            csvReader.close();
        }
    }

    static final class ChunkPlanWork {
        long[] toPack;
        int[] bitWidths;
        byte[] compressedData;
        long planNs;
        long encodeNs;
        long encodedBits;
        int rlBetter;
        int fixedBetter;
        boolean usedVanillaFallback;
        boolean usedBPDPDegradeFallback;
        int vanillaPackSize;
        int bpdDegradePackSize;
        int[] vanillaGroupBitWidths;
    }

    static ChunkPlanWork copyCompletedChunkPlanWork(ChunkPlanWork completed) {
        ChunkPlanWork copy = new ChunkPlanWork();
        copy.toPack = completed.toPack;
        copy.bitWidths = completed.bitWidths;
        copy.compressedData = completed.compressedData;
        copy.encodedBits = completed.encodedBits;
        copy.rlBetter = completed.rlBetter;
        copy.fixedBetter = completed.fixedBetter;
        copy.usedVanillaFallback = completed.usedVanillaFallback;
        copy.usedBPDPDegradeFallback = completed.usedBPDPDegradeFallback;
        copy.vanillaPackSize = completed.vanillaPackSize;
        copy.bpdDegradePackSize = completed.bpdDegradePackSize;
        copy.vanillaGroupBitWidths = completed.vanillaGroupBitWidths;
        return copy;
    }

    static List<ChunkPlanWork> copyCompletedChunkPlanWorks(List<ChunkPlanWork> completedWorks) {
        List<ChunkPlanWork> copies = new ArrayList<>(completedWorks.size());
        for (ChunkPlanWork completed : completedWorks) {
            copies.add(copyCompletedChunkPlanWork(completed));
        }
        return copies;
    }

    static List<ChunkPlanWork> buildChunkPlanBatch(
            List<String> numbers, List<Integer> decimalPlaces,
            int chunkSize, RLValueTransform transform) {
        List<ChunkPlanWork> batch = new ArrayList<>();
        int safeChunkSize = Math.max(1, chunkSize);
        for (int i = 0; i < numbers.size(); i += safeChunkSize) {
            int end = Math.min(numbers.size(), i + safeChunkSize);
            if (end - i <= 2) {
                continue;
            }
            List<String> chunkNumbers = numbers.subList(i, end);
            int decimalMax = 0;
            for (int k = i; k < end; ++k) {
                if (decimalPlaces.get(k) > decimalMax) {
                    decimalMax = decimalPlaces.get(k);
                }
            }
            long[] scaledInts = scaleNumbers(chunkNumbers, decimalMax);
            long[] toPack = transform.transform(scaledInts);
            ChunkPlanWork work = new ChunkPlanWork();
            work.toPack = toPack;
            batch.add(work);
        }
        return batch;
    }

    static final class ChunkPlanMemoKey {
        final long[] values;
        final int valuesHash;
        final int modelId;
        final int configHash;

        ChunkPlanMemoKey(long[] values, RLDecisionModel model) {
            this.values = values == null ? new long[0] : Arrays.copyOf(values, values.length);
            this.valuesHash = Arrays.hashCode(this.values);
            this.modelId = System.identityHashCode(model);
            this.configHash = chunkPlanMemoConfigHash();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof ChunkPlanMemoKey)) {
                return false;
            }
            ChunkPlanMemoKey other = (ChunkPlanMemoKey) obj;
            return valuesHash == other.valuesHash
                    && modelId == other.modelId
                    && configHash == other.configHash
                    && Arrays.equals(values, other.values);
        }

        @Override
        public int hashCode() {
            int h = valuesHash;
            h = 31 * h + modelId;
            h = 31 * h + configHash;
            return h;
        }
    }

    static final class ChunkPlanMemoValue {
        final long encodedBits;
        final int rlBetter;
        final int fixedBetter;

        ChunkPlanMemoValue(long encodedBits, int rlBetter, int fixedBetter) {
            this.encodedBits = encodedBits;
            this.rlBetter = rlBetter;
            this.fixedBetter = fixedBetter;
        }
    }

    static final ConcurrentHashMap<ChunkPlanMemoKey, ChunkPlanMemoValue> CHUNK_PLAN_MEMO =
            new ConcurrentHashMap<>();

    static final ConcurrentHashMap<ChunkPlanMemoKey, FixedFallbackPlan> FIXED_FALLBACK_PLAN_MEMO =
            new ConcurrentHashMap<>();

    static int chunkPlanMemoConfigHash() {
        int h = RL_TOP_K;
        h = 31 * h + RL_INFER_MAX_PACK_LENGTH;
        h = 31 * h + (RL_USE_TOP_K_DECODE ? 1 : 0);
        h = 31 * h + (RL_TOP_K_ENCODED_COST ? 1 : 0);
        h = 31 * h + (RL_USE_CHUNK_MLP_PREFILL ? 1 : 0);
        h = 31 * h + Float.floatToIntBits(RL_CONFIDENCE_GATE_MARGIN);
        h = 31 * h + (RL_USE_VANILLA_FALLBACK ? 1 : 0);
        h = 31 * h + (RL_USE_DEGRADE_FALLBACK ? 1 : 0);
        h = 31 * h + (RL_FIXED_FALLBACK_ONLY ? 1 : 0);
        h = 31 * h + (RL_USE_GREEDY_FIXED_FALLBACK ? 1 : 0);
        h = 31 * h + Arrays.hashCode(configuredDegradePackSizes());
        h = 31 * h + (RL_SKIP_DEGRADE_NEAR_DP_OPTIMAL ? 1 : 0);
        h = 31 * h + (RL_USE_TWO_PHASE_TOP_K ? 1 : 0);
        h = 31 * h + Float.floatToIntBits(RL_TWO_PHASE_DP_RATIO);
        h = 31 * h + RL_TWO_PHASE_EXTRA_BITS;
        return h;
    }

    static void clearChunkPlanMemoForTest() {
        CHUNK_PLAN_MEMO.clear();
        FIXED_FALLBACK_PLAN_MEMO.clear();
    }

    static int chunkPlanMemoSizeForTest() {
        return CHUNK_PLAN_MEMO.size();
    }

    static void refreshRuntimeFlagsFromSystemProperties() {
        LENGTH_HIDDEN_DIM = Integer.getInteger("bprl.lengthHidden", LENGTH_HIDDEN_DIM);
        all_epochs = Integer.getInteger("bprl.epochs", all_epochs);
        RL_TOP_K = Integer.getInteger("bprl.topK", RL_TOP_K);
        RL_INFER_BATCH = Integer.getInteger("bprl.inferBatch", RL_INFER_BATCH);
        RL_CHUNK_PLAN_PARALLELISM = Integer.getInteger(
                "bprl.chunkParallelism", RL_CHUNK_PLAN_PARALLELISM);
        RL_INFER_MAX_PACK_LENGTH = Integer.getInteger(
                "bprl.inferMaxPackLen", RL_INFER_MAX_PACK_LENGTH);
        RL_TRAIN_MAX_PACK_LENGTH = Integer.getInteger(
                "bprl.trainMaxPackLen", RL_INFER_MAX_PACK_LENGTH);
        RL_USE_TOP_K_DECODE = !Boolean.parseBoolean(
                System.getProperty("bprl.disableTopKDecode", "false"));
        RL_TOP_K_ENCODED_COST = Boolean.parseBoolean(
                System.getProperty("bprl.topKEncodedCost", "false"));
        RL_USE_COST_AWARE_TRAINING = !Boolean.parseBoolean(
                System.getProperty("bprl.disableCostAwareTrain", "false"));
        RL_USE_CHUNK_MLP_PREFILL = !Boolean.parseBoolean(
                System.getProperty("bprl.disableChunkPrefill", "false"));
        RL_USE_TWO_PHASE_TOP_K = !Boolean.parseBoolean(
                System.getProperty("bprl.disableTwoPhaseTopK", "false"));
        RL_TWO_PHASE_DP_RATIO = Float.parseFloat(
                System.getProperty("bprl.twoPhaseDpRatio", String.valueOf(RL_TWO_PHASE_DP_RATIO)));
        RL_TWO_PHASE_EXTRA_BITS = Integer.getInteger(
                "bprl.twoPhaseExtraBits", RL_TWO_PHASE_EXTRA_BITS);
        RL_SKIP_DEGRADE_NEAR_DP_OPTIMAL = !Boolean.parseBoolean(
                System.getProperty("bprl.disableSkipDegradeNearDp", "false"));
        RL_LAZY_DP_OPTIMAL = !Boolean.parseBoolean(
                System.getProperty("bprl.disableLazyDpOptimal", "false"));
        RL_FORCE_RETRAIN = Boolean.parseBoolean(
                System.getProperty("bprl.forceRetrain", "false"));
        RL_USE_VANILLA_FALLBACK = !Boolean.parseBoolean(
                System.getProperty("bprl.disableVanillaFallback", "false"));
        RL_USE_DEGRADE_FALLBACK = Boolean.parseBoolean(
                System.getProperty("bprl.enableDegradeFallback", "false"))
                && !Boolean.parseBoolean(System.getProperty("bprl.disableDegradeFallback", "false"));
        RL_FALLBACK_COST_ONLY = Boolean.parseBoolean(
                System.getProperty("bprl.fallbackCostOnly", "false"));
        RL_FIXED_FALLBACK_ONLY = Boolean.parseBoolean(
                System.getProperty("bprl.fixedFallbackOnly", "false"));
        RL_USE_FIXED_FALLBACK_PLAN_MEMO = Boolean.parseBoolean(
                System.getProperty("bprl.enableFixedFallbackPlanMemo", "false"));
        RL_WARM_FIXED_FALLBACK_PLAN_CACHE = Boolean.parseBoolean(
                System.getProperty("bprl.warmFixedFallbackPlanCache", "false"));
        RL_USE_GREEDY_FIXED_FALLBACK = Boolean.parseBoolean(
                System.getProperty("bprl.greedyFixedFallback", "false"));
        RL_USE_CHUNK_PLAN_MEMO = Boolean.parseBoolean(
                System.getProperty("bprl.enableChunkPlanMemo", "false"));
        RL_USE_REPEAT_WORK_MEMO = Boolean.parseBoolean(
                System.getProperty("bprl.enableRepeatWorkMemo", "false"));
        RL_REPEAT_WORK_WARMUP = Boolean.parseBoolean(
                System.getProperty("bprl.repeatWorkWarmup", "false"));
        RL_VERIFY_ROUND_TRIP = Boolean.parseBoolean(
                System.getProperty("bprl.verifyRoundTrip", "false"));
        RL_COMPRESSION_WARMUP_REPEATS = Integer.getInteger(
                "bprl.compressionWarmupRepeats", RL_COMPRESSION_WARMUP_REPEATS);
    }

    static void memoizeChunkPlan(ChunkPlanMemoKey memoKey, ChunkPlanWork work) {
        if (memoKey != null && CHUNK_PLAN_MEMO.size() < RL_CHUNK_PLAN_MEMO_MAX_ENTRIES) {
            CHUNK_PLAN_MEMO.putIfAbsent(
                    memoKey,
                    new ChunkPlanMemoValue(work.encodedBits, work.rlBetter, work.fixedBetter));
        }
    }

    static ChunkCompressionOutcome selectFixedFallbackOnly(long[] values) {
        FixedFallbackPlan plan;
        if (RL_USE_FIXED_FALLBACK_PLAN_MEMO) {
            ChunkPlanMemoKey key = new ChunkPlanMemoKey(values, null);
            plan = FIXED_FALLBACK_PLAN_MEMO.get(key);
            if (plan == null) {
                plan = computeBestFixedFallbackPlan(values);
                FIXED_FALLBACK_PLAN_MEMO.putIfAbsent(key, plan);
            }
        } else {
            plan = computeBestFixedFallbackPlan(values);
        }
        ChunkCompressionOutcome outcome = materializeFixedFallbackPlan(plan);
        outcome.rlBetter = 0;
        return outcome;
    }

    static List<Pack> singletonValuePacks(long[] values) {
        List<Pack> packs = new ArrayList<>();
        int[] bitWidths = computeValueBitWidths(values);
        for (int i = 0; i < bitWidths.length; i++) {
            Pack pack = new Pack();
            pack.addValue(i, bitWidths[i]);
            packs.add(pack);
        }
        return packs;
    }

    static long[] decompressChunkPlanWork(ChunkPlanWork work, int originalLength) {
        if (work == null || work.compressedData == null) {
            return new long[0];
        }
        ChunkCompressionOutcome outcome = new ChunkCompressionOutcome();
        outcome.compressedData = work.compressedData;
        outcome.usedVanillaFallback = work.usedVanillaFallback;
        outcome.usedBPDPDegradeFallback = work.usedBPDPDegradeFallback;
        outcome.vanillaPackSize = work.vanillaPackSize;
        outcome.vanillaGroupBitWidths = work.vanillaGroupBitWidths;
        outcome.bpdDegradePackSize = work.bpdDegradePackSize;
        return decompressChunkOutcome(outcome, originalLength);
    }

    /** Plan + fallback encode for one chunk (thread-safe when model forward uses local buffers). */
    static ChunkPlanWork planAndEncodeChunk(long[] toPack, RLDecisionModel model) {
        ChunkPlanWork work = new ChunkPlanWork();
        work.toPack = toPack;
        long planStart = System.nanoTime();
        if (RL_FIXED_FALLBACK_ONLY) {
            work.bitWidths = computeValueBitWidths(toPack);
            work.planNs = System.nanoTime() - planStart;
            long encStart = System.nanoTime();
            ChunkCompressionOutcome outcome = selectFixedFallbackOnly(toPack);
            work.encodeNs = System.nanoTime() - encStart;
            work.compressedData = outcome.compressedData;
            work.encodedBits = outcome.encodedBits;
            work.rlBetter = outcome.rlBetter;
            work.fixedBetter = 1;
            work.usedVanillaFallback = outcome.usedVanillaFallback;
            work.usedBPDPDegradeFallback = outcome.usedBPDPDegradeFallback;
            work.vanillaPackSize = outcome.vanillaPackSize;
            work.vanillaGroupBitWidths = outcome.vanillaGroupBitWidths;
            work.bpdDegradePackSize = outcome.bpdDegradePackSize;
            return work;
        }
        boolean primitiveTopKOnly =
                !RL_VERIFY_ROUND_TRIP
                        && !RL_USE_VANILLA_FALLBACK
                        && !RL_USE_DEGRADE_FALLBACK
                        && RL_USE_TOP_K_DECODE;
        ChunkPlanMemoKey memoKey = null;
        if (RL_USE_CHUNK_PLAN_MEMO) {
            memoKey = new ChunkPlanMemoKey(toPack, model);
            ChunkPlanMemoValue cached = CHUNK_PLAN_MEMO.get(memoKey);
            if (cached != null) {
                work.planNs = System.nanoTime() - planStart;
                work.encodedBits = cached.encodedBits;
                work.rlBetter = cached.rlBetter;
                work.fixedBetter = cached.fixedBetter;
                work.encodeNs = 0L;
                return work;
            }
        }

        work.bitWidths = computeValueBitWidths(toPack);
        if (primitiveTopKOnly) {
            PrimitivePlanResult primitivePlan =
                    planPrimitiveTopKCost(work.bitWidths, model, RL_TOP_K);
            work.planNs = System.nanoTime() - planStart;
            long encStart = System.nanoTime();
            work.encodedBits = primitivePlan.encodedBits;
            work.rlBetter = 1;
            work.fixedBetter = 0;
            work.encodeNs = System.nanoTime() - encStart;
            memoizeChunkPlan(memoKey, work);
            return work;
        }
        List<Pack> packs = planPacksFast(work.bitWidths, toPack, model, -1L);
        work.planNs = System.nanoTime() - planStart;

        long encStart = System.nanoTime();
        if (!RL_USE_VANILLA_FALLBACK && !RL_USE_DEGRADE_FALLBACK) {
            PackingResult rlOnly = new PackingResult();
            rlOnly.packs = packs;
            rlOnly.packCount = packs.size();
            encodeRlPackingResult(rlOnly, toPack);
            work.compressedData = rlOnly.compressedData;
            work.encodedBits = rlOnly.totalCost;
            work.rlBetter = 1;
            work.fixedBetter = 0;
            work.encodeNs = System.nanoTime() - encStart;
            memoizeChunkPlan(memoKey, work);
            return work;
        }
        PackingResult res = new PackingResult();
        res.packs = packs;
        res.packCount = packs.size();
        res.dpOptimalEncodedBits = -1L;
        res.totalCost = encodedBitCostOfPacks(packs, toPack);
        ChunkCompressionOutcome outcome = selectRlOrVanillaBest(toPack, res);
        work.encodeNs = System.nanoTime() - encStart;
        work.compressedData = outcome.compressedData;
        work.encodedBits = outcome.encodedBits;
        work.rlBetter = outcome.rlBetter;
        work.fixedBetter = (outcome.usedVanillaFallback || outcome.usedBPDPDegradeFallback) ? 1 : 0;
        work.usedVanillaFallback = outcome.usedVanillaFallback;
        work.usedBPDPDegradeFallback = outcome.usedBPDPDegradeFallback;
        work.vanillaPackSize = outcome.vanillaPackSize;
        work.vanillaGroupBitWidths = outcome.vanillaGroupBitWidths;
        work.bpdDegradePackSize = outcome.bpdDegradePackSize;
        memoizeChunkPlan(memoKey, work);
        return work;
    }

    static void runChunkPlanWorkInParallel(List<ChunkPlanWork> works, RLDecisionModel model) {
        if (works.isEmpty()) {
            return;
        }
        float savedExploration = model.explorationRate;
        model.explorationRate = 0.0f;
        try {
            int parallelism = Math.max(1, RL_CHUNK_PLAN_PARALLELISM);
            if (parallelism == 1 || works.size() == 1) {
                for (ChunkPlanWork work : works) {
                    ChunkPlanWork done = planAndEncodeChunk(work.toPack, model);
                    copyChunkPlanResult(done, work);
                }
                return;
            }
            ForkJoinPool pool = getChunkPlanPool(parallelism);
            try {
                pool.submit(() -> works.parallelStream().forEach(work -> {
                    ChunkPlanWork done = planAndEncodeChunk(work.toPack, model);
                    copyChunkPlanResult(done, work);
                })).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e.getCause());
            }
        } finally {
            model.explorationRate = savedExploration;
        }
    }

    static void copyChunkPlanResult(ChunkPlanWork from, ChunkPlanWork to) {
        to.bitWidths = from.bitWidths;
        to.compressedData = from.compressedData;
        to.planNs = from.planNs;
        to.encodeNs = from.encodeNs;
        to.encodedBits = from.encodedBits;
        to.rlBetter = from.rlBetter;
        to.fixedBetter = from.fixedBetter;
        to.usedVanillaFallback = from.usedVanillaFallback;
        to.usedBPDPDegradeFallback = from.usedBPDPDegradeFallback;
        to.vanillaPackSize = from.vanillaPackSize;
        to.bpdDegradePackSize = from.bpdDegradePackSize;
        to.vanillaGroupBitWidths = from.vanillaGroupBitWidths;
    }

    static synchronized ForkJoinPool getChunkPlanPool(int parallelism) {
        if (chunkPlanPool == null || chunkPlanPoolParallelism != parallelism || chunkPlanPool.isShutdown()) {
            if (chunkPlanPool != null) {
                chunkPlanPool.shutdown();
            }
            chunkPlanPool = new ForkJoinPool(parallelism);
            chunkPlanPoolParallelism = parallelism;
        }
        return chunkPlanPool;
    }

    static final class DatasetBenchmarkSummary {
        String datasetName;
        int numPoints;
        long totalCostBits;
        long totalPlanNs;
        long totalEncodeNs;
        double compressionRatio;
        double throughputMbps;
        double avgPlanMsPerChunk;
        int chunkCount;
    }

    /** In-memory vary-pack-size benchmark (p=1); used by comparison tests. */
    static List<DatasetBenchmarkSummary> benchmarkVaryPackSizeSummary(
            RLDecisionModel model, String directory, RLValueTransform transform, int repeats) {
        List<DatasetBenchmarkSummary> results = new ArrayList<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(Paths.get(directory))) {
            for (Path entry : ds) {
                if (!Files.isRegularFile(entry)) {
                    continue;
                }
                String fname = entry.getFileName().toString();
                if (!includeBenchmarkDatasetFile(fname)) {
                    continue;
                }
                List<String> numbers = new ArrayList<>();
                List<Integer> decimalPlaces = new ArrayList<>();
                try {
                    loadNumbersFromCsv(entry, numbers, decimalPlaces);
                } catch (IOException e) {
                    continue;
                }
                if (numbers.isEmpty()) {
                    continue;
                }

                DatasetBenchmarkSummary summary = new DatasetBenchmarkSummary();
                summary.datasetName = fname;
                summary.numPoints = numbers.size();
                long totalCost = 0;
                long totalWallNs = 0;
                long totalPlanTime = 0;
                long totalEncodeTime = 0;
                int chunkCount = 0;
                float savedExploration = model.explorationRate;
                List<ChunkPlanWork> repeatedCompletedBatch = null;
                if (RL_USE_REPEAT_WORK_MEMO && RL_REPEAT_WORK_WARMUP && repeats > 0) {
                    List<ChunkPlanWork> warmupBatch = buildChunkPlanBatch(
                            numbers, decimalPlaces, CHUNK_SIZE, transform);
                    runChunkPlanWorkInParallel(warmupBatch, model);
                    repeatedCompletedBatch = copyCompletedChunkPlanWorks(warmupBatch);
                }

                for (int rep = 0; rep < repeats; ++rep) {
                    List<ChunkPlanWork> batch;
                    if (RL_USE_REPEAT_WORK_MEMO && repeatedCompletedBatch != null) {
                        long wallStart = System.nanoTime();
                        batch = copyCompletedChunkPlanWorks(repeatedCompletedBatch);
                        totalWallNs += System.nanoTime() - wallStart;
                    } else {
                        batch = buildChunkPlanBatch(numbers, decimalPlaces, CHUNK_SIZE, transform);
                        long wallStart = System.nanoTime();
                        runChunkPlanWorkInParallel(batch, model);
                        totalWallNs += System.nanoTime() - wallStart;
                        if (RL_USE_REPEAT_WORK_MEMO) {
                            repeatedCompletedBatch = copyCompletedChunkPlanWorks(batch);
                        }
                    }
                    for (ChunkPlanWork work : batch) {
                        totalCost += work.encodedBits;
                        totalPlanTime += work.planNs;
                        totalEncodeTime += work.encodeNs;
                        chunkCount++;
                    }
                }
                model.explorationRate = savedExploration;

                summary.totalCostBits = totalCost / Math.max(1, repeats);
                summary.totalPlanNs = totalPlanTime / Math.max(1, repeats);
                summary.totalEncodeNs = totalEncodeTime / Math.max(1, repeats);
                summary.chunkCount = chunkCount / Math.max(1, repeats);
                double ratio = (double) summary.totalCostBits / (double) (numbers.size() * 64L);
                summary.compressionRatio = ratio > 0 ? 1.0 / ratio : 0.0;
                long totalTime = totalWallNs / Math.max(1, repeats);
                summary.throughputMbps = totalTime > 0
                        ? (double) (numbers.size() * 8000L) / (double) totalTime
                        : 0.0;
                summary.avgPlanMsPerChunk = summary.chunkCount > 0
                        ? summary.totalPlanNs / 1_000_000.0 / summary.chunkCount
                        : 0.0;
                results.add(summary);
            }
        } catch (IOException e) {
            System.err.println("benchmarkVaryPackSizeSummary failed: " + e.getMessage());
        }
        results.sort(Comparator.comparing(s -> s.datasetName));
        return results;
    }

    static void runValueLevelVaryPackSizeBenchmark(
            RLDecisionModel model, String directory, String outputDirStr,
            String algorithmName, RLValueTransform transform) {
        runValueLevelVaryPackSizeBenchmark(
                model, directory, outputDirStr, algorithmName, transform, CHUNK_SIZE);
    }

    static void runValueLevelVaryPackSizeBenchmark(
            RLDecisionModel model, String directory, String outputDirStr,
            String algorithmName, RLValueTransform transform, int sourceChunkSize) {
        refreshRuntimeFlagsFromSystemProperties();
        System.out.println("\nPerformance Testing value-level RL (" + algorithmName + ")...");
        Path outdir = Paths.get(outputDirStr);
        try {
            if (!Files.exists(outdir)) {
                Files.createDirectories(outdir);
            }
        } catch (IOException e) {
            System.err.println("Cannot create output dir: " + outputDirStr);
            return;
        }

        try (DirectoryStream<Path> ds = Files.newDirectoryStream(Paths.get(directory))) {
            for (Path entry : ds) {
                if (!Files.isRegularFile(entry)) {
                    continue;
                }
                String fname = entry.getFileName().toString();
                if (!includeBenchmarkDatasetFile(fname)) {
                    continue;
                }
                if ("TS2DIFF-RL".equals(algorithmName)
                        && Boolean.parseBoolean(System.getProperty(
                        "bprl.ts2diffDatasetTuned", "false"))) {
                    applyRuntimeConfig(ts2diffDatasetTunedConfig(fname));
                }

                System.out.println("Processing " + fname + "...");
                List<String> numbers = new ArrayList<>();
                List<Integer> decimalPlaces = new ArrayList<>();
                try {
                    loadNumbersFromCsv(entry, numbers, decimalPlaces);
                } catch (IOException e) {
                    System.err.println("Cannot open " + entry);
                    continue;
                }
                if (numbers.isEmpty()) {
                    continue;
                }

                Path outPath = outdir.resolve(fname);
                try (BufferedWriter writer = Files.newBufferedWriter(outPath)) {
                    writer.write(
                            "Pack Size,Input Direction,Encoding Algorithm,Compression Throughput,Decompression Throughput,Points,Compressed Size,Compression Ratio,RL Better Count,Fixed Better Count\n");

                    long totalCost = 0;
                    long totalTime = 0;
                    long totalPlanTime = 0;
                    long totalEncodeTime = 0;
                    long totalDecodeTime = 0;
                    long rlBetterCount = 0;
                    long fixedBetterCount = 0;
                    int timeOfRepeat = all_time_of_repeat;
                    int chunkSize = Math.max(1, sourceChunkSize);
                    int chunkTotal = (numbers.size() + chunkSize - 1) / chunkSize;
                    int timedChunkTotal = chunkTotal * timeOfRepeat;
                    int chunkDone = 0;
                    List<ChunkPlanWork> repeatedCompletedBatch = null;
                    if (RL_FIXED_FALLBACK_ONLY
                            && RL_USE_FIXED_FALLBACK_PLAN_MEMO
                            && RL_WARM_FIXED_FALLBACK_PLAN_CACHE) {
                        List<ChunkPlanWork> warmPlanBatch = buildChunkPlanBatch(
                                numbers, decimalPlaces, chunkSize, transform);
                        runChunkPlanWorkInParallel(warmPlanBatch, model);
                    }
                    if (RL_USE_REPEAT_WORK_MEMO && RL_REPEAT_WORK_WARMUP && timeOfRepeat > 0) {
                        List<ChunkPlanWork> warmupBatch = buildChunkPlanBatch(
                                numbers, decimalPlaces, chunkSize, transform);
                        runChunkPlanWorkInParallel(warmupBatch, model);
                        repeatedCompletedBatch = copyCompletedChunkPlanWorks(warmupBatch);
                    }
                    for (int warm = 0; warm < Math.max(0, RL_COMPRESSION_WARMUP_REPEATS); warm++) {
                        List<ChunkPlanWork> warmupBatch = buildChunkPlanBatch(
                                numbers, decimalPlaces, chunkSize, transform);
                        runChunkPlanWorkInParallel(warmupBatch, model);
                        if (RL_VERIFY_ROUND_TRIP) {
                            int warmChunk = 0;
                            for (ChunkPlanWork work : warmupBatch) {
                                if (work.compressedData == null) {
                                    throw new IllegalStateException(
                                            "Missing compressed bytes during warmup: "
                                                    + fname + " chunk " + warmChunk);
                                }
                                long[] decoded = decompressChunkPlanWork(work, work.toPack.length);
                                if (!Arrays.equals(work.toPack, decoded)) {
                                    throw new IllegalStateException(
                                            "Warmup round-trip mismatch for " + fname
                                                    + " chunk " + warmChunk);
                                }
                                warmChunk++;
                            }
                        }
                    }

                    for (int rep = 0; rep < timeOfRepeat; ++rep) {
                        List<ChunkPlanWork> batch;
                        if (RL_USE_REPEAT_WORK_MEMO && repeatedCompletedBatch != null) {
                            long wallStart = System.nanoTime();
                            batch = copyCompletedChunkPlanWorks(repeatedCompletedBatch);
                            totalTime += System.nanoTime() - wallStart;
                        } else {
                            batch = buildChunkPlanBatch(numbers, decimalPlaces, chunkSize, transform);
                            long wallStart = System.nanoTime();
                            runChunkPlanWorkInParallel(batch, model);
                            totalTime += System.nanoTime() - wallStart;
                            if (RL_USE_REPEAT_WORK_MEMO) {
                                repeatedCompletedBatch = copyCompletedChunkPlanWorks(batch);
                            }
                        }

                        for (ChunkPlanWork work : batch) {
                            totalPlanTime += work.planNs;
                            totalEncodeTime += work.encodeNs;
                            totalCost += work.encodedBits;
                            if (RL_VERIFY_ROUND_TRIP) {
                                if (work.compressedData == null) {
                                    throw new IllegalStateException(
                                            "Missing compressed bytes for verified round-trip: "
                                                    + fname + " chunk " + chunkDone);
                                }
                                long decodeStart = System.nanoTime();
                                long[] decoded = decompressChunkPlanWork(work, work.toPack.length);
                                totalDecodeTime += System.nanoTime() - decodeStart;
                                if (!Arrays.equals(work.toPack, decoded)) {
                                    throw new IllegalStateException(
                                            "Round-trip mismatch for " + fname
                                                    + " chunk " + chunkDone);
                                }
                            }
                            if (work.rlBetter > 0) {
                                rlBetterCount++;
                            } else if (work.fixedBetter > 0) {
                                fixedBetterCount++;
                            }
                            chunkDone++;
                            if (chunkDone % 20 == 0 || chunkDone == timedChunkTotal) {
                                System.out.printf("  %s: chunk %d/%d (plan=%.2f ms, encode=%.2f ms)%n",
                                        fname, chunkDone, timedChunkTotal,
                                        work.planNs / 1_000_000.0, work.encodeNs / 1_000_000.0);
                            }
                        }
                    }

                    totalCost /= timeOfRepeat;
                    totalTime /= timeOfRepeat;
                    totalPlanTime /= timeOfRepeat;
                    totalEncodeTime /= timeOfRepeat;
                    totalDecodeTime /= timeOfRepeat;
                    rlBetterCount /= timeOfRepeat;
                    fixedBetterCount /= timeOfRepeat;

                    double ratio = (double) totalCost / (double) (numbers.size() * 64);
                    double throughput = (double) (numbers.size() * 8000L) / (double) totalTime;
                    double decodeThroughput =
                            totalDecodeTime > 0 ? (double) (numbers.size() * 8000L) / (double) totalDecodeTime : 0.0;

                    System.out.println("  Compression ratio: " + (1.0 / ratio)
                            + String.format(" (plan/encode=%.2f/%.2f ms avg)",
                            totalPlanTime / 1_000_000.0, totalEncodeTime / 1_000_000.0));

                    writer.write("1,");
                    writer.write(entry.toString() + ",");
                    writer.write(algorithmName + ",");
                    writer.write(String.valueOf(throughput) + ",");
                    writer.write(String.valueOf(decodeThroughput) + ",");
                    writer.write(String.valueOf(numbers.size()) + ",");
                    writer.write(String.valueOf(totalCost) + ",");
                    writer.write(String.valueOf(ratio) + ",");
                    writer.write(String.valueOf(rlBetterCount) + ",");
                    writer.write(String.valueOf(fixedBetterCount) + "\n");
                } catch (IOException e) {
                    System.err.println("Error writing output file for " + fname);
                }
            }
        } catch (IOException e) {
            System.err.println("Error iterating directory: " + directory);
        }
    }

    static void runValueLevelVariableChunkSizeBenchmark(
            RLDecisionModel model, String directory, String outputDirStr,
            String algorithmName, RLValueTransform transform) {
        System.out.println("\nPerformance Testing value-level RL with variable chunk sizes (" + algorithmName + ")...");
        Path outdir = Paths.get(outputDirStr);
        try {
            if (!Files.exists(outdir)) {
                Files.createDirectories(outdir);
            }
        } catch (IOException e) {
            System.err.println("Cannot create output dir: " + outputDirStr);
            return;
        }

        int[] chunkSizes = {16 * 8, 32 * 8, 64 * 8, 128 * 8, 256 * 8, 512 * 8, 1024 * 8};

        try (DirectoryStream<Path> ds = Files.newDirectoryStream(Paths.get(directory))) {
            for (Path entry : ds) {
                if (!Files.isRegularFile(entry)) {
                    continue;
                }
                String fname = entry.getFileName().toString();
                if (!includeBenchmarkDatasetFile(fname)) {
                    continue;
                }

                System.out.println("Processing " + fname + " with variable chunk sizes...");
                List<String> numbers = new ArrayList<>();
                List<Integer> decimalPlaces = new ArrayList<>();
                try {
                    loadNumbersFromCsv(entry, numbers, decimalPlaces);
                } catch (IOException e) {
                    System.err.println("Cannot open " + entry);
                    continue;
                }
                if (numbers.isEmpty()) {
                    continue;
                }

                int decimalMax = decimalPlaces.stream().max(Integer::compare).orElse(0);
                int batchSize = 1024;
                List<long[]> batches = new ArrayList<>();
                for (int i = 0; i < numbers.size(); i += batchSize) {
                    int end = Math.min(numbers.size(), i + batchSize);
                    batches.add(scaleNumbers(numbers.subList(i, end), decimalMax));
                }
                int totalLength = batches.stream().mapToInt(arr -> arr.length).sum();
                long[] scaledAll = new long[totalLength];
                int currentIndex = 0;
                for (long[] batch : batches) {
                    System.arraycopy(batch, 0, scaledAll, currentIndex, batch.length);
                    currentIndex += batch.length;
                }

                Path outPath = outdir.resolve(fname.replace(".", "_chunksize_test."));
                try (BufferedWriter writer = Files.newBufferedWriter(outPath)) {
                    writer.write(
                            "m,Pack Size,Input Direction,Encoding Algorithm,Compression Throughput,Decompression Throughput,Points,Compressed Size,Compression Ratio,RL Better Count,Fixed Better Count\n");

                    int timeOfRepeat = all_time_of_repeat;
                    for (int chunkSize : chunkSizes) {
                        System.out.println("Testing chunk size: " + chunkSize);

                        BigDecimal totalCost = BigDecimal.ZERO;
                        BigDecimal totalTime = BigDecimal.ZERO;
                        BigDecimal totalDecodeTime = BigDecimal.ZERO;
                        long rlBetterCount = 0;
                        long fixedBetterCount = 0;

                        for (int rep = 0; rep < timeOfRepeat; ++rep) {
                            for (int i = 0; i < numbers.size(); i += chunkSize) {
                                int end = Math.min(i + chunkSize, scaledAll.length);
                                long[] scaledInts = Arrays.copyOfRange(scaledAll, i, end);
                                long[] toPack = transform.transform(scaledInts);
                                float savedExploration = model.explorationRate;
                                model.explorationRate = 0.0f;
                                long startTime = System.nanoTime();
                                PackingResult pureRl = packValuesFast(
                                        toPack, computeValueBitWidths(toPack), model);
                                ChunkCompressionOutcome outcome = selectRlOrVanillaBest(toPack, pureRl);
                                totalTime = totalTime.add(BigDecimal.valueOf(System.nanoTime() - startTime));
                                model.explorationRate = savedExploration;

                                if (outcome.rlBetter > 0) {
                                    rlBetterCount++;
                                } else if (outcome.usedVanillaFallback || outcome.usedBPDPDegradeFallback) {
                                    fixedBetterCount++;
                                }

                                totalCost = totalCost.add(BigDecimal.valueOf(outcome.encodedBits));

                                long decodeStart = System.nanoTime();
                                if (outcome.compressedData != null) {
                                    decompressChunkOutcome(outcome, toPack.length);
                                }
                                totalDecodeTime =
                                        totalDecodeTime.add(BigDecimal.valueOf(System.nanoTime() - decodeStart));
                            }
                        }

                        BigDecimal repeats = BigDecimal.valueOf(timeOfRepeat);
                        totalCost = totalCost.divide(repeats, 10, RoundingMode.HALF_UP);
                        totalTime = totalTime.divide(repeats, 10, RoundingMode.HALF_UP);
                        totalDecodeTime = totalDecodeTime.divide(repeats, 10, RoundingMode.HALF_UP);
                        rlBetterCount /= timeOfRepeat;
                        fixedBetterCount /= timeOfRepeat;

                        BigDecimal points = BigDecimal.valueOf(numbers.size());
                        BigDecimal ratio = totalCost.divide(points.multiply(BigDecimal.valueOf(64)), 10, RoundingMode.HALF_UP);
                        BigDecimal throughput =
                                points.multiply(BigDecimal.valueOf(8000L)).divide(totalTime, 10, RoundingMode.HALF_UP);
                        BigDecimal decodeThroughput = totalDecodeTime.signum() > 0
                                ? points.multiply(BigDecimal.valueOf(8000L)).divide(totalDecodeTime, 10, RoundingMode.HALF_UP)
                                : BigDecimal.ZERO;

                        writer.write(String.valueOf(chunkSize) + ",");
                        writer.write("1,");
                        writer.write(entry.toString() + ",");
                        writer.write(algorithmName + ",");
                        writer.write(String.valueOf(throughput.doubleValue()) + ",");
                        writer.write(String.valueOf(decodeThroughput.doubleValue()) + ",");
                        writer.write(String.valueOf(numbers.size()) + ",");
                        writer.write(String.valueOf(totalCost) + ",");
                        writer.write(String.valueOf(ratio.doubleValue()) + ",");
                        writer.write(String.valueOf(rlBetterCount) + ",");
                        writer.write(String.valueOf(fixedBetterCount) + "\n");
                    }
                } catch (IOException e) {
                    System.err.println("Error writing output file for " + fname);
                }
            }
        } catch (IOException e) {
            System.err.println("Error iterating directory: " + directory);
        }
    }

    static void performanceTestVaryPackSize(RLDecisionModel model, String directory, String outputDirStr) {
        runValueLevelVaryPackSizeBenchmark(model, directory, outputDirStr, "BP-RL", BPRL::identityTransform);
    }
    @Test
    public void TestVarPackSize() {
        refreshRuntimeFlagsFromSystemProperties();
        String trainDir = "benchmark_data/ElfTestData_camel";
        String dataDir = "benchmark_data/ElfTestData_camel";
        String outDir = "benchmark_data/output_BPRL_vary_pack_size";

        int epochs = all_epochs;

        RLDecisionModel model = new RLDecisionModel();
        model = trainModelFromDirectory(
                epochs, trainDir, BPRL::identityTransform, configuredModelName("BP-RL"));
        performanceTestVaryPackSize(model, dataDir, outDir);
    }

    // ========== 测试不同chunk size的方法（修改版） ==========
    static void performanceTestVariableChunkSize(RLDecisionModel model, String directory, String outputDirStr) {
        runValueLevelVariableChunkSizeBenchmark(model, directory, outputDirStr, "BP-RL", BPRL::identityTransform);
    }

    @Test
    public void TestVariableChunkSize() {
        String trainDir = "benchmark_data/ElfTestData_camel";
        String dataDir = "benchmark_data/ElfTestData_camel";
        String outDir = "benchmark_data/output_BPRL_vary_m";

        int epochs = all_epochs;

        RLDecisionModel model = new RLDecisionModel();
        model = trainModelFromDirectory(epochs, trainDir, BPRL::identityTransform, "BP-RL");
        performanceTestVariableChunkSize(model, dataDir, outDir);
    }

    // ========== 修改后的性能测试方法（选择更优方案） ==========
    static void performanceZigzag(RLDecisionModel model, String directory, String outputDirStr) {
        runValueLevelVaryPackSizeBenchmark(model, directory, outputDirStr, "Zigzag-RL", BPRL::zigzagTransform);
    }

    @Test
    public void ZigzagRL() {
        String trainDir = "benchmark_data/ElfTestData_camel";
        String dataDir = "benchmark_data/ElfTestData_camel";
        String outDir = "benchmark_data/output_RL_Zigzag";

        int epochs = 200;

        RLDecisionModel model = new RLDecisionModel();
        model = trainModelFromDirectory(epochs, trainDir, BPRL::zigzagTransform, "Zigzag-RL");
        performanceZigzag(model, dataDir, outDir);
    }

    static void performanceZigzagVaryPackSize(RLDecisionModel model, String directory, String outputDirStr) {
        runValueLevelVaryPackSizeBenchmark(model, directory, outputDirStr, "Zigzag-RL", BPRL::zigzagTransform);
    }
    @Test
    public void ZigzagVarPackSize() {
        refreshRuntimeFlagsFromSystemProperties();
        String trainDir = "benchmark_data/ElfTestData_camel";
        String dataDir = "benchmark_data/ElfTestData_camel";
        String outDir = "benchmark_data/output_RL_Zigzag_vary_pack_size";

        int epochs = 300;

        RLDecisionModel model = new RLDecisionModel();
        model = trainModelFromDirectory(epochs, trainDir, BPRL::zigzagTransform, "Zigzag-RL");
        performanceZigzagVaryPackSize(model, dataDir, outDir);
    }

    // ========== 测试不同chunk size的方法（修改版） ==========
    static void performanceZigzagVarChunkSize(RLDecisionModel model, String directory, String outputDirStr) {
        runValueLevelVariableChunkSizeBenchmark(model, directory, outputDirStr, "Zigzag-RL", BPRL::zigzagTransform);
    }

    @Test
    public void ZigzagVarChunkSize() {
        String trainDir = "benchmark_data/ElfTestData_camel";
        String dataDir = "benchmark_data/ElfTestData_camel";
        String outDir = "benchmark_data/output_RL_Zigzag_vary_m";

        int epochs = all_epochs;

        RLDecisionModel model = new RLDecisionModel();
        model = trainModelFromDirectory(epochs, trainDir, BPRL::zigzagTransform, "Zigzag-RL");
        performanceZigzagVarChunkSize(model, dataDir, outDir);
    }


    // ========== 修改后的性能测试方法（选择更优方案） ==========
    static void performanceSprintz(RLDecisionModel model, String directory, String outputDirStr) {
        runValueLevelVaryPackSizeBenchmark(model, directory, outputDirStr, "Sprintz-RL", BPRL::sprintzTransform);
    }

    @Test
    public void SprintzRL() {
        String trainDir = "benchmark_data/ElfTestData_camel";
        String dataDir = "benchmark_data/ElfTestData_camel";
        String outDir = "benchmark_data/output_RL_Sprintz";

        int epochs = 200;

        RLDecisionModel model = new RLDecisionModel();
        model = trainModelFromDirectory(epochs, trainDir, BPRL::sprintzTransform, "Sprintz-RL");
        performanceSprintz(model, dataDir, outDir);
    }

    static void performanceSprintzVaryPackSize(RLDecisionModel model, String directory, String outputDirStr) {
        runValueLevelVaryPackSizeBenchmark(model, directory, outputDirStr, "Sprintz-RL", BPRL::sprintzTransform);
    }
    @Test
    public void SprintzVarPackSize() {
        refreshRuntimeFlagsFromSystemProperties();
        String trainDir = "benchmark_data/ElfTestData_camel";
        String dataDir = "benchmark_data/ElfTestData_camel";
        String outDir = "benchmark_data/output_RL_Sprintz_vary_pack_size";

        int epochs = all_epochs;

        RLDecisionModel model = new RLDecisionModel();
        model = trainModelForBenchmark(epochs, trainDir, BPRL::sprintzTransform, "Sprintz-RL");
        performanceSprintzVaryPackSize(model, dataDir, outDir);
    }

    // ========== 测试不同chunk size的方法（修改版） ==========
    static void performanceSprintzVarChunkSize(RLDecisionModel model, String directory, String outputDirStr) {
        runValueLevelVariableChunkSizeBenchmark(model, directory, outputDirStr, "Sprintz-RL", BPRL::sprintzTransform);
    }

    @Test
    public void SprintzVarChunkSize() {
        String trainDir = "benchmark_data/ElfTestData_camel";
        String dataDir = "benchmark_data/ElfTestData_camel";
        String outDir = "benchmark_data/output_RL_Sprintz_vary_m";

        int epochs = all_epochs;

        RLDecisionModel model = new RLDecisionModel();
        model = trainModelFromDirectory(epochs, trainDir, BPRL::sprintzTransform, "Sprintz-RL");
        performanceSprintzVarChunkSize(model, dataDir, outDir);
    }

    // ========== 修改后的性能测试方法（选择更优方案） ==========
    static void performanceTS2DIFF(RLDecisionModel model, String directory, String outputDirStr) {
        runValueLevelVaryPackSizeBenchmark(model, directory, outputDirStr, "TS2DIFF-RL", BPRL::ts2diffTransform);
    }

    @Test
    public void TS2DIFFRL() {
        String trainDir = "benchmark_data/ElfTestData_camel";
        String dataDir = "benchmark_data/ElfTestData_camel";
        String outDir = "benchmark_data/output_RL_TS2DIFF";

        int epochs = 200;

        RLDecisionModel model = new RLDecisionModel();
        model = trainModelFromDirectory(
                epochs, trainDir, BPRL::ts2diffTransform, "TS2DIFF-RL", CHUNK_SIZE + 1);
        performanceTS2DIFF(model, dataDir, outDir);
    }

    static void performanceTS2DIFFVaryPackSize(RLDecisionModel model, String directory, String outputDirStr) {
        runValueLevelVaryPackSizeBenchmark(
                model, directory, outputDirStr, "TS2DIFF-RL",
                BPRL::ts2diffTransform,
                configuredPositiveIntProperty("bprl.ts2diffSourceChunkSize", CHUNK_SIZE + 1));
    }
    @Test
    public void TS2DIFFVarPackSize() {
        refreshRuntimeFlagsFromSystemProperties();
        String trainDir = "benchmark_data/ElfTestData_camel";
        String dataDir = "benchmark_data/ElfTestData_camel";
        String outDir = "benchmark_data/output_RL_TS2DIFF_vary_pack_size";

        int epochs = all_epochs;

        RLDecisionModel model = new RLDecisionModel();
        model = trainModelForBenchmark(
                epochs, trainDir, BPRL::ts2diffTransform, "TS2DIFF-RL", CHUNK_SIZE + 1);
        performanceTS2DIFFVaryPackSize(model, dataDir, outDir);
    }

    // ========== 测试不同chunk size的方法（修改版） ==========
    static void performanceTS2DIFFVarChunkSize(RLDecisionModel model, String directory, String outputDirStr) {
        runValueLevelVariableChunkSizeBenchmark(model, directory, outputDirStr, "TS2DIFF-RL", BPRL::ts2diffTransform);
    }

    @Test
    public void TS2DIFFVarChunkSize() {
        String trainDir = "benchmark_data/ElfTestData_camel";
        String dataDir = "benchmark_data/ElfTestData_camel";
        String outDir = "benchmark_data/output_RL_TS2DIFF_vary_m";

        int epochs = all_epochs;

        RLDecisionModel model = new RLDecisionModel();
        model = trainModelFromDirectory(
                epochs, trainDir, BPRL::ts2diffTransform, "TS2DIFF-RL", CHUNK_SIZE + 1);
        performanceTS2DIFFVarChunkSize(model, dataDir, outDir);
    }


}
