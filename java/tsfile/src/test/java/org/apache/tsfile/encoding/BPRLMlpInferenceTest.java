package org.apache.tsfile.encoding;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

public class BPRLMlpInferenceTest {

    @Test
    public void topKFromLogitsMatchesSoftmaxRanking() {
        Random random = new Random(11);
        float[] logits = new float[BPRL.MAX_PACK_LENGTH];
        float[] probs = new float[BPRL.MAX_PACK_LENGTH];
        for (int trial = 0; trial < 200; trial++) {
            int valid = 1 + random.nextInt(BPRL.MAX_PACK_LENGTH);
            for (int c = 0; c < valid; c++) {
                logits[c] = random.nextFloat() * 4.0f - 2.0f;
            }
            int topK = 1 + random.nextInt(5);
            BPRL.RLDecisionModel.maskedSoftmax(logits, valid, probs);
            int[] fromSoftmax = BPRL.selectTopKLengthCandidates(probs, valid, topK);
            int[] fromLogits = BPRL.selectTopKLengthCandidatesFromLogits(logits, valid, topK);
            Assert.assertArrayEquals(fromSoftmax, fromLogits);
        }
    }

    @Test
    public void lengthFeatureWindowCacheMatchesInto() {
        Random random = new Random(7);
        for (int t = 0; t < 50; t++) {
            int n = 32 + random.nextInt(512);
            int[] bw = new int[n];
            for (int i = 0; i < n; i++) {
                bw[i] = 1 + random.nextInt(32);
            }
            BPRL.LengthFeatureWindowCache cache = new BPRL.LengthFeatureWindowCache(bw, n);
            for (int trial = 0; trial < 20; trial++) {
                int start = random.nextInt(n);
                int packCount = random.nextInt(n);
                int globalMax = 1 + random.nextInt(64);
                float[] expected = new float[BPRL.LENGTH_INPUT_DIM];
                BPRL.lengthFeaturesInto(bw, start, n, packCount, globalMax, expected);
                float[] actual = new float[BPRL.LENGTH_INPUT_DIM];
                cache.fill(start, packCount, globalMax, actual);
                Assert.assertArrayEquals(expected, actual, 1e-6f);
            }
        }
    }

    @Test
    public void primitiveTopKScratchReuseMatchesFreshPlanner() {
        int savedCap = BPRL.RL_INFER_MAX_PACK_LENGTH;
        boolean savedPrefill = BPRL.RL_USE_CHUNK_MLP_PREFILL;
        try {
            BPRL.RL_INFER_MAX_PACK_LENGTH = 16;
            BPRL.RL_USE_CHUNK_MLP_PREFILL = true;
            BPRL.RLDecisionModel model = new BPRL.RLDecisionModel();
            BPRL.PrimitiveTopKScratch scratch = new BPRL.PrimitiveTopKScratch();

            long[][] sequences = {
                    {1, 2, 4, 8, 16, 32, 64, 128, 256},
                    {7, 7, 7, 1024, 3, 5, 2048, 9, 9, 9, 4096, 11},
                    new long[257]
            };
            for (int i = 0; i < sequences[2].length; i++) {
                sequences[2][i] = (i % 17 == 0) ? (1L << (i % 31)) : i * 3L + 1L;
            }

            for (long[] values : sequences) {
                int[] bitWidths = BPRL.computeValueBitWidths(values);
                BPRL.PrimitivePlanResult fresh =
                        BPRL.planPrimitiveTopKCost(bitWidths, model, 5);
                BPRL.PrimitivePlanResult reused =
                        BPRL.planPrimitiveTopKCost(bitWidths, model, 5, scratch);
                Assert.assertEquals(fresh.encodedBits, reused.encodedBits);
                Assert.assertEquals(fresh.packCount, reused.packCount);
            }
        } finally {
            BPRL.RL_INFER_MAX_PACK_LENGTH = savedCap;
            BPRL.RL_USE_CHUNK_MLP_PREFILL = savedPrefill;
        }
    }

    @Test
    public void configuredModelNameUsesSystemPropertyOverride() {
        String key = "bprl.modelName";
        String saved = System.getProperty(key);
        try {
            System.clearProperty(key);
            Assert.assertEquals("BP-RL", BPRL.configuredModelName("BP-RL"));
            System.setProperty(key, "BP-RL-h8");
            Assert.assertEquals("BP-RL-h8", BPRL.configuredModelName("BP-RL"));
        } finally {
            if (saved == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, saved);
            }
        }
    }

    @Test
    public void modelSeedPropertyMakesInitializationDeterministic() {
        String key = "bprl.modelSeed";
        String saved = System.getProperty(key);
        try {
            System.setProperty(key, "17");
            BPRL.RLDecisionModel first = new BPRL.RLDecisionModel();
            BPRL.RLDecisionModel second = new BPRL.RLDecisionModel();
            Assert.assertArrayEquals(first.lengthW1, second.lengthW1, 0.0f);
            Assert.assertArrayEquals(first.lengthOutW, second.lengthOutW, 0.0f);

            System.setProperty(key, "18");
            BPRL.RLDecisionModel third = new BPRL.RLDecisionModel();
            Assert.assertFalse(Arrays.equals(first.lengthW1, third.lengthW1));
        } finally {
            if (saved == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, saved);
            }
        }
    }

    @Test
    public void configuredPositiveIntPropertyFallsBackForMissingInvalidOrNonPositiveValues() {
        String key = "bprl.testPositiveInt";
        String saved = System.getProperty(key);
        try {
            System.clearProperty(key);
            Assert.assertEquals(1025, BPRL.configuredPositiveIntProperty(key, 1025));
            System.setProperty(key, "4097");
            Assert.assertEquals(4097, BPRL.configuredPositiveIntProperty(key, 1025));
            System.setProperty(key, "0");
            Assert.assertEquals(1025, BPRL.configuredPositiveIntProperty(key, 1025));
            System.setProperty(key, "-1");
            Assert.assertEquals(1025, BPRL.configuredPositiveIntProperty(key, 1025));
            System.setProperty(key, "not-a-number");
            Assert.assertEquals(1025, BPRL.configuredPositiveIntProperty(key, 1025));
        } finally {
            if (saved == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, saved);
            }
        }
    }

    @Test
    public void configuredPositiveIntListPropertyParsesValidListsAndFallsBack() {
        String key = "bprl.testPositiveIntList";
        String saved = System.getProperty(key);
        int[] defaults = {1, 2, 4};
        try {
            System.clearProperty(key);
            Assert.assertArrayEquals(defaults, BPRL.configuredPositiveIntListProperty(key, defaults));
            System.setProperty(key, "8, 16,32");
            Assert.assertArrayEquals(
                    new int[] {8, 16, 32},
                    BPRL.configuredPositiveIntListProperty(key, defaults));
            System.setProperty(key, "8,0,32");
            Assert.assertArrayEquals(defaults, BPRL.configuredPositiveIntListProperty(key, defaults));
            System.setProperty(key, "not-a-list");
            Assert.assertArrayEquals(defaults, BPRL.configuredPositiveIntListProperty(key, defaults));
        } finally {
            if (saved == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, saved);
            }
        }
    }

    @Test
    public void chunkPlanMemoReusesIdenticalPrimitiveTopKResult() {
        boolean savedMemo = BPRL.RL_USE_CHUNK_PLAN_MEMO;
        boolean savedTopK = BPRL.RL_USE_TOP_K_DECODE;
        boolean savedVanilla = BPRL.RL_USE_VANILLA_FALLBACK;
        boolean savedDegrade = BPRL.RL_USE_DEGRADE_FALLBACK;
        int savedCap = BPRL.RL_INFER_MAX_PACK_LENGTH;
        try {
            BPRL.RL_USE_CHUNK_PLAN_MEMO = true;
            BPRL.RL_USE_TOP_K_DECODE = true;
            BPRL.RL_USE_VANILLA_FALLBACK = false;
            BPRL.RL_USE_DEGRADE_FALLBACK = false;
            BPRL.RL_INFER_MAX_PACK_LENGTH = 16;
            BPRL.clearChunkPlanMemoForTest();

            long[] values = new long[128];
            for (int i = 0; i < values.length; i++) {
                values[i] = (i % 11 == 0) ? (1L << (i % 23)) : i * 17L + 3L;
            }
            BPRL.RLDecisionModel model = new BPRL.RLDecisionModel();
            BPRL.ChunkPlanWork first = BPRL.planAndEncodeChunk(values, model);
            Assert.assertEquals(1, BPRL.chunkPlanMemoSizeForTest());
            BPRL.ChunkPlanWork second = BPRL.planAndEncodeChunk(Arrays.copyOf(values, values.length), model);
            Assert.assertEquals(first.encodedBits, second.encodedBits);
            Assert.assertEquals(first.rlBetter, second.rlBetter);
            Assert.assertEquals(first.fixedBetter, second.fixedBetter);
            Assert.assertEquals(1, BPRL.chunkPlanMemoSizeForTest());
        } finally {
            BPRL.RL_USE_CHUNK_PLAN_MEMO = savedMemo;
            BPRL.RL_USE_TOP_K_DECODE = savedTopK;
            BPRL.RL_USE_VANILLA_FALLBACK = savedVanilla;
            BPRL.RL_USE_DEGRADE_FALLBACK = savedDegrade;
            BPRL.RL_INFER_MAX_PACK_LENGTH = savedCap;
            BPRL.clearChunkPlanMemoForTest();
        }
    }

    @Test
    public void chunkPlanMemoReusesFallbackOutcome() {
        boolean savedMemo = BPRL.RL_USE_CHUNK_PLAN_MEMO;
        boolean savedTopK = BPRL.RL_USE_TOP_K_DECODE;
        boolean savedVanilla = BPRL.RL_USE_VANILLA_FALLBACK;
        boolean savedDegrade = BPRL.RL_USE_DEGRADE_FALLBACK;
        int savedCap = BPRL.RL_INFER_MAX_PACK_LENGTH;
        try {
            BPRL.RL_USE_CHUNK_PLAN_MEMO = true;
            BPRL.RL_USE_TOP_K_DECODE = true;
            BPRL.RL_USE_VANILLA_FALLBACK = true;
            BPRL.RL_USE_DEGRADE_FALLBACK = false;
            BPRL.RL_INFER_MAX_PACK_LENGTH = 16;
            BPRL.clearChunkPlanMemoForTest();

            long[] values = new long[96];
            for (int i = 0; i < values.length; i++) {
                values[i] = (i % 7 == 0) ? (1L << (i % 19)) : i * 5L + 1L;
            }
            BPRL.RLDecisionModel model = new BPRL.RLDecisionModel();
            BPRL.ChunkPlanWork first = BPRL.planAndEncodeChunk(values, model);
            Assert.assertEquals(1, BPRL.chunkPlanMemoSizeForTest());
            BPRL.ChunkPlanWork second = BPRL.planAndEncodeChunk(Arrays.copyOf(values, values.length), model);
            Assert.assertEquals(first.encodedBits, second.encodedBits);
            Assert.assertEquals(first.rlBetter, second.rlBetter);
            Assert.assertEquals(first.fixedBetter, second.fixedBetter);
            Assert.assertEquals(1, BPRL.chunkPlanMemoSizeForTest());
        } finally {
            BPRL.RL_USE_CHUNK_PLAN_MEMO = savedMemo;
            BPRL.RL_USE_TOP_K_DECODE = savedTopK;
            BPRL.RL_USE_VANILLA_FALLBACK = savedVanilla;
            BPRL.RL_USE_DEGRADE_FALLBACK = savedDegrade;
            BPRL.RL_INFER_MAX_PACK_LENGTH = savedCap;
            BPRL.clearChunkPlanMemoForTest();
        }
    }

    @Test
    public void repeatWorkMemoCopiesCompletedChunkResultWithoutTiming() {
        BPRL.ChunkPlanWork completed = new BPRL.ChunkPlanWork();
        completed.toPack = new long[] {3L, 5L, 8L, 13L};
        completed.bitWidths = new int[] {2, 3, 4, 4};
        completed.planNs = 123L;
        completed.encodeNs = 456L;
        completed.encodedBits = 789L;
        completed.rlBetter = 1;
        completed.fixedBetter = 0;

        BPRL.ChunkPlanWork copy = BPRL.copyCompletedChunkPlanWork(completed);

        Assert.assertSame(completed.toPack, copy.toPack);
        Assert.assertSame(completed.bitWidths, copy.bitWidths);
        Assert.assertEquals(0L, copy.planNs);
        Assert.assertEquals(0L, copy.encodeNs);
        Assert.assertEquals(completed.encodedBits, copy.encodedBits);
        Assert.assertEquals(completed.rlBetter, copy.rlBetter);
        Assert.assertEquals(completed.fixedBetter, copy.fixedBetter);
    }

    @Test
    public void fallbackCostOnlyUsesEstimatedBitsWithoutEncodingBytes() {
        boolean savedCostOnly = BPRL.RL_FALLBACK_COST_ONLY;
        boolean savedVanilla = BPRL.RL_USE_VANILLA_FALLBACK;
        boolean savedDegrade = BPRL.RL_USE_DEGRADE_FALLBACK;
        try {
            BPRL.RL_FALLBACK_COST_ONLY = true;
            BPRL.RL_USE_VANILLA_FALLBACK = true;
            BPRL.RL_USE_DEGRADE_FALLBACK = false;

            long[] values = new long[] {1L, 1L, 2L, 2L, 3L, 3L, 4L, 4L};
            BPRL.Pack pack = new BPRL.Pack();
            for (int i = 0; i < values.length; i++) {
                pack.addValue(i, 64);
            }
            BPRL.PackingResult rl = new BPRL.PackingResult();
            rl.packs = Arrays.asList(pack);
            rl.packCount = 1;
            rl.totalCost = 4096L;
            rl.compressedData = new byte[512];

            BPRL.ChunkCompressionOutcome outcome = BPRL.selectRlOrVanillaBest(values, rl);

            Assert.assertTrue(outcome.usedVanillaFallback);
            Assert.assertNull(outcome.compressedData);
            Assert.assertTrue(outcome.encodedBits > 0);
            Assert.assertTrue(outcome.encodedBits < rl.totalCost);
        } finally {
            BPRL.RL_FALLBACK_COST_ONLY = savedCostOnly;
            BPRL.RL_USE_VANILLA_FALLBACK = savedVanilla;
            BPRL.RL_USE_DEGRADE_FALLBACK = savedDegrade;
        }
    }

    @Test
    public void packValuesFastMaterializesRoundTrippableBytes() {
        BPRL.RLDecisionModel model = new BPRL.RLDecisionModel();
        model.explorationRate = 0.0f;
        long[] values = new long[] {3, 7, 7, 8, 13, 21, 34, 55, 89, 144};

        BPRL.PackingResult result = BPRL.packValuesFast(
                values, BPRL.computeValueBitWidths(values), model);

        Assert.assertNotNull(result.compressedData);
        Assert.assertEquals(result.compressedData.length * 8L, result.totalCost);
        Assert.assertArrayEquals(values, BPRL.decompressPackingResult(result, values.length));
    }

    @Test
    public void fixedFallbackOnlyChunkPathMaterializesRoundTrippableBytes() {
        boolean savedFixedOnly = BPRL.RL_FIXED_FALLBACK_ONLY;
        boolean savedVanilla = BPRL.RL_USE_VANILLA_FALLBACK;
        boolean savedDegrade = BPRL.RL_USE_DEGRADE_FALLBACK;
        boolean savedCostOnly = BPRL.RL_FALLBACK_COST_ONLY;
        try {
            BPRL.RL_FIXED_FALLBACK_ONLY = true;
            BPRL.RL_USE_VANILLA_FALLBACK = true;
            BPRL.RL_USE_DEGRADE_FALLBACK = true;
            BPRL.RL_FALLBACK_COST_ONLY = false;

            BPRL.RLDecisionModel model = new BPRL.RLDecisionModel();
            long[] values = new long[] {11, 12, 12, 12, 40, 41, 43, 44, 99, 100, 101, 102};
            BPRL.ChunkPlanWork work = BPRL.planAndEncodeChunk(values, model);

            Assert.assertNotNull(work.compressedData);
            Assert.assertTrue(work.encodedBits > 0);
            Assert.assertArrayEquals(values, BPRL.decompressChunkPlanWork(work, values.length));
        } finally {
            BPRL.RL_FIXED_FALLBACK_ONLY = savedFixedOnly;
            BPRL.RL_USE_VANILLA_FALLBACK = savedVanilla;
            BPRL.RL_USE_DEGRADE_FALLBACK = savedDegrade;
            BPRL.RL_FALLBACK_COST_ONLY = savedCostOnly;
        }
    }

    @Test
    public void ts2diffDecodeRestoresOriginalValues() {
        long[] values = new long[] {1000, 997, 1003, 1003, 1019, 995, 1001};

        BPRL.TSDIFFEncodedResult encoded = BPRL.ts2diff(values);

        Assert.assertArrayEquals(
                values,
                BPRL.ts2diffDecode(
                        encoded.getEncodedData(), encoded.getFirstValue(), encoded.getMinDiff()));
    }

    @Test
    public void ts2diffDatasetTunedConfigReturnsKnownSafeProfiles() {
        BPRL.RLRuntimeConfig cs = BPRL.ts2diffDatasetTunedConfig("CS-Sensors.csv");
        Assert.assertEquals(32, cs.inferMaxPackLength);
        Assert.assertEquals(5, cs.topK);
        Assert.assertEquals(1.0f, cs.confidenceGateMargin, 0.0f);
        Assert.assertTrue(cs.useVanillaFallback);
        Assert.assertTrue(cs.useDegradeFallback);

        BPRL.RLRuntimeConfig pm10 = BPRL.ts2diffDatasetTunedConfig("PM10-dust.csv");
        Assert.assertEquals(192, pm10.inferMaxPackLength);
        Assert.assertEquals(10, pm10.topK);
        Assert.assertTrue(Float.isNaN(pm10.confidenceGateMargin));
        Assert.assertFalse(pm10.useVanillaFallback);
        Assert.assertFalse(pm10.useDegradeFallback);

        BPRL.RLRuntimeConfig transport = BPRL.ts2diffDatasetTunedConfig("TY-Transport.csv");
        Assert.assertEquals(192, transport.inferMaxPackLength);
        Assert.assertEquals(16, transport.topK);
        Assert.assertTrue(Float.isNaN(transport.confidenceGateMargin));
        Assert.assertTrue(transport.useVanillaFallback);
        Assert.assertFalse(transport.useDegradeFallback);
    }

    @Test
    public void includeBenchmarkDatasetFileCanFilterBenchmarkWithoutChangingWhitelist() {
        String key = "bprl.benchmarkDatasets";
        String saved = System.getProperty(key);
        try {
            System.clearProperty(key);
            Assert.assertTrue(BPRL.includeBenchmarkDatasetFile("CS-Sensors.csv"));
            Assert.assertFalse(BPRL.includeBenchmarkDatasetFile("City-temp.csv"));

            System.setProperty(key, "CS-Sensors.csv,PM10-dust.csv");
            Assert.assertTrue(BPRL.includeBenchmarkDatasetFile("CS-Sensors.csv"));
            Assert.assertTrue(BPRL.includeBenchmarkDatasetFile("PM10-dust.csv"));
            Assert.assertFalse(BPRL.includeBenchmarkDatasetFile("TY-Transport.csv"));
            Assert.assertFalse(BPRL.includeBenchmarkDatasetFile("City-temp.csv"));
        } finally {
            if (saved == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, saved);
            }
        }
    }

    @Test
    public void lengthFeaturesIntoMatchesLegacy() {
        Random random = new Random(3);
        for (int t = 0; t < 100; t++) {
            int n = 16 + random.nextInt(200);
            int[] bw = new int[n];
            for (int i = 0; i < n; i++) {
                bw[i] = 1 + random.nextInt(32);
            }
            int start = random.nextInt(n);
            int packCount = random.nextInt(n);
            int globalMax = 1 + random.nextInt(64);
            float[] legacy = legacyLengthFeatures(bw, start, n, packCount, globalMax);
            float[] fast = new float[BPRL.LENGTH_INPUT_DIM];
            BPRL.lengthFeaturesInto(bw, start, n, packCount, globalMax, fast);
            Assert.assertArrayEquals(legacy, fast, 1e-6f);
        }
    }

    private static float[] legacyLengthFeatures(
            int[] bitWidths, int startIndex, int numValues, int packCount, int globalMaxPackSize) {
        float[] feat = new float[BPRL.LENGTH_INPUT_DIM];
        int n = Math.max(1, numValues);
        int remaining = Math.max(1, numValues - startIndex);
        feat[0] = startIndex / (float) n;
        feat[1] = remaining / (float) n;
        feat[2] = packCount / (float) n;
        feat[3] = globalMaxPackSize / (float) n;
        int[] windows = {1, 2, 4, 8, 16, 32, 64, 128};
        int f = 4;
        for (int window : windows) {
            int end = Math.min(numValues, startIndex + window);
            int max = 0;
            int sum = 0;
            int count = 0;
            for (int i = startIndex; i < end; i++) {
                int b = bitWidths[i];
                if (b > max) {
                    max = b;
                }
                sum += b;
                count++;
            }
            float avg = count > 0 ? (sum / (float) count) : 0.0f;
            feat[f++] = max / 64.0f;
            feat[f++] = avg / 64.0f;
        }
        feat[f] = bitWidths[startIndex] / 64.0f;
        return feat;
    }
}
