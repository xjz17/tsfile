package org.apache.tsfile.encoding;

import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class BPRLCostConsistencyTest {

    @Test
    public void rlEstimatedCostMatchesActualEncodedBits() {
        long[] values = {1, 2, 7, 3, 15, 9, 2, 1};
        int[] bitWidths = BPRL.computeValueBitWidths(values);

        BPRL.RLDecisionModel model = new BPRL.RLDecisionModel();
        model.explorationRate = 0.0f;

        BPRL.PackingResult result = BPRL.packValues(values, bitWidths, model, null);

        Assert.assertNotNull(result.compressedData);
        Assert.assertEquals(result.compressedData.length * 8L, result.totalCost);
        Assert.assertTrue(result.paperCost > 0);

        long[] decoded = BPRL.decompressPackingResult(result, values.length);
        Assert.assertArrayEquals(values, decoded);
    }

    @Test
    public void rlPaperCostMatchesMainTexDefinition() {
        long[] values = {1, 2, 7, 3, 15, 9, 2, 1};
        int[] bitWidths = BPRL.computeValueBitWidths(values);

        BPRL.RLDecisionModel model = new BPRL.RLDecisionModel();
        model.explorationRate = 0.0f;

        BPRL.PackingResult result = BPRL.packValues(values, bitWidths, model, null);
        BPDP.PackingPlan plan = BPRL.toBPDPPackingPlan(result.packs);

        Assert.assertEquals(
                BPDP.computeBitstreamBitCost(plan, BPRL.VALUE_PACK_SIZE), result.paperCost);
        Assert.assertEquals(
                BPDP.computeEncodedBitCostForPlan(values, plan, BPRL.VALUE_PACK_SIZE), result.totalCost);
    }

    @Test
    public void rlDpPlanMatchesBPDPValueLevelEncodedCost() {
        long[] values = {1, 2, 7, 3, 15, 9, 2, 1};
        int[] bitWidths = BPRL.computeValueBitWidths(values);
        BPDP.PackingPlan dpPlan = BPDP.computeOptimalPackingPlan(bitWidths, BPRL.VALUE_PACK_SIZE);

        int dpEncoded = BPDP.computeEncodedBitCostForPlan(values, dpPlan, BPRL.VALUE_PACK_SIZE);
        BPRL.PackingResult fromDp = BPRL.packFromDpPlan(values, bitWidths, dpPlan);

        Assert.assertEquals(dpEncoded, fromDp.totalCost);
        Assert.assertEquals(
                BPDP.computeBitstreamBitCost(dpPlan, BPRL.VALUE_PACK_SIZE), fromDp.paperCost);
        Assert.assertEquals(
                BPDP.computeOptimalEncodedBitCost(values, bitWidths, BPRL.VALUE_PACK_SIZE), dpEncoded);
    }

    @Test
    public void optimalDpCostUsesSameEncodingAsBPDP() {
        long[] values = {1, 2, 7, 3, 15, 9, 2, 1};
        int[] bitWidths = BPRL.computeValueBitWidths(values);

        int[] encodePos = new int[1];
        BPDP.compressWithOptimalPacking(values, bitWidths, BPRL.VALUE_PACK_SIZE, encodePos);
        int fromCompress = encodePos[0] * 8;
        int fromHelper = BPDP.computeOptimalEncodedBitCost(values, bitWidths, BPRL.VALUE_PACK_SIZE);
        long fromCostOnly = BPRL.computeOptimalEncodedCost(values, bitWidths);

        Assert.assertEquals(fromCompress, fromHelper);
        Assert.assertEquals(fromCompress, fromCostOnly);
    }

    @Test
    public void dpValueLevelRoundtripOnSyntheticChunk() {
        Random random = new Random(42);
        long[] values = new long[1024];
        for (int i = 0; i < values.length; i++) {
            values[i] = random.nextInt(100000);
        }
        int[] bitWidths = BPRL.computeValueBitWidths(values);
        int[] encodePos = new int[1];
        byte[] encoded = BPDP.compressWithOptimalPacking(values, bitWidths, BPRL.VALUE_PACK_SIZE, encodePos);
        long[] decoded = BPDP.decompressWithOptimalPacking(encoded, values.length, BPRL.VALUE_PACK_SIZE);
        Assert.assertArrayEquals(values, decoded);
    }

    @Test
    public void rlValueLevelRoundtripOnSyntheticChunk() {
        Random random = new Random(42);
        long[] values = new long[1024];
        for (int i = 0; i < values.length; i++) {
            values[i] = random.nextInt(100000);
        }
        BPRL.RLDecisionModel model = new BPRL.RLDecisionModel();
        model.explorationRate = 0.0f;
        BPRL.PackingResult result = BPRL.compressValuesWithRL(values, model);
        Assert.assertNotNull(result.compressedData);
        long[] decoded = BPRL.decompressPackingResult(result, values.length);
        Assert.assertArrayEquals(values, decoded);
    }

    @Test
    public void singleValue64BitRoundtrip() {
        long[] values = {Long.MAX_VALUE};
        int[] bitWidths = {64};
        int[] encodePos = new int[1];
        byte[] encoded = BPDP.compressWithOptimalPacking(values, bitWidths, BPRL.VALUE_PACK_SIZE, encodePos);
        long[] decoded = BPDP.decompressWithOptimalPacking(encoded, 1, BPRL.VALUE_PACK_SIZE);
        Assert.assertArrayEquals(values, decoded);
    }

    @Test
    public void inspectUsgsDpPlan() throws Exception {
        long[] values = loadUsgsChunk1024();
        int[] bitWidths = BPRL.computeValueBitWidths(values);
        BPDP.PackingPlan plan = BPDP.computeOptimalPackingPlan(bitWidths, BPRL.VALUE_PACK_SIZE);
        int sum = 0;
        for (int groupSize : plan.groupSizes) {
            sum += groupSize;
        }
        System.out.println("groups=" + plan.groupSizes.length + " sum=" + sum + " optimalC=" + plan.optimalC);
        if (plan.groupSizes.length > 13) {
            System.out.println("g13 size=" + plan.groupSizes[13] + " bw=" + plan.groupBitWidths[13]);
        }
        Assert.assertEquals(values.length, sum);
    }

    @Test
    public void dpValueLevelRoundtripOnUsgsChunk() throws Exception {
        long[] values = loadUsgsChunk1024();
        int[] bitWidths = BPRL.computeValueBitWidths(values);
        int[] encodePos = new int[1];
        byte[] encoded = BPDP.compressWithOptimalPacking(values, bitWidths, BPRL.VALUE_PACK_SIZE, encodePos);
        long[] decoded = BPDP.decompressWithOptimalPacking(encoded, values.length, BPRL.VALUE_PACK_SIZE);
        Assert.assertArrayEquals(values, decoded);
    }

    @Test
    public void rlValueLevelRoundtripOnUsgsChunk() throws Exception {
        long[] values = loadUsgsChunk1024();
        BPRL.RLDecisionModel model = new BPRL.RLDecisionModel();
        model.explorationRate = 0.0f;
        BPRL.PackingResult result = BPRL.compressValuesWithRL(values, model);
        long[] decoded = BPRL.decompressPackingResult(result, values.length);
        Assert.assertArrayEquals(values, decoded);
    }

    private static long[] loadUsgsChunk1024() throws Exception {
        List<String> nums = new ArrayList<>();
        try (BufferedReader br = Files.newBufferedReader(Paths.get(
                "benchmark_data/ElfTestData_camel/USGS-Earthquakes.csv"))) {
            String line;
            while ((line = br.readLine()) != null && nums.size() < 1024) {
                for (String token : line.split(",")) {
                    String t = token.trim().replace("\"", "");
                    if (!t.isEmpty()) {
                        nums.add(t);
                        if (nums.size() >= 1024) {
                            break;
                        }
                    }
                }
            }
        }
        return BPRL.scaleNumbersForTest(nums, 0);
    }

    @Test
    public void vanillaFallbackRoundtripWhenVanillaBeatsRl() {
        long[] values = new long[256];
        for (int i = 0; i < values.length; i++) {
            values[i] = i % 17;
        }
        BPRL.RLDecisionModel model = new BPRL.RLDecisionModel();
        model.explorationRate = 0.0f;
        BPRL.PackingResult result = BPRL.compressValuesWithRL(values, model);
        Assert.assertNotNull(result.compressedData);
        long[] decoded = BPRL.decompressPackingResult(result, values.length);
        Assert.assertArrayEquals(values, decoded);
        Assert.assertTrue(
                "hybrid encoded cost should be <= pure RL",
                result.totalCost <= BPRL.packValuesFast(
                        values, BPRL.computeValueBitWidths(values), model).totalCost);
    }

    @Test
    public void fixedPackCostMatchesActualEncodedBits() {
        long[] values = {1, 2, 7, 3, 15, 9, 2, 1, 31, 8};

        BPRL.FixedPackResult result = BPRL.calculateFixedPackCost(values, values.length);

        Assert.assertNotNull(result.compressedData);
        Assert.assertEquals(result.compressedData.length * 8L, result.totalCost);

        long[] decoded = BPRL.decompressFixedPack(result.compressedData, values.length);
        Assert.assertArrayEquals(values, decoded);
    }

    @Test
    public void exactOracleDominatesRlHybridFallbackChoices() {
        long[] values = new long[256];
        for (int i = 0; i < values.length; i++) {
            values[i] = (i % 32 == 0) ? 4095 : (i % 7);
        }

        BPRL.RLDecisionModel model = new BPRL.RLDecisionModel();
        model.explorationRate = 0.0f;
        BPRL.PackingResult rlHybrid = BPRL.compressValuesWithRL(values, model);
        BPDP.ExactOracleCompressionResult exactOracle = BPDP.compressWithBestExactPacking(values);

        Assert.assertTrue("RL hybrid must produce compressed output", rlHybrid.totalCost > 0);
        Assert.assertTrue("exact DP oracle should cover RL fallback choices",
                exactOracle.encodedBits <= rlHybrid.totalCost);
    }
}
