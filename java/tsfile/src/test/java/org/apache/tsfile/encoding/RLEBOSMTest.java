package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZOutputFile;
import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.junit.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;

import static java.lang.Math.*;
import static org.apache.iotdb.tsfile.constant.TestConstant.random;

public class RLEBOSMTest {

    public static int findMedianImprove(int[] arr) {
        if (arr == null || arr.length == 0) {
            throw new IllegalArgumentException("数组不能为空");
        }
        int n = arr.length;
        return quickSelect2(arr, 0, n - 1, n / 2);
    }

    private static int quickSelect2(int[] arr, int left, int right, int k){
        int pivotV=arr[left+random.nextInt(right-left+1)],tmpV;
        int posEqual=left,posSmaller=left; // a[left,posEqual): = pivotV; a[posEqual,posSmaller): < pivotV
        for(int i=left;i<=right;i++){
            if(arr[i]==pivotV){
                tmpV=arr[i];
                arr[i]=arr[posSmaller];
                arr[posSmaller]=arr[posEqual];
                arr[posEqual]=tmpV;
                posEqual++;
                posSmaller++;
            }else
            if(arr[i]<pivotV){
                tmpV=arr[posSmaller];
                arr[posSmaller]=arr[i];
                arr[i]=tmpV;
                posSmaller++;
            }
        }
        if(k+(posEqual-left)<=posSmaller-1)return quickSelect2(arr,posEqual,posSmaller-1,k+(posEqual-left));
        else if(k<=posSmaller-1)return pivotV;
        else return quickSelect2(arr,posSmaller,right,k);
    }
    public static int findMedian(int[] arr) {
        if (arr == null || arr.length == 0) {
            throw new IllegalArgumentException("数组不能为空");
        }
        int n = arr.length;
        return quickSelect(arr, 0, n - 1, n / 2);
    }

    private static int quickSelect(int[] arr, int left, int right, int k) {
        if (left == right) {
            return arr[left];
        }

        int pivotIndex = partition(arr, left, right);
        if (k == pivotIndex) {
            return arr[k];
        } else if (k < pivotIndex) {
            return quickSelect(arr, left, pivotIndex - 1, k);
        } else {
            return quickSelect(arr, pivotIndex + 1, right, k);
        }
    }

    private static int partition(int[] arr, int left, int right) {
        int pivot = arr[right];
        int i = left;
        for (int j = left; j < right; j++) {
            if (arr[j] <= pivot) {
                swap(arr, i, j);
                i++;
            }
        }
        swap(arr, i, right);
        return i;
    }

    private static void swap(int[] arr, int i, int j) {
        int temp = arr[i];
        arr[i] = arr[j];
        arr[j] = temp;
    }

    public static int getBitWith(int num) {
        if (num == 0) return 1;
        else return 32 - Integer.numberOfLeadingZeros(num);
    }
    public static int getCount(long long1, int mask) {
        return ((int) (long1 & mask));
    }
    public static int getUniqueValue(long long1, int left_shift) {
        return ((int) ((long1) >> left_shift));
    }


    public static void int2Bytes(int integer,int encode_pos , byte[] cur_byte) {
        cur_byte[encode_pos] = (byte) (integer >> 24);
        cur_byte[encode_pos+1] = (byte) (integer >> 16);
        cur_byte[encode_pos+2] = (byte) (integer >> 8);
        cur_byte[encode_pos+3] = (byte) (integer);
    }

    public static void intByte2Bytes(int integer, int encode_pos , byte[] cur_byte) {
        cur_byte[encode_pos] = (byte) (integer);
    }

    private static void long2intBytes(long integer, int encode_pos , byte[] cur_byte) {
        cur_byte[encode_pos] = (byte) (integer >> 24);
        cur_byte[encode_pos+1] = (byte) (integer >> 16);
        cur_byte[encode_pos+2] = (byte) (integer >> 8);
        cur_byte[encode_pos+3] = (byte) (integer);
    }


    public static int bytes2Integer(byte[] encoded, int start, int num) {
        int value = 0;
        if (num > 4) {
            System.out.println("bytes2Integer error");
            return 0;
        }
        for (int i = 0; i < num; i++) {
            value <<= 8;
            int b = encoded[i + start] & 0xFF;
            value |= b;
        }
        return value;
    }

    private static long bytesLong2Integer(byte[] encoded, int decode_pos) {
        long value = 0;
        for (int i = 0; i < 4; i++) {
            value <<= 8;
            int b = encoded[i + decode_pos] & 0xFF;
            value |= b;
        }
        return value;
    }

    public static void pack8Values(ArrayList<Integer> values, int offset, int width, int encode_pos,  byte[] encoded_result) {
        int bufIdx = 0;
        int valueIdx = offset;
        // remaining bits for the current unfinished Integer
        int leftBit = 0;

        while (valueIdx < 8 + offset) {
            // buffer is used for saving 32 bits as a part of result
            int buffer = 0;
            // remaining size of bits in the 'buffer'
            int leftSize = 32;

            // encode the left bits of current Integer to 'buffer'
            if (leftBit > 0) {
                buffer |= (values.get(valueIdx) << (32 - leftBit));
                leftSize -= leftBit;
                leftBit = 0;
                valueIdx++;
            }

            while (leftSize >= width && valueIdx < 8 + offset) {
                // encode one Integer to the 'buffer'
                buffer |= (values.get(valueIdx)<< (leftSize - width));
                leftSize -= width;
                valueIdx++;
            }
            // If the remaining space of the buffer can not save the bits for one Integer,
            if (leftSize > 0 && valueIdx < 8 + offset) {
                // put the first 'leftSize' bits of the Integer into remaining space of the
                // buffer
                buffer |= (values.get(valueIdx) >>> (width - leftSize));
                leftBit = width - leftSize;
            }

            // put the buffer into the final result
            for (int j = 0; j < 4; j++) {
                encoded_result[encode_pos] = (byte) ((buffer >>> ((3 - j) * 8)) & 0xFF);
                encode_pos ++;
                bufIdx++;
                if (bufIdx >= width) {
                    return ;
                }
            }
        }

    }

    public static void unpack8Values(byte[] encoded, int offset,int width,  ArrayList<Integer> result_list) {
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
                result_list.add ((int) (buffer >>> (totalBits - width)));
                valueIdx++;
                totalBits -= width;
                buffer = buffer & ((1L << totalBits) - 1);
            }
        }
    }

    public static int bitPacking(ArrayList<Integer> numbers, int start, int bit_width,int encode_pos,  byte[] encoded_result) {
        int block_num = (numbers.size()-start) / 8;
        for(int i=0;i<block_num;i++){
            pack8Values( numbers, start+i*8, bit_width,encode_pos, encoded_result);
            encode_pos +=bit_width;
        }

        return encode_pos;

    }

    public static ArrayList<Integer> decodeBitPacking(
            byte[] encoded, int decode_pos, int bit_width, int block_size) {
        ArrayList<Integer> result_list = new ArrayList<>();
        int block_num = (block_size - 1) / 8;

        for (int i = 0; i < block_num; i++) { // bitpacking
            unpack8Values( encoded, decode_pos, bit_width,  result_list);
            decode_pos += bit_width;
        }
        return result_list;
    }


    public static int[]  getAbsDeltaTsBlock(
            int[] ts_block,
            int i,
            int block_size,
            int remaining,
            int[] min_delta,
            ArrayList<Integer> repeat_count) {
        int[] ts_block_delta = new int[remaining];

        int value_delta_min = Integer.MAX_VALUE;
        int value_delta_max = Integer.MIN_VALUE;
        int base = i*block_size;
        int end = i*block_size+remaining;
        for (int j = base; j < end; j++) {

            int integer = ts_block[j];
            if (integer < value_delta_min) value_delta_min = integer;
            if (integer > value_delta_max) {
                value_delta_max = integer;
            }
        }
        int pre_delta = ts_block[i*block_size]-value_delta_min;
        int pre_count = 1;

        min_delta[0]=(value_delta_min);
        int repeat_i = 0;
        int ts_block_delta_i = 0;
        for (int j = base+1; j < end; j++) {
            int delta = ts_block[j]-value_delta_min;
            if(delta == pre_delta){
                pre_count ++;
            } else {
                if(pre_count>7){
                    repeat_count.add(repeat_i);
                    repeat_count.add(pre_count);
                    ts_block_delta[ts_block_delta_i]=pre_delta;
                    ts_block_delta_i ++;
                } else{
                    for (int k = 0; k < pre_count; k++){
                        ts_block_delta[ts_block_delta_i] =pre_delta;
                        ts_block_delta_i++;
                    }
                }
                pre_count =1;
                repeat_i = j - i*block_size;
            }
            pre_delta = delta;

        }
        for (int j = 0; j < pre_count; j++){
            ts_block_delta[ts_block_delta_i] =pre_delta;
            ts_block_delta_i++;
        }
        min_delta[1]=(ts_block_delta_i);
        min_delta[2]=(value_delta_max-value_delta_min);
        int[] new_ts_block_delta = new int[ts_block_delta_i];
        System.arraycopy(ts_block_delta, 0, new_ts_block_delta, 0, ts_block_delta_i);

        return new_ts_block_delta;
    }



    public static int encodeOutlier2Bytes(
            ArrayList<Integer> ts_block_delta,
            int bit_width,
            int encode_pos,  byte[] encoded_result) {

        encode_pos = bitPacking(ts_block_delta, 0, bit_width, encode_pos, encoded_result);

        int n_k = ts_block_delta.size();
        int n_k_b = n_k / 8;
        long cur_remaining = 0; // encoded int
        int cur_number_bits = 0; // the bit width used of encoded int
        for (int i = n_k_b * 8; i < n_k; i++) {
            long cur_value = ts_block_delta.get(i);
            int cur_bit_width = bit_width; // remaining bit width of current value

            if (cur_number_bits + bit_width >= 32) {
                cur_remaining <<= (32 - cur_number_bits);
                cur_bit_width = bit_width - 32 + cur_number_bits;
                cur_remaining += ((cur_value >> cur_bit_width));
                long2intBytes(cur_remaining,encode_pos,encoded_result);
                encode_pos += 4;

                cur_remaining = 0;
                cur_number_bits = 0;
            }

            cur_remaining <<= cur_bit_width;
            cur_number_bits += cur_bit_width;
            cur_remaining += (((cur_value << (32 - cur_bit_width)) & 0xFFFFFFFFL) >> (32 - cur_bit_width));
        }
        cur_remaining <<= (32 - cur_number_bits);
        long2intBytes(cur_remaining,encode_pos,encoded_result);
        encode_pos += 4;
        return encode_pos;

    }


    public static ArrayList<Integer> decodeOutlier2Bytes(
            byte[] encoded,
            int decode_pos,
            int bit_width,
            int length,
            ArrayList<Integer> encoded_pos_result
    ) {

        int n_k_b = length / 8;
        int remaining = length - n_k_b * 8;
        ArrayList<Integer> result_list = new ArrayList<>(decodeBitPacking(encoded, decode_pos, bit_width, n_k_b * 8 + 1));
        decode_pos += n_k_b * bit_width;

        ArrayList<Long> int_remaining = new ArrayList<>();
        int int_remaining_size = remaining * bit_width / 32 + 1;
        for (int j = 0; j < int_remaining_size; j++) {

            int_remaining.add(bytesLong2Integer(encoded, decode_pos));
            decode_pos += 4;
        }

        int cur_remaining_bits = 32; // remaining bit width of current value
        long cur_number = int_remaining.get(0);
        int cur_number_i = 1;
        for (int i = n_k_b * 8; i < length; i++) {
            if (bit_width < cur_remaining_bits) {
                int tmp = (int) (cur_number >> (32 - bit_width));
                result_list.add(tmp);
                cur_number <<= bit_width;
                cur_number &= 0xFFFFFFFFL;
                cur_remaining_bits -= bit_width;
            } else {
                int tmp = (int) (cur_number >> (32 - cur_remaining_bits));
                int remain_bits = bit_width - cur_remaining_bits;
                tmp <<= remain_bits;

                cur_number = int_remaining.get(cur_number_i);
                cur_number_i++;
                tmp += (cur_number >> (32 - remain_bits));
                result_list.add(tmp);
                cur_number <<= remain_bits;
                cur_number &= 0xFFFFFFFFL;
                cur_remaining_bits = 32 - remain_bits;
            }
        }
        encoded_pos_result.add(decode_pos);
        return result_list;
    }

    private static void addToArchiveCompression(SevenZOutputFile out, File file, String dir) {
        String name = dir + File.separator + file.getName();
        if(dir.equals(".")) {
            name = file.getName();
        }
        if (file.isFile()){
            SevenZArchiveEntry entry = null;
            FileInputStream in = null;
            try {
                entry = out.createArchiveEntry(file, name);
                out.putArchiveEntry(entry);
                in = new FileInputStream(file);
                byte[] b = new byte[1024];
                int count = 0;
                while ((count = in.read(b)) > 0) {
                    out.write(b, 0, count);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    out.closeArchiveEntry();
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        } else if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null){
                for (File child : children){
                    addToArchiveCompression(out, child, name);
                }
            }
        } else {
            System.out.println(file.getName() + " is not supported");
        }
    }

    private static int BOSEncodeBits(int[] ts_block_delta,
                                      int init_block_size,
                                      int final_k_start_value,
                                     int final_x_l_plus,
                                      int final_k_end_value,
                                     int final_x_u_minus,
                                      int max_delta_value,
                                      int[] min_delta,
                                      ArrayList<Integer> repeat_count,
                                      int encode_pos,
                                      byte[] cur_byte) {
        int block_size = ts_block_delta.length;

        ArrayList<Integer> final_left_outlier_index = new ArrayList<>();
        ArrayList<Integer> final_right_outlier_index = new ArrayList<>();
        ArrayList<Integer> final_left_outlier = new ArrayList<>();
        ArrayList<Integer> final_right_outlier = new ArrayList<>();
        ArrayList<Integer> final_normal = new ArrayList<>();
        int k1 = 0;
        int k2 = 0;
        ArrayList<Integer> bitmap_outlier = new ArrayList<>();
        int index_bitmap_outlier = 0;
        int cur_index_bitmap_outlier_bits = 0;
        for (int i = 0; i < block_size; i++) {
            int cur_value = ts_block_delta[i];
            if (cur_value < final_k_start_value) {
                final_left_outlier.add(cur_value);
                final_left_outlier_index.add(i);
                if (cur_index_bitmap_outlier_bits % 8 != 7) {
                    index_bitmap_outlier <<= 2;
                    index_bitmap_outlier += 3;
                    cur_index_bitmap_outlier_bits += 2;
                } else {
                    index_bitmap_outlier <<= 1;
                    index_bitmap_outlier += 1;
                    bitmap_outlier.add(index_bitmap_outlier);
                    index_bitmap_outlier = 1;
                    cur_index_bitmap_outlier_bits = 1;
                }


                k1++;


            } else if (cur_value >= final_k_end_value) {
                final_right_outlier.add(cur_value - final_k_end_value);
                final_right_outlier_index.add(i);
                if (cur_index_bitmap_outlier_bits % 8 != 7) {
                    index_bitmap_outlier <<= 2;
                    index_bitmap_outlier += 2;
                    cur_index_bitmap_outlier_bits += 2;
                } else {
                    index_bitmap_outlier <<= 1;
                    index_bitmap_outlier += 1;
                    bitmap_outlier.add(index_bitmap_outlier);
                    index_bitmap_outlier = 0;
                    cur_index_bitmap_outlier_bits = 1;
                }
                k2++;

            } else {
                final_normal.add(cur_value - final_x_l_plus);
                index_bitmap_outlier <<= 1;
                cur_index_bitmap_outlier_bits += 1;
            }
            if (cur_index_bitmap_outlier_bits % 8 == 0) {
                bitmap_outlier.add(index_bitmap_outlier);
                index_bitmap_outlier = 0;
            }
        }
        if (cur_index_bitmap_outlier_bits % 8 != 0) {

            index_bitmap_outlier <<= (8 - cur_index_bitmap_outlier_bits % 8);

            index_bitmap_outlier &= 0xFF;
            bitmap_outlier.add(index_bitmap_outlier);
        }
        int final_alpha = ((k1 + k2) * getBitWith(block_size-1)) <= (block_size + k1 + k2) ? 1 : 0;

        int k_byte = (k1 << 1);
        k_byte += final_alpha;
        k_byte += (k2 << 16);

        int2Bytes(k_byte,encode_pos,cur_byte);
        encode_pos += 4;

        int2Bytes(min_delta[0],encode_pos,cur_byte);
        encode_pos += 4;
        int size = repeat_count.size();
        intByte2Bytes(size,encode_pos,cur_byte);
        encode_pos += 1;

        if (size != 0)
            encode_pos =encodeOutlier2Bytes(repeat_count, getBitWith(init_block_size-1),encode_pos,cur_byte);


        int bit_width_final = 0;
        int left_bit_width = getBitWith(final_k_start_value);//final_left_max
        int right_bit_width = getBitWith(max_delta_value - final_k_end_value);//final_right_min

        if(k1==0 && k2==0){
            bit_width_final = getBitWith(max_delta_value);
            intByte2Bytes(bit_width_final,encode_pos,cur_byte);
            encode_pos += 1;

//            encode_pos = encodeOutlier2Bytes(final_normal, bit_width_final,encode_pos,cur_byte);
//            return encode_pos;
        }
        else{
            int2Bytes(final_x_l_plus,encode_pos,cur_byte);
            encode_pos += 4;
            int2Bytes(final_k_end_value,encode_pos,cur_byte);
            encode_pos += 4;

            bit_width_final = getBitWith(final_x_u_minus - final_x_l_plus);
            intByte2Bytes(bit_width_final,encode_pos,cur_byte);
            encode_pos += 1;
            intByte2Bytes(left_bit_width,encode_pos,cur_byte);
            encode_pos += 1;
            intByte2Bytes(right_bit_width,encode_pos,cur_byte);
            encode_pos += 1;
            if (final_alpha == 0) { // 0

                for (int i : bitmap_outlier) {

                    intByte2Bytes(i,encode_pos,cur_byte);
                    encode_pos += 1;
                }
            } else {
                encode_pos = encodeOutlier2Bytes(final_left_outlier_index, getBitWith(block_size-1),encode_pos,cur_byte);
                encode_pos = encodeOutlier2Bytes(final_right_outlier_index, getBitWith(block_size-1),encode_pos,cur_byte);
            }
        }

        encode_pos =encodeOutlier2Bytes(final_normal, bit_width_final,encode_pos,cur_byte);
        if (k1 != 0)
            encode_pos = encodeOutlier2Bytes(final_left_outlier, left_bit_width,encode_pos,cur_byte);
        if (k2 != 0)
            encode_pos = encodeOutlier2Bytes(final_right_outlier, right_bit_width,encode_pos,cur_byte);
        return encode_pos;

    }




    private static int BOSBlockEncoder(int[] ts_block, int block_i, int block_size,int remaining, int encode_pos , byte[] cur_byte) {

        ArrayList<Integer> repeat_count = new ArrayList<>();
        int init_block_size = block_size;

        int[] min_delta = new int[3];
        int[]  ts_block_delta = getAbsDeltaTsBlock(ts_block,block_i, init_block_size,remaining, min_delta, repeat_count);
        block_size = min_delta[1];
        int max_delta_value = min_delta[2];

        int max_bit_width = getBitWith(max_delta_value) + 1;


        int[] findMedianArray = new int[block_size];
        System.arraycopy(ts_block_delta, 0, findMedianArray, 0, block_size);

        int median = findMedian(findMedianArray);

        //        int xl=  median;
        //        int xu = median;
        // xl = 2 median - xu
        // xl = xu - 2 ^ beta
        int left_number = 0;
        int right_number = 0;

        int length_outlier = block_size;
//        for(int value:findMedianArray){
//            if(value<=median) left_number++;
//            if (value >= median)  right_number ++;
//        }


        int final_k_start_value = -1; // x_l_minus
        int final_x_l_plus = 0; // x_l_plus
        int final_k_end_value = max_delta_value+1; // x_u_plus
        int final_x_u_minus = max_delta_value; // x_u_minus

        int min_bits = 0;
        min_bits += (getBitWith(final_k_end_value - final_k_start_value - 2) * (block_size));


        int[] count_left = new int[max_bit_width];
        int[] count_right = new int[max_bit_width];
        int count_0 = 0;


        for(int i=0;i<length_outlier;i++){
            int cur_value = ts_block_delta[i];
            if(cur_value > median){
                int beta = getBitWith(cur_value - median) ;
                count_right[beta] ++;
            } else if (cur_value < median) {
                int beta = getBitWith(median - cur_value) ;
                count_left[beta] ++;
            }else{
                count_0 ++;
            }


        }

        for(int beta = max_bit_width - 1; beta > 0 ; beta --){
            left_number += count_left[beta];
            right_number += count_right[beta];
            int pow_beta = (int) pow(2,beta-1);
            int xu = min(max_delta_value+1, median + pow_beta) ;
            int xl = max(median - pow_beta,-1);
            int cur_bits = Math.min((left_number + right_number) * getBitWith(block_size - 1), block_size + left_number + right_number);
            cur_bits += left_number * getBitWith(xl);
            cur_bits += right_number * getBitWith(max_delta_value - xu);
            cur_bits += (block_size - left_number - right_number) * getBitWith(xu - xl - 2);
            if (cur_bits < min_bits) {
                min_bits = cur_bits;

                final_k_start_value = xl;
                final_x_l_plus = xl + 1;
                final_k_end_value = xu;
                final_x_u_minus = xu -1;
            }

        }

        encode_pos = BOSEncodeBits(ts_block_delta,init_block_size,  final_k_start_value,final_x_l_plus, final_k_end_value,final_x_u_minus,
                max_delta_value, min_delta,repeat_count, encode_pos , cur_byte);

        return encode_pos;
    }

    public static int BOSEncoder(
            int[] data, int block_size, byte[] encoded_result) {

        int length_all = data.length;

        int encode_pos = 0;
        int2Bytes(length_all,encode_pos,encoded_result);
        encode_pos += 4;

        int block_num = length_all / block_size;
        int2Bytes(block_size,encode_pos,encoded_result);
        encode_pos+= 4;

        for (int i = 0; i < block_num; i++) {

            encode_pos =  BOSBlockEncoder(data, i, block_size, block_size,encode_pos,encoded_result);
//            System.out.println(encode_pos);
        }

        int remaining_length = length_all - block_num * block_size;
        if (remaining_length <= 3) {
            for (int i = remaining_length; i > 0; i--) {
                int2Bytes(data[data.length - i], encode_pos, encoded_result);
                encode_pos += 4;
            }

        } else {

            int start = block_num * block_size;
            int remaining = length_all-start;


            encode_pos = BOSBlockEncoder(data, block_num, block_size,remaining, encode_pos,encoded_result);

//            int[] ts_block = new int[length_all-start];
//            if (length_all - start >= 0) System.arraycopy(data, start, ts_block, 0, length_all - start);
//
//
//            encode_pos = BOSBlockEncoder(ts_block, encode_pos,encoded_result);

        }


        return encode_pos;
    }

    public static int EncodeBits(int num,
                                 int bit_width,
                                 int encode_pos,
                                 byte[] cur_byte,
                                 int[] bit_index_list){
        // 找到要插入的位的索引
        int bit_index = bit_index_list[0] ;//cur_byte[encode_pos + 1];

        // 计算数值的起始位位置
        int remaining_bits = bit_width;

        while (remaining_bits > 0) {
            // 计算在当前字节中可以使用的位数
            int available_bits = bit_index;
            int bits_to_write = Math.min(available_bits, remaining_bits);

            // 更新 bit_index
            bit_index = available_bits - bits_to_write;

            // 计算要写入的位的掩码和数值
            int mask = (1 << bits_to_write) - 1;
            int bits = (num >> (remaining_bits - bits_to_write)) & mask;

            // 写入到当前位置
            cur_byte[encode_pos] &= (byte) ~(mask << bit_index); // 清除对应位置的位
            cur_byte[encode_pos] |= (byte) (bits << bit_index);

            // 更新位宽和数值
            remaining_bits -= bits_to_write;
            if (bit_index == 0) {
                bit_index = 8;
                encode_pos++;
            }
        }
        bit_index_list[0] = bit_index;
//        cur_byte[encode_pos + 1] = (byte) bit_index;
        return encode_pos;
    }
    private static int BOSEncodeBitsImprove(int[] ts_block_delta,
                                            int init_block_size,
                                            int final_k_start_value,
                                            int final_x_l_plus,
                                            int final_k_end_value,
                                            int final_x_u_minus,
                                            int max_delta_value,
                                            int[] min_delta,
                                            ArrayList<Integer> repeat_count,
                                            int encode_pos,
                                            byte[] cur_byte) {
        int block_size = ts_block_delta.length;

        ArrayList<Integer> final_left_outlier_index = new ArrayList<>();
        ArrayList<Integer> final_right_outlier_index = new ArrayList<>();

        int k1 = 0;
        int k2 = 0;



        ArrayList<Integer> bitmap_outlier = new ArrayList<>();
        int index_bitmap_outlier = 0;
        int cur_index_bitmap_outlier_bits = 0;
        for (int i = 0; i < block_size; i++) {
            int cur_value = ts_block_delta[i];
            if ( cur_value<= final_k_start_value) {
//                encode_pos = EncodeBits(cur_value,left_bit_width,encode_pos,cur_byte);
//                final_left_outlier.add(cur_value);
                final_left_outlier_index.add(i);
                if (cur_index_bitmap_outlier_bits % 8 != 7) {
                    index_bitmap_outlier <<= 2;
                    index_bitmap_outlier += 3;
                    cur_index_bitmap_outlier_bits += 2;
                } else {
                    index_bitmap_outlier <<= 1;
                    index_bitmap_outlier += 1;
                    bitmap_outlier.add(index_bitmap_outlier);
                    index_bitmap_outlier = 1;
                    cur_index_bitmap_outlier_bits = 1;
                }
                k1++;


            } else if (cur_value >= final_k_end_value) {
//                encode_pos = EncodeBits(cur_value- final_k_end_value,right_bit_width,encode_pos,cur_byte);
//                final_right_outlier.add(cur_value - final_k_end_value);
                final_right_outlier_index.add(i);
                if (cur_index_bitmap_outlier_bits % 8 != 7) {
                    index_bitmap_outlier <<= 2;
                    index_bitmap_outlier += 2;
                    cur_index_bitmap_outlier_bits += 2;
                } else {
                    index_bitmap_outlier <<= 1;
                    index_bitmap_outlier += 1;
                    bitmap_outlier.add(index_bitmap_outlier);
                    index_bitmap_outlier = 0;
                    cur_index_bitmap_outlier_bits = 1;
                }
                k2++;

            } else {
//                final_normal.add(cur_value - final_x_l_plus);
//                encode_pos = EncodeBits(cur_value- final_x_l_plus,right_bit_width,encode_pos,cur_byte);
                index_bitmap_outlier <<= 1;
                cur_index_bitmap_outlier_bits += 1;
            }
            if (cur_index_bitmap_outlier_bits % 8 == 0) {
                bitmap_outlier.add(index_bitmap_outlier);
                index_bitmap_outlier = 0;
            }
        }
        if (cur_index_bitmap_outlier_bits % 8 != 0) {

            index_bitmap_outlier <<= (8 - cur_index_bitmap_outlier_bits % 8);

            index_bitmap_outlier &= 0xFF;
            bitmap_outlier.add(index_bitmap_outlier);
        }

        int final_alpha = ((k1 + k2) * getBitWith(block_size-1)) <= (block_size + k1 + k2) ? 1 : 0;


        int k_byte = (k1 << 1);
        k_byte += final_alpha;
        k_byte += (k2 << 16);

        int2Bytes(k_byte,encode_pos,cur_byte);
        encode_pos += 4;


        int2Bytes(min_delta[0],encode_pos,cur_byte);
        encode_pos += 4;

        int size = repeat_count.size();
        intByte2Bytes(size,encode_pos,cur_byte);
        encode_pos += 1;

        int[] bit_index_list = new int[1];
        bit_index_list[0] = 8;
        if (size != 0){
            int bit_width_init = getBitWith(init_block_size-1);
            for(int repeat_count_v:repeat_count){
                encode_pos = EncodeBits(repeat_count_v, bit_width_init, encode_pos, cur_byte, bit_index_list);
            }
            if(bit_index_list[0] != 8){
                bit_index_list[0] = 8;
                encode_pos ++;
            }
        }
//            encode_pos =encodeOutlier2Bytes(repeat_count, getBitWith(init_block_size-1),encode_pos,cur_byte);

//        int2Bytes(min_delta[1],encode_pos,cur_byte);
//        encode_pos += 4;

        int bit_width_final = getBitWith(final_x_u_minus - final_x_l_plus);
        intByte2Bytes(bit_width_final,encode_pos,cur_byte);
        encode_pos += 1;


        if(final_k_start_value<0 && final_k_end_value > max_delta_value){
//            int bit_width_final= getBitWith(final_x_u_minus - final_x_l_plus);
//            cur_byte[encode_pos+1] = 8;
            bit_index_list[0] = 8;
            for (int cur_value : ts_block_delta) {
                encode_pos = EncodeBits(cur_value, bit_width_final, encode_pos, cur_byte, bit_index_list);
//                final_normal.add(cur_value);
            }
            if(bit_index_list[0] != 8){
                encode_pos ++;
            }
//            cur_byte[encode_pos+1] = 0;
//            encode_pos = encodeOutlier2Bytes(final_normal, bit_width_final,encode_pos,cur_byte);
            return encode_pos;
        }


        int left_bit_width = getBitWith(final_k_start_value);//final_left_max
        int right_bit_width = getBitWith(max_delta_value - final_k_end_value);//final_right_min
        int2Bytes(final_x_l_plus,encode_pos,cur_byte);
        encode_pos += 4;
        int2Bytes(final_k_end_value,encode_pos,cur_byte);
        encode_pos += 4;

//        bit_width_final = getBitWith(final_x_u_minus - final_x_l_plus);
//        intByte2Bytes(bit_width_final,encode_pos,cur_byte);
//        encode_pos += 1;
        intByte2Bytes(left_bit_width,encode_pos,cur_byte);
        encode_pos += 1;
        intByte2Bytes(right_bit_width,encode_pos,cur_byte);
        encode_pos += 1;

        if (final_alpha == 0) { // 0

            for (int i : bitmap_outlier) {

                intByte2Bytes(i,encode_pos,cur_byte);
                encode_pos += 1;
            }
        } else {
            encode_pos = encodeOutlier2Bytes(final_left_outlier_index, getBitWith(block_size-1),encode_pos,cur_byte);
            encode_pos = encodeOutlier2Bytes(final_right_outlier_index, getBitWith(block_size-1),encode_pos,cur_byte);
        }
//        cur_byte[encode_pos+1] = 8;
//        bit_index_list[0] = 8;
        for (int cur_value : ts_block_delta) {
            if (cur_value <= final_k_start_value) {
                encode_pos = EncodeBits(cur_value, left_bit_width, encode_pos, cur_byte,bit_index_list);
            } else if (cur_value >= final_k_end_value) {
                encode_pos = EncodeBits(cur_value - final_k_end_value, right_bit_width, encode_pos, cur_byte,bit_index_list);
            } else {
                encode_pos = EncodeBits(cur_value - final_x_l_plus, bit_width_final, encode_pos, cur_byte,bit_index_list);
            }
        }
        if(bit_index_list[0] != 8){
            encode_pos ++;
        }

//        cur_byte[encode_pos+1] = 0;

//        if(k1==0 && k2==0){
//            intByte2Bytes(bit_width_final,encode_pos,cur_byte);
//            encode_pos += 1;
//
//
//        }
//        else{
//            int2Bytes(final_x_l_plus,encode_pos,cur_byte);
//            encode_pos += 4;
//            int2Bytes(final_k_end_value,encode_pos,cur_byte);
//            encode_pos += 4;
//
//            bit_width_final = getBitWith(final_x_u_minus - final_x_l_plus);
//            intByte2Bytes(bit_width_final,encode_pos,cur_byte);
//            encode_pos += 1;
//            intByte2Bytes(left_bit_width,encode_pos,cur_byte);
//            encode_pos += 1;
//            intByte2Bytes(right_bit_width,encode_pos,cur_byte);
//            encode_pos += 1;
//            if (final_alpha == 0) { // 0
//
//                for (int i : bitmap_outlier) {
//
//                    intByte2Bytes(i,encode_pos,cur_byte);
//                    encode_pos += 1;
//                }
//            } else {
//                encode_pos = encodeOutlier2Bytes(final_left_outlier_index, getBitWith(block_size-1),encode_pos,cur_byte);
//                encode_pos = encodeOutlier2Bytes(final_right_outlier_index, getBitWith(block_size-1),encode_pos,cur_byte);
//            }
//        }


//        if(k1+k2!=block_size)
//        encode_pos = encodeOutlier2Bytes(final_normal, bit_width_final,encode_pos,cur_byte);
//        if (k1 != 0)
//            encode_pos = encodeOutlier2Bytes(final_left_outlier, left_bit_width,encode_pos,cur_byte);
//        if (k2 != 0)
//            encode_pos = encodeOutlier2Bytes(final_right_outlier, right_bit_width,encode_pos,cur_byte);
//        System.out.println(encode_pos);
        return encode_pos;

    }
    private static int BOSBlockEncoderImprove(int[] ts_block, int block_i, int block_size,int remaining, int encode_pos , byte[] cur_byte) {

        ArrayList<Integer> repeat_count = new ArrayList<>();
        int init_block_size = block_size;

        int[] min_delta = new int[3];
        int[]  ts_block_delta = getAbsDeltaTsBlock(ts_block,block_i, init_block_size,remaining, min_delta, repeat_count);
        block_size = min_delta[1];
        int max_delta_value = min_delta[2];

        int max_bit_width = getBitWith(max_delta_value) + 1;


        int[] findMedianArray = new int[block_size];
        System.arraycopy(ts_block_delta, 0, findMedianArray, 0, block_size);

        int median = findMedianImprove(findMedianArray);

        //        int xl=  median;
        //        int xu = median;
        // xl = 2 median - xu
        // xl = xu - 2 ^ beta
        int left_number = 0;
        int right_number = 0;

        int length_outlier = block_size;
//        for(int value:findMedianArray){
//            if(value<=median) left_number++;
//            if (value >= median)  right_number ++;
//        }


        int final_k_start_value = -1; // x_l_minus
        int final_x_l_plus = 0; // x_l_plus
        int final_k_end_value = max_delta_value+1; // x_u_plus
        int final_x_u_minus = max_delta_value; // x_u_minus

        int min_bits = 0;
        min_bits += (getBitWith(final_k_end_value - final_k_start_value - 2) * (block_size));


        int[] count_left = new int[max_bit_width];
        int[] count_right = new int[max_bit_width];
        int count_0 = 0;


        for(int i=0;i<length_outlier;i++){
            int cur_value = ts_block_delta[i];
            if(cur_value > median){
                int beta = getBitWith(cur_value - median) ;
                count_right[beta] ++;
            } else if (cur_value < median) {
                int beta = getBitWith(median - cur_value) ;
                count_left[beta] ++;
            }else{
                count_0 ++;
            }


        }

        for(int beta = max_bit_width - 1; beta > 0 ; beta --){
            left_number += count_left[beta];
            right_number += count_right[beta];
            int pow_beta = 1 << (beta-1);
            int xu = min(max_delta_value+1, median + pow_beta) ;
            int xl = max(median - pow_beta,-1);
            int cur_bits = Math.min((left_number + right_number) * getBitWith(block_size - 1), block_size + left_number + right_number);
            cur_bits += left_number * getBitWith(xl);
            cur_bits += right_number * getBitWith(max_delta_value - xu);
            cur_bits += (block_size - left_number - right_number) * getBitWith(xu - xl - 2);
            if (cur_bits < min_bits) {
                min_bits = cur_bits;

                final_k_start_value = xl;
                final_x_l_plus = xl + 1;
                final_k_end_value = xu;
                final_x_u_minus = xu -1;
            }

        }

        encode_pos = BOSEncodeBitsImprove(ts_block_delta,init_block_size,  final_k_start_value,final_x_l_plus, final_k_end_value,final_x_u_minus,
                max_delta_value, min_delta,repeat_count, encode_pos , cur_byte);

        return encode_pos;
    }

    public static int BOSEncoderImprove(
            int[] data, int block_size, byte[] encoded_result) {

        int length_all = data.length;

        int encode_pos = 0;
        int2Bytes(length_all,encode_pos,encoded_result);
        encode_pos += 4;

        int block_num = length_all / block_size;
        int2Bytes(block_size,encode_pos,encoded_result);
        encode_pos+= 4;

        for (int i = 0; i < block_num; i++) {


            encode_pos =  BOSBlockEncoderImprove(data, i, block_size, block_size,encode_pos,encoded_result);
//            System.out.println(encode_pos);
        }

        int remaining_length = length_all - block_num * block_size;
        if (remaining_length <= 3) {
            for (int i = remaining_length; i > 0; i--) {
                int2Bytes(data[data.length - i], encode_pos, encoded_result);
                encode_pos += 4;
            }

        } else {

            int start = block_num * block_size;
            int remaining = length_all-start;

            encode_pos = BOSBlockEncoderImprove(data, block_num, block_size,remaining, encode_pos,encoded_result);

//            int[] ts_block = new int[length_all-start];
//            if (length_all - start >= 0) System.arraycopy(data, start, ts_block, 0, length_all - start);
//
//
//            encode_pos = BOSBlockEncoder(ts_block, encode_pos,encoded_result);

        }


        return encode_pos;
    }


    public static int BOSBlockDecoder(byte[] encoded, int decode_pos, int[] value_list,int init_block_size, int block_size, int[] value_pos_arr) {

        int k_byte = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;
        int k1_byte = (int) (k_byte % pow(2, 16));
        int k1 = k1_byte / 2;
        int final_alpha = k1_byte % 2;

        int k2 = (int) (k_byte / pow(2, 16));


        int min_delta = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;

        int count_size = bytes2Integer(encoded, decode_pos, 1);
        decode_pos += 1;

        ArrayList<Integer> repeat_count = new ArrayList<>();
        if (count_size != 0) {
            ArrayList<Integer> repeat_count_result = new ArrayList<>();
            repeat_count = decodeOutlier2Bytes(encoded, decode_pos, getBitWith(init_block_size-1), count_size, repeat_count_result);
            decode_pos = repeat_count_result.get(0);

        }

        int cur_block_size = block_size;
        for (int i = 1; i < count_size; i += 2) {
            cur_block_size -= (repeat_count.get(i) - 1);
        }

        ArrayList<Integer> final_left_outlier_index = new ArrayList<>();
        ArrayList<Integer> final_right_outlier_index = new ArrayList<>();
        ArrayList<Integer> final_left_outlier = new ArrayList<>();
        ArrayList<Integer> final_right_outlier = new ArrayList<>();
        ArrayList<Integer> final_normal= new ArrayList<>();;
        ArrayList<Integer> bitmap_outlier = new ArrayList<>();
        int final_k_start_value = 0;
        int final_k_end_value = 0;
        int bit_width_final = 0;
        int left_bit_width = 0;
        int right_bit_width = 0;

        if(k1!=0 || k2 != 0){
            final_k_start_value = bytes2Integer(encoded, decode_pos, 4);
            decode_pos += 4;

            final_k_end_value = bytes2Integer(encoded, decode_pos, 4);
            decode_pos += 4;

            bit_width_final = bytes2Integer(encoded, decode_pos, 1);
            decode_pos += 1;

            left_bit_width = bytes2Integer(encoded, decode_pos, 1);
            decode_pos += 1;
            right_bit_width = bytes2Integer(encoded, decode_pos, 1);
            decode_pos += 1;

            if (final_alpha == 0) {
                int bitmap_bytes = (int) Math.ceil((double) (cur_block_size + k1 + k2) / (double) 8);
                for (int i = 0; i < bitmap_bytes; i++) {
                    bitmap_outlier.add(bytes2Integer(encoded, decode_pos, 1));
                    decode_pos += 1;
                }
                int bitmap_outlier_i = 0;
                int remaining_bits = 8;
                int tmp = bitmap_outlier.get(bitmap_outlier_i);
                bitmap_outlier_i++;
                int i = 0;
                while (i < block_size ) {
                    if (remaining_bits > 1) {
                        int bit_i = (tmp >> (remaining_bits - 1)) & 0x1;
                        remaining_bits -= 1;
                        if (bit_i == 1) {
                            int bit_left_right = (tmp >> (remaining_bits - 1)) & 0x1;
                            remaining_bits -= 1;
                            if (bit_left_right == 1) {
                                final_left_outlier_index.add(i);
                            } else {
                                final_right_outlier_index.add(i);
                            }
                        }
                        if (remaining_bits == 0) {
                            remaining_bits = 8;
                            if (bitmap_outlier_i >= bitmap_bytes) break;
                            tmp = bitmap_outlier.get(bitmap_outlier_i);
                            bitmap_outlier_i++;
                        }
                    } else if (remaining_bits == 1) {
                        int bit_i = tmp & 0x1;
                        remaining_bits = 8;
                        if (bitmap_outlier_i >= bitmap_bytes) break;
                        tmp = bitmap_outlier.get(bitmap_outlier_i);
                        bitmap_outlier_i++;
                        if (bit_i == 1) {
                            int bit_left_right = (tmp >> (remaining_bits - 1)) & 0x1;
                            remaining_bits -= 1;
                            if (bit_left_right == 1) {
                                final_left_outlier_index.add(i);
                            } else {
                                final_right_outlier_index.add(i);
                            }
                        }
                    }
                    i++;
                }
            } else {
                ArrayList<Integer> decode_pos_result_left = new ArrayList<>();
                final_left_outlier_index = decodeOutlier2Bytes(encoded, decode_pos, getBitWith(block_size-1), k1, decode_pos_result_left);
                decode_pos = (decode_pos_result_left.get(0));
                ArrayList<Integer> decode_pos_result_right = new ArrayList<>();
                final_right_outlier_index = decodeOutlier2Bytes(encoded, decode_pos, getBitWith(block_size-1), k2, decode_pos_result_right);
                decode_pos = (decode_pos_result_right.get(0));
            }
        }else {
            bit_width_final = bytes2Integer(encoded, decode_pos, 1);

            decode_pos += 1;
        }


//        System.out.println(bit_width_final);
//        System.out.println("cur_block_size" + cur_block_size);
//        System.out.println(k1);
//        System.out.println("k2"+k2);
        ArrayList<Integer> decode_pos_normal = new ArrayList<>();
        final_normal = decodeOutlier2Bytes(encoded, decode_pos, bit_width_final, cur_block_size - k1 - k2, decode_pos_normal);

        decode_pos = decode_pos_normal.get(0);
        if (k1 != 0) {
            ArrayList<Integer> decode_pos_result_left = new ArrayList<>();
            final_left_outlier = decodeOutlier2Bytes(encoded, decode_pos, left_bit_width, k1, decode_pos_result_left);

            decode_pos = decode_pos_result_left.get(0);
        }
        if (k2 != 0) {
            ArrayList<Integer> decode_pos_result_right = new ArrayList<>();
            final_right_outlier = decodeOutlier2Bytes(encoded, decode_pos, right_bit_width, k2, decode_pos_result_right);
            decode_pos = decode_pos_result_right.get(0);
        }
        int left_outlier_i = 0;
        int right_outlier_i = 0;
        int normal_i = 0;
        int pre_v;
//        int final_k_end_value = (int) (final_k_start_value + pow(2, bit_width_final));

        int cur_i = 0;
        int repeat_i = 0;
        for (int i = 0; i < cur_block_size; i++) {

            int current_delta;
            if (left_outlier_i >= k1) {
                if (right_outlier_i >= k2) {
                    current_delta = final_normal.get(normal_i) + final_k_start_value+1;
                    normal_i++;
                } else if (i == final_right_outlier_index.get(right_outlier_i)) {
                    current_delta = final_right_outlier.get(right_outlier_i) + final_k_end_value;
                    right_outlier_i++;
                } else {
                    current_delta = final_normal.get(normal_i) + final_k_start_value+1;
                    normal_i++;
                }
            } else if (i == final_left_outlier_index.get(left_outlier_i)) {
                current_delta = final_left_outlier.get(left_outlier_i);
                left_outlier_i++;
            } else {

                if (right_outlier_i >= k2) {
                    current_delta = final_normal.get(normal_i) + final_k_start_value+1;
                    normal_i++;
                } else if (i == final_right_outlier_index.get(right_outlier_i)) {
                    current_delta = final_right_outlier.get(right_outlier_i) + final_k_end_value;
                    right_outlier_i++;
                } else {
                    current_delta = final_normal.get(normal_i) + final_k_start_value+1;
                    normal_i++;
                }
            }
            pre_v = current_delta + min_delta;
            if (repeat_i < count_size) {
                if (cur_i == repeat_count.get(repeat_i)) {
                    cur_i += (repeat_count.get(repeat_i+1));

                    for (int j = 0; j < repeat_count.get(repeat_i + 1); j++) {
                        value_list[value_pos_arr[0]] = pre_v;
                        value_pos_arr[0]++;
                    }
                    repeat_i += 2;
                }else {
                    cur_i++;
                    value_list[value_pos_arr[0]] = pre_v;
                    value_pos_arr[0]++;
                }
            } else {
                cur_i++;
                value_list[value_pos_arr[0]] = pre_v;
                value_pos_arr[0]++;
            }
        }
        return decode_pos;
    }

    public static void BOSDecoder(byte[] encoded) {

        int decode_pos = 0;
        int length_all = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;
        int block_size = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;


        int block_num = length_all / block_size;
        int remain_length = length_all - block_num * block_size;

        int[] value_list = new int[length_all+block_size];
        int[] value_pos_arr = new int[1];

        for (int k = 0; k < block_num; k++) {
//            System.out.println(k);
            decode_pos = BOSBlockDecoder(encoded, decode_pos, value_list,block_size, block_size,value_pos_arr);
        }

        if (remain_length <= 3) {
            for (int i = 0; i < remain_length; i++) {
                int value_end = bytes2Integer(encoded, decode_pos, 4);
                decode_pos += 4;
                value_list[value_pos_arr[0]] = value_end;
                value_pos_arr[0]++;
            }
        } else {
            BOSBlockDecoder(encoded, decode_pos, value_list,block_size, remain_length, value_pos_arr);
        }
    }

    public static int DecodeBits(byte[] cur_byte, int bit_width, int[] decode_pos_list) {
        int decode_pos = decode_pos_list[0];
        int bit_index = decode_pos_list[1];  //cur_byte[decode_pos + 1];
        int remaining_bits = bit_width;
        int num = 0;

        while (remaining_bits > 0) {
            int available_bits = bit_index;
            int bits_to_read = Math.min(available_bits, remaining_bits);

            // 计算要读取的位的掩码
            int mask = (1 << bits_to_read) - 1;
            int bits = (cur_byte[decode_pos] >> (available_bits - bits_to_read)) & mask;

            // 将读取的位合并到结果中
            num = (num << bits_to_read) | bits;

            // 更新位宽和 bit_index
            remaining_bits -= bits_to_read;
            bit_index = available_bits - bits_to_read;

            if (bit_index == 0) {
                bit_index = 8;
                decode_pos++;
            }
        }
        decode_pos_list[0] = decode_pos;
        decode_pos_list[1] = bit_index;

        return num;
    }
    public static int BOSBlockDecoderImprove(byte[] encoded, int decode_pos, int[] value_list,int init_block_size, int block_size, int[] value_pos_arr) {

        int k_byte = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;
        int k1_byte = (int) (k_byte % pow(2, 16));
        int k1 = k1_byte / 2;
        int final_alpha = k1_byte % 2;

        int k2 = (int) (k_byte / pow(2, 16));

//        int value0 = bytes2Integer(encoded, decode_pos, 4);
//        decode_pos += 4;
//        value_list[value_pos_arr[0]] =value0;
//        value_pos_arr[0] ++;

        int min_delta = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;

        int count_size = bytes2Integer(encoded, decode_pos, 1);
        decode_pos += 1;

        int[] decode_list = new int[2];
        decode_list[0]= decode_pos;
        decode_list[1]= 8;

        int valuePos = value_pos_arr[0];

//        ArrayList<Integer> repeat_count = new ArrayList<>();
        ArrayList<Integer> repeat_count = new ArrayList<>();
        if (count_size != 0) {
            int bit_width_init =  getBitWith(init_block_size-1);
            for(int i = 0;i<count_size;i++){
                int repeat_count_v = DecodeBits(encoded, bit_width_init,  decode_list);
                repeat_count.add(repeat_count_v);
            }

            if(decode_list[1] != 8){
                decode_list[1] = 8;
                decode_list[0] ++;
            }
//            repeat_count = decodeOutlier2Bytes(encoded, decode_pos, getBitWith(init_block_size-1), count_size, repeat_count_result);
            decode_pos = decode_list[0];
//            decode_list[1]= 8;
        }

        int cur_block_size = block_size;
        for (int i = 1; i < count_size; i += 2) {
            cur_block_size -= (repeat_count.get(i) - 1);
        }

        int bit_width_final = bytes2Integer(encoded, decode_pos, 1);
        decode_pos += 1;



        int pre_v;
        int cur_i = 0;
        int repeat_i = 0;
        if(k1==0 && k2==0){
//            int pre_v = value0;
            decode_list[0] = decode_pos;
            decode_list[1]= 8;
            for (int i = 0; i < cur_block_size; i++) {
                pre_v = min_delta + DecodeBits(encoded, bit_width_final,  decode_list);
//                value_list[value_pos_arr[0]++] = pre_v;
                if (repeat_i < count_size && cur_i == repeat_count.get(repeat_i)) {
                    cur_i += (repeat_count.get(repeat_i+1));

                    for (int j = 0; j < repeat_count.get(repeat_i + 1); j++) {
                        value_list[value_pos_arr[0]++] = pre_v;
                    }
                    repeat_i += 2;
                } else {
                    cur_i++;
                    value_list[value_pos_arr[0]++] = pre_v;
                }
            }
            if(decode_list[1] != 8){
                decode_list[1] = 8;
                decode_list[0] ++;
            }

//            value_pos_arr[0] = valuePos;
            return decode_list[0];
        }

        ArrayList<Integer> final_left_outlier_index = new ArrayList<>();
        ArrayList<Integer> final_right_outlier_index = new ArrayList<>();
//        ArrayList<Integer> final_left_outlier = new ArrayList<>();
//        ArrayList<Integer> final_right_outlier = new ArrayList<>();
//        ArrayList<Integer> final_normal= new ArrayList<>();;
        ArrayList<Integer> bitmap_outlier = new ArrayList<>();
        int final_k_start_value = 0;
        int final_k_end_value = 0;
//        int bit_width_final = 0;
        int left_bit_width = 0;
        int right_bit_width = 0;

        final_k_start_value = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;

        final_k_end_value = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;

//        bit_width_final = bytes2Integer(encoded, decode_pos, 1);
//        decode_pos += 1;

        left_bit_width = bytes2Integer(encoded, decode_pos, 1);
        decode_pos += 1;
        right_bit_width = bytes2Integer(encoded, decode_pos, 1);
        decode_pos += 1;

        if (final_alpha == 0) {
            int bitmap_bytes = (int) Math.ceil((double) (cur_block_size + k1 + k2) / (double) 8);
            for (int i = 0; i < bitmap_bytes; i++) {
                bitmap_outlier.add(bytes2Integer(encoded, decode_pos, 1));
                decode_pos += 1;
            }
            int bitmap_outlier_i = 0;
            int remaining_bits = 8;
            int tmp = bitmap_outlier.get(bitmap_outlier_i);
            bitmap_outlier_i++;
            int i = 0;
            while (i < cur_block_size ) {
                if (remaining_bits > 1) {
                    int bit_i = (tmp >> (remaining_bits - 1)) & 0x1;
                    remaining_bits -= 1;
                    if (bit_i == 1) {
                        int bit_left_right = (tmp >> (remaining_bits - 1)) & 0x1;
                        remaining_bits -= 1;
                        if (bit_left_right == 1) {
                            final_left_outlier_index.add(i);
                        } else {
                            final_right_outlier_index.add(i);
                        }
                    }
                    if (remaining_bits == 0) {
                        remaining_bits = 8;
                        if (bitmap_outlier_i >= bitmap_bytes) break;
                        tmp = bitmap_outlier.get(bitmap_outlier_i);
                        bitmap_outlier_i++;
                    }
                } else if (remaining_bits == 1) {
                    int bit_i = tmp & 0x1;
                    remaining_bits = 8;
                    if (bitmap_outlier_i >= bitmap_bytes) break;
                    tmp = bitmap_outlier.get(bitmap_outlier_i);
                    bitmap_outlier_i++;
                    if (bit_i == 1) {
                        int bit_left_right = (tmp >> (remaining_bits - 1)) & 0x1;
                        remaining_bits -= 1;
                        if (bit_left_right == 1) {
                            final_left_outlier_index.add(i);
                        } else {
                            final_right_outlier_index.add(i);
                        }
                    }
                }
                i++;
            }
        } else {
            ArrayList<Integer> decode_pos_result_left = new ArrayList<>();
            final_left_outlier_index = decodeOutlier2Bytes(encoded, decode_pos, getBitWith(cur_block_size-1), k1, decode_pos_result_left);
            decode_pos = (decode_pos_result_left.get(0));
            ArrayList<Integer> decode_pos_result_right = new ArrayList<>();
            final_right_outlier_index = decodeOutlier2Bytes(encoded, decode_pos, getBitWith(cur_block_size-1), k2, decode_pos_result_right);
            decode_pos = (decode_pos_result_right.get(0));
        }





//        ArrayList<Integer> decode_pos_normal = new ArrayList<>();
//        final_normal = decodeOutlier2Bytes(encoded, decode_pos, bit_width_final, block_size - k1 - k2, decode_pos_normal);
//
//        decode_pos = decode_pos_normal.get(0);
//        if (k1 != 0) {
//            ArrayList<Integer> decode_pos_result_left = new ArrayList<>();
//            final_left_outlier = decodeOutlier2Bytes(encoded, decode_pos, left_bit_width, k1, decode_pos_result_left);
//            decode_pos = decode_pos_result_left.get(0);
//        }
//        if (k2 != 0) {
//            ArrayList<Integer> decode_pos_result_right = new ArrayList<>();
//            final_right_outlier = decodeOutlier2Bytes(encoded, decode_pos, right_bit_width, k2, decode_pos_result_right);
//            decode_pos = decode_pos_result_right.get(0);
//        }
        int left_outlier_i = 0;
        int right_outlier_i = 0;
        int normal_i = 0;
//        int pre_v = value0;
//        int final_k_end_value = (int) (final_k_start_value + pow(2, bit_width_final));

// Precompute constants
        int normalOffset = min_delta + final_k_start_value;
        int rightOutlierOffset = min_delta + final_k_end_value;

// Initialize indices and pre-fetch next outlier positions
        int leftOutlierNextIndex = (left_outlier_i < k1) ? final_left_outlier_index.get(left_outlier_i) : Integer.MAX_VALUE;
        int rightOutlierNextIndex = (right_outlier_i < k2) ? final_right_outlier_index.get(right_outlier_i) : Integer.MAX_VALUE;
        decode_list[0]= decode_pos;
//        decode_list[1]= 8;
        // Use a local variable for the position
        for (int i = 0; i < cur_block_size; i++) {
//            int currentDelta;
            if (i == leftOutlierNextIndex) {
                // Process left outlier
                pre_v = min_delta + DecodeBits(encoded, left_bit_width,  decode_list); // final_left_outlier.get(left_outlier_i);
                left_outlier_i++;
                leftOutlierNextIndex = (left_outlier_i < k1) ? final_left_outlier_index.get(left_outlier_i) : Integer.MAX_VALUE;
            } else if (i == rightOutlierNextIndex) {
                // Process right outlier
                pre_v = rightOutlierOffset + DecodeBits(encoded, right_bit_width,  decode_list);// final_right_outlier.get(right_outlier_i);
                right_outlier_i++;
                rightOutlierNextIndex = (right_outlier_i < k2) ? final_right_outlier_index.get(right_outlier_i) : Integer.MAX_VALUE;
            } else {
                // Process normal value
                pre_v = normalOffset + DecodeBits(encoded, bit_width_final,  decode_list);
                normal_i++;
            }
            if (repeat_i < count_size && cur_i == repeat_count.get(repeat_i)) {
                cur_i += (repeat_count.get(repeat_i+1));

                for (int j = 0; j < repeat_count.get(repeat_i + 1); j++) {
                    value_list[value_pos_arr[0]++] = pre_v;
                }
                repeat_i += 2;
            } else {
                cur_i++;
                value_list[value_pos_arr[0]++] = pre_v;
            }
            // Update the cumulative value and store it
//            pre_v += deZigzag(currentDelta);
//            value_list[valuePos++] = pre_v;
        }
//        value_pos_arr[0] = valuePos;
        if(decode_list[1]!=8){
            return decode_list[0]+1;
        }else {
            return decode_list[0];
        }
//        decode_pos = decode_list[0];
// Update the position in the array


//        return decode_pos;
    }
    public static void BOSDecoderImprove(byte[] encoded) {

        int decode_pos = 0;
        int length_all = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;
        int block_size = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;


        int block_num = length_all / block_size;
        int remain_length = length_all - block_num * block_size;

        int[] value_list = new int[length_all+block_size];
        int[] value_pos_arr = new int[1];

        for (int k = 0; k < block_num; k++) {
//            System.out.println(k);
            decode_pos = BOSBlockDecoderImprove(encoded, decode_pos, value_list, block_size, block_size, value_pos_arr);
//            System.out.println(decode_pos);
        }
        if (remain_length <= 3) {
            for (int i = 0; i < remain_length; i++) {
                int value_end = bytes2Integer(encoded, decode_pos, 4);
                decode_pos += 4;
                value_list[value_pos_arr[0]] = value_end;
                value_pos_arr[0]++;
            }
        } else {
            BOSBlockDecoderImprove(encoded, decode_pos, value_list,block_size, remain_length, value_pos_arr);
        }
    }


    public static void main(@org.jetbrains.annotations.NotNull String[] args) throws IOException {
        String parent_dir = "/Users/xiaojinzhao/Documents/GitHub/encoding-outlier/";// your data path
//        String parent_dir = "/Users/zihanguo/Downloads/R/outlier/outliier_code/encoding-outlier/";
        String output_parent_dir = parent_dir + "icde0802/compression_ratio/rle_bos_m";

        String input_parent_dir = parent_dir + "trans_data/";
        ArrayList<String> input_path_list = new ArrayList<>();
        ArrayList<String> output_path_list = new ArrayList<>();
        ArrayList<String> dataset_name = new ArrayList<>();
        ArrayList<Integer> dataset_block_size = new ArrayList<>();
        dataset_name.add("CS-Sensors");
        dataset_name.add("Metro-Traffic");
        dataset_name.add("USGS-Earthquakes");
        dataset_name.add("YZ-Electricity");
        dataset_name.add("GW-Magnetic");
        dataset_name.add("TY-Fuel");
        dataset_name.add("Cyber-Vehicle");
        dataset_name.add("Vehicle-Charge");
        dataset_name.add("Nifty-Stocks");
        dataset_name.add("TH-Climate");
        dataset_name.add("TY-Transport");
        dataset_name.add("EPM-Education");

        for (String value : dataset_name) {
            input_path_list.add(input_parent_dir + value);
            dataset_block_size.add(1024);
        }

        output_path_list.add(output_parent_dir + "/CS-Sensors_ratio.csv"); // 0
//        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/Metro-Traffic_ratio.csv");// 1
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/USGS-Earthquakes_ratio.csv");// 2
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/YZ-Electricity_ratio.csv"); // 3
//        dataset_block_size.add(256);
        output_path_list.add(output_parent_dir + "/GW-Magnetic_ratio.csv"); //4
//        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/TY-Fuel_ratio.csv");//5
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Cyber-Vehicle_ratio.csv"); //6
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Vehicle-Charge_ratio.csv");//7
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Nifty-Stocks_ratio.csv");//8
//        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/TH-Climate_ratio.csv");//9
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/TY-Transport_ratio.csv");//10
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/EPM-Education_ratio.csv");//11
//        dataset_block_size.add(1024);

        int repeatTime2 = 100;
//        for (int file_i = 0; file_i < 1; file_i++) {
//
        for (int file_i = 0; file_i < input_path_list.size(); file_i++) {

            String inputPath = input_path_list.get(file_i);
            System.out.println(inputPath);
            String Output = output_path_list.get(file_i);

            File file = new File(inputPath);
            File[] tempList = file.listFiles();

            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file

            assert tempList != null;

            for (File f : tempList) {
                System.out.println(f);
                InputStream inputStream = Files.newInputStream(f.toPath());

                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                ArrayList<Integer> data1 = new ArrayList<>();
                ArrayList<Integer> data2 = new ArrayList<>();

                loader.readHeaders();
                while (loader.readRecord()) {
                    data1.add(Integer.valueOf(loader.getValues()[0]));
                    data2.add(Integer.valueOf(loader.getValues()[1]));
                }

                inputStream.close();
                int[] data2_arr = new int[data1.size()];
                for(int i = 0;i<data2.size();i++){
                    data2_arr[i] = data2.get(i);
                }
                byte[] encoded_result = new byte[data2_arr.length*4];
                long encodeTime = 0;
                long decodeTime = 0;
                double ratio = 0;
                double compressed_size = 0;


                int length = 0;

                long s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime2; repeat++) {
                    length =  BOSEncoder(data2_arr, dataset_block_size.get(file_i), encoded_result);
                }

                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime2);
                compressed_size += length;
                double ratioTmp = compressed_size / (double) (data1.size() * Integer.BYTES);
                ratio += ratioTmp;
                s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime2; repeat++)
                    BOSDecoder(encoded_result);
                e = System.nanoTime();
                decodeTime += ((e - s) / repeatTime2);


                String[] record = {
                        f.toString(),
                        "RLE+BOS-M",
                        String.valueOf(encodeTime),
                        String.valueOf(decodeTime),
                        String.valueOf(data1.size()),
                        String.valueOf(compressed_size),
                        String.valueOf(ratio)
                };
                writer.writeRecord(record);
                System.out.println(ratio);
            }
            writer.close();
        }
    }

    @Test
    public void ExpTest() throws IOException {
        String parent_dir = "/Users/xiaojinzhao/Documents/GitHub/encoding-outlier/";// your data path
//        String parent_dir = "/Users/zihanguo/Downloads/R/outlier/outliier_code/encoding-outlier/";
//        String output_parent_dir = parent_dir + "icde0802/compression_ratio/exp_m";
        String output_parent_dir = parent_dir + "icde0802/supply_experiment/R2O3_lower_outlier_compare/compression_ratio/bos";
        String input_parent_dir = parent_dir + "trans_data/";
        ArrayList<String> input_path_list = new ArrayList<>();
        ArrayList<String> output_path_list = new ArrayList<>();
        ArrayList<String> dataset_name = new ArrayList<>();
        ArrayList<Integer> dataset_block_size = new ArrayList<>();
        dataset_name.add("Synthetic_Exp_100");
        dataset_name.add("Synthetic_Exp_1000");
        dataset_name.add("Synthetic_Exp_10000");
        dataset_name.add("Synthetic_Exp_100000");
        dataset_name.add("Synthetic_Exp_1000000");
        dataset_name.add("Synthetic_Normal_100");
        dataset_name.add("Synthetic_Normal_1000");
        dataset_name.add("Synthetic_Normal_10000");
        dataset_name.add("Synthetic_Normal_100000");
        dataset_name.add("Synthetic_Normal_1000000");

        for (String value : dataset_name) {
            input_path_list.add(input_parent_dir + value);
            dataset_block_size.add(1024);
        }

        output_path_list.add(output_parent_dir + "/Exp_100.csv"); // 0
//        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/Exp_1000.csv");// 1
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Exp_10000.csv");// 2
        output_path_list.add(output_parent_dir + "/Exp_100000.csv");// 2
        output_path_list.add(output_parent_dir + "/Exp_1000000.csv");// 2
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Normal_100.csv"); // 3
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Normal_1000.csv"); //4
//        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/Normal_10000.csv");//5
        output_path_list.add(output_parent_dir + "/Normal_100000.csv");//5
        output_path_list.add(output_parent_dir + "/Normal_1000000.csv");//5
//        dataset_block_size.add(2048);

        int repeatTime2 = 100;
//        for (int file_i = 8; file_i < 9; file_i++) {

        for (int file_i = input_path_list.size()-1; file_i >=0 ; file_i--) {

            String inputPath = input_path_list.get(file_i);
            System.out.println(inputPath);
            String Output = output_path_list.get(file_i);

            File file = new File(inputPath);
            File[] tempList = file.listFiles();

            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file

            assert tempList != null;

            for (File f : tempList) {
                System.out.println(f);
                if(f.toString().contains(".DS")) continue;
                InputStream inputStream = Files.newInputStream(f.toPath());

                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                ArrayList<Integer> data1 = new ArrayList<>();
                ArrayList<Integer> data2 = new ArrayList<>();



                loader.readHeaders();
                while (loader.readRecord()) {
                    data1.add(Integer.valueOf(loader.getValues()[0]));
                    data2.add(Integer.valueOf(loader.getValues()[1]));
                }

                inputStream.close();
                int[] data2_arr = new int[data1.size()];
                for(int i = 0;i<data2.size();i++){
                    data2_arr[i] = data2.get(i);
                }
                byte[] encoded_result = new byte[data2_arr.length*4];
                long encodeTime = 0;
                long decodeTime = 0;
                double ratio = 0;
                double compressed_size = 0;


                int length = 0;

                long s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime2; repeat++) {
                    length =  BOSEncoder(data2_arr, dataset_block_size.get(file_i), encoded_result);
                }

                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime2);
                compressed_size += length;
                double ratioTmp = compressed_size / (double) (data1.size() * Integer.BYTES);
                ratio += ratioTmp;
                s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime2; repeat++)
                    BOSDecoder(encoded_result);
                e = System.nanoTime();
                decodeTime += ((e - s) / repeatTime2);


                String[] record = {
                        f.toString(),
                        "RLE+BOS-M",
                        String.valueOf(encodeTime),
                        String.valueOf(decodeTime),
                        String.valueOf(data1.size()),
                        String.valueOf(compressed_size),
                        String.valueOf(ratio)
                };
                writer.writeRecord(record);
                System.out.println(ratio);
            }
            writer.close();
        }
    }

    @Test
    public void compressTest() throws IOException {
//        String parent_dir = "/Users/xiaojinzhao/Documents/GitHub/encoding-outlier/";// your data path
        String parent_dir = "/Users/zihanguo/Downloads/R/outlier/outliier_code/encoding-outlier/";
        String output_parent_dir = parent_dir + "icde0802/supply_experiment/R3O2_compare_compression/compression_ratio/rle_m_c";
        String input_parent_dir = parent_dir + "trans_data/";
        ArrayList<String> input_path_list = new ArrayList<>();
        ArrayList<String> output_path_list = new ArrayList<>();
        ArrayList<String> dataset_name = new ArrayList<>();
        ArrayList<Integer> dataset_block_size = new ArrayList<>();
        dataset_name.add("CS-Sensors");
        dataset_name.add("Metro-Traffic");
        dataset_name.add("USGS-Earthquakes");
        dataset_name.add("YZ-Electricity");
        dataset_name.add("GW-Magnetic");
        dataset_name.add("TY-Fuel");
        dataset_name.add("Cyber-Vehicle");
        dataset_name.add("Vehicle-Charge");
        dataset_name.add("Nifty-Stocks");
        dataset_name.add("TH-Climate");
        dataset_name.add("TY-Transport");
        dataset_name.add("EPM-Education");

        for (String value : dataset_name) {
            input_path_list.add(input_parent_dir + value);
            dataset_block_size.add(1024);
        }

        output_path_list.add(output_parent_dir + "/CS-Sensors_ratio.csv"); // 0
//        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/Metro-Traffic_ratio.csv");// 1
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/USGS-Earthquakes_ratio.csv");// 2
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/YZ-Electricity_ratio.csv"); // 3
//        dataset_block_size.add(256);
        output_path_list.add(output_parent_dir + "/GW-Magnetic_ratio.csv"); //4
//        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/TY-Fuel_ratio.csv");//5
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Cyber-Vehicle_ratio.csv"); //6
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Vehicle-Charge_ratio.csv");//7
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Nifty-Stocks_ratio.csv");//8
//        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/TH-Climate_ratio.csv");//9
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/TY-Transport_ratio.csv");//10
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/EPM-Education_ratio.csv");//11
//        dataset_block_size.add(1024);

        int repeatTime2 = 1;
//        for (int file_i = 8; file_i < 9; file_i++) {
        CompressionType[] compressList = {
                CompressionType.LZ4,
                CompressionType.LZMA2,
        };

        for (int file_i = input_path_list.size()-1; file_i >=0 ; file_i--) {

            String inputPath = input_path_list.get(file_i);
            System.out.println(inputPath);
            String Output = output_path_list.get(file_i);

            File file = new File(inputPath);
            File[] tempList = file.listFiles();

            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Compress Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file

            assert tempList != null;

            for (File f : tempList) {
                System.out.println(f);
                InputStream inputStream = Files.newInputStream(f.toPath());

                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                ArrayList<Integer> data1 = new ArrayList<>();
                ArrayList<Integer> data2 = new ArrayList<>();

                loader.readHeaders();
                while (loader.readRecord()) {
                    data1.add(Integer.valueOf(loader.getValues()[0]));
                    data2.add(Integer.valueOf(loader.getValues()[1]));
                }
                inputStream.close();

                int[] data2_arr = new int[data1.size()];
                for(int i = 0;i<data2.size();i++){
                    data2_arr[i] = data2.get(i);
                }
                byte[] encoded_result = new byte[data2_arr.length*4];
                long encodeTime = 0;
                long decodeTime = 0;
                int length = 0;

                long s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime2; repeat++) {
                    length =  BOSEncoder(data2_arr, dataset_block_size.get(file_i), encoded_result);
                }
                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime2);

                for (CompressionType comp : compressList) {
                    double ratio = 0;
                    double compressed_size = 0;
                    ICompressor compressor = ICompressor.getCompressor(comp);
                    byte[] compressed = compressor.compress(encoded_result);

                    // test compression ratio and compressed size
                    compressed_size += compressed.length;
                    double ratioTmp = compressed_size / (double) (data1.size() * Integer.BYTES);
                    ratio += ratioTmp;
                    s = System.nanoTime();
                    for (int repeat = 0; repeat < repeatTime2; repeat++)
                        BOSDecoder(encoded_result);
                    e = System.nanoTime();
                    decodeTime += ((e - s) / repeatTime2);


                    String[] record = {
                            f.toString(),
                            "RLE+BOS-M",
                            comp.toString(),
                            String.valueOf(encodeTime),
                            String.valueOf(decodeTime),
                            String.valueOf(data1.size()),
                            String.valueOf(compressed_size),
                            String.valueOf(ratio)
                    };
                    writer.writeRecord(record);
                    System.out.println(ratio);
                }
                double ratio = 0;
                double compressed_size = 0;
                File outfile = new File(parent_dir + "icde0802/example.bin");

                // 使用FileOutputStream将byte数组写入文件
                try (FileOutputStream fos = new FileOutputStream(outfile)) {
                    fos.write(encoded_result);
                } catch (IOException e2) {
                    // 处理可能的I/O异常
                    e2.printStackTrace();
                }

                File input = new File(parent_dir + "icde0802/example.bin");
                File output = new File(parent_dir + "icde0802/example.7z");
                SevenZOutputFile out = new SevenZOutputFile(output);

                addToArchiveCompression(out, input, ".");
                out.closeArchiveEntry();

                long compressed = output.length();


                // test compression ratio and compressed size
                compressed_size += compressed;
                double ratioTmp =
                        (double) compressed / (double) (double) (data1.size() * Integer.BYTES);
                ratio += ratioTmp;


                String[] record = {
                        f.toString(),
                        "RLE+BOS-M",
                        "7ZIP",
                        String.valueOf(encodeTime),
                        String.valueOf(decodeTime),
                        String.valueOf(data1.size()),
                        String.valueOf(compressed_size),
                        String.valueOf(ratio)
                };
                writer.writeRecord(record);
                System.out.println(ratio);
            }
            writer.close();
        }
    }

    @Test
    public void BOSImproveDecodeTest() throws IOException {
        String parent_dir = "/Users/xiaojinzhao/Documents/GitHub/encoding-outlier/"; // your data path
//        String parent_dir = "/Users/zihanguo/Downloads/R/outlier/outliier_code/encoding-outlier/";
        String output_parent_dir = parent_dir + "icde0802/supply_experiment/R1O4_decode_time/compression_ratio/rle_bos_m";
        String input_parent_dir = parent_dir + "trans_data/";
        ArrayList<String> input_path_list = new ArrayList<>();
        ArrayList<String> output_path_list = new ArrayList<>();
        ArrayList<String> dataset_name = new ArrayList<>();
        ArrayList<Integer> dataset_block_size = new ArrayList<>();
        dataset_name.add("CS-Sensors");
        dataset_name.add("Metro-Traffic");
        dataset_name.add("USGS-Earthquakes");
        dataset_name.add("YZ-Electricity");
        dataset_name.add("GW-Magnetic");
        dataset_name.add("TY-Fuel");
        dataset_name.add("Cyber-Vehicle");
        dataset_name.add("Vehicle-Charge");
        dataset_name.add("Nifty-Stocks");
        dataset_name.add("TH-Climate");
        dataset_name.add("TY-Transport");
        dataset_name.add("EPM-Education");

        for (String value : dataset_name) {
            input_path_list.add(input_parent_dir + value);
            dataset_block_size.add(1024);
        }

        output_path_list.add(output_parent_dir + "/CS-Sensors_ratio.csv"); // 0
//        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/Metro-Traffic_ratio.csv");// 1
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/USGS-Earthquakes_ratio.csv");// 2
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/YZ-Electricity_ratio.csv"); // 3
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/GW-Magnetic_ratio.csv"); //4
//        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/TY-Fuel_ratio.csv");//5
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Cyber-Vehicle_ratio.csv"); //6
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Vehicle-Charge_ratio.csv");//7
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Nifty-Stocks_ratio.csv");//8
//        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/TH-Climate_ratio.csv");//9
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/TY-Transport_ratio.csv");//10
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/EPM-Education_ratio.csv");//11
//        dataset_block_size.add(1024);

        int repeatTime2 = 100;
//        for (int file_i = 4; file_i < 5; file_i++) {
//
        for (int file_i = 0; file_i < input_path_list.size(); file_i++) {

            String inputPath = input_path_list.get(file_i);
            System.out.println(inputPath);
            String Output = output_path_list.get(file_i);

            File file = new File(inputPath);
            File[] tempList = file.listFiles();

            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file

            assert tempList != null;

            for (File f : tempList) {
//                f=tempList[1];
                System.out.println(f);
                InputStream inputStream = Files.newInputStream(f.toPath());

                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                ArrayList<Integer> data1 = new ArrayList<>();
                ArrayList<Integer> data2 = new ArrayList<>();


                loader.readHeaders();
                while (loader.readRecord()) {
//                        String value = loader.getValues()[index];
                    data1.add(Integer.valueOf(loader.getValues()[0]));
                    data2.add(Integer.valueOf(loader.getValues()[1]));
//                        data.add(Integer.valueOf(value));
                }
                inputStream.close();
                int[] data2_arr = new int[data1.size()];
                for(int i = 0;i<data2.size();i++){
                    data2_arr[i] = data2.get(i);
                }
                byte[] encoded_result = new byte[data2_arr.length*4];
                long encodeTime = 0;
                long decodeTime = 0;
                double ratio = 0;
                double compressed_size = 0;


                int length = 0;

                long s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime2; repeat++) {
                    length =  BOSEncoderImprove(data2_arr, dataset_block_size.get(file_i), encoded_result);
                }

                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime2);
                compressed_size += length;
                double ratioTmp = compressed_size / (double) (data1.size() * Integer.BYTES);
                ratio += ratioTmp;
                s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime2; repeat++)
                    BOSDecoderImprove(encoded_result);
                e = System.nanoTime();
                decodeTime += ((e - s) / repeatTime2);


                String[] record = {
                        f.toString(),
                        "RLE+BOS-M",
                        String.valueOf(encodeTime),
                        String.valueOf(decodeTime),
                        String.valueOf(data1.size()),
                        String.valueOf(compressed_size),
                        String.valueOf(ratio)
                };
                writer.writeRecord(record);
                System.out.println(ratio);
//                break;
            }
            writer.close();
        }
    }

    @Test
    public void BOSImproveEncodeTest() throws IOException {
        String parent_dir = "/Users/xiaojinzhao/Documents/GitHub/encoding-outlier/"; // your data path
//        String parent_dir = "/Users/zihanguo/Downloads/R/outlier/outliier_code/encoding-outlier/";
        String output_parent_dir = parent_dir + "icde0802/compression_ratio/rle_bos_m_improve";
        String input_parent_dir = parent_dir + "trans_data/";
        ArrayList<String> input_path_list = new ArrayList<>();
        ArrayList<String> output_path_list = new ArrayList<>();
        ArrayList<String> dataset_name = new ArrayList<>();
        ArrayList<Integer> dataset_block_size = new ArrayList<>();
        dataset_name.add("CS-Sensors");
        dataset_name.add("Metro-Traffic");
        dataset_name.add("USGS-Earthquakes");
        dataset_name.add("YZ-Electricity");
        dataset_name.add("GW-Magnetic");
        dataset_name.add("TY-Fuel");
        dataset_name.add("Cyber-Vehicle");
        dataset_name.add("Vehicle-Charge");
        dataset_name.add("Nifty-Stocks");
        dataset_name.add("TH-Climate");
        dataset_name.add("TY-Transport");
        dataset_name.add("EPM-Education");

        for (String value : dataset_name) {
            input_path_list.add(input_parent_dir + value);
            dataset_block_size.add(1024);
        }

        output_path_list.add(output_parent_dir + "/CS-Sensors_ratio.csv"); // 0
//        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/Metro-Traffic_ratio.csv");// 1
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/USGS-Earthquakes_ratio.csv");// 2
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/YZ-Electricity_ratio.csv"); // 3
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/GW-Magnetic_ratio.csv"); //4
//        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/TY-Fuel_ratio.csv");//5
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Cyber-Vehicle_ratio.csv"); //6
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Vehicle-Charge_ratio.csv");//7
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Nifty-Stocks_ratio.csv");//8
//        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/TH-Climate_ratio.csv");//9
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/TY-Transport_ratio.csv");//10
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/EPM-Education_ratio.csv");//11
//        dataset_block_size.add(1024);

        int repeatTime2 = 100;
//        for (int file_i = 4; file_i < 5; file_i++) {
//
        for (int file_i = 0; file_i < input_path_list.size(); file_i++) {

            String inputPath = input_path_list.get(file_i);
            System.out.println(inputPath);
            String Output = output_path_list.get(file_i);

            File file = new File(inputPath);
            File[] tempList = file.listFiles();

            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file

            assert tempList != null;

            for (File f : tempList) {
//                f=tempList[1];
                System.out.println(f);
                InputStream inputStream = Files.newInputStream(f.toPath());

                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                ArrayList<Integer> data1 = new ArrayList<>();
                ArrayList<Integer> data2 = new ArrayList<>();


                loader.readHeaders();
                while (loader.readRecord()) {
//                        String value = loader.getValues()[index];
                    data1.add(Integer.valueOf(loader.getValues()[0]));
                    data2.add(Integer.valueOf(loader.getValues()[1]));
//                        data.add(Integer.valueOf(value));
                }
                inputStream.close();
                int[] data2_arr = new int[data1.size()];
                for(int i = 0;i<data2.size();i++){
                    data2_arr[i] = data2.get(i);
                }
                byte[] encoded_result = new byte[data2_arr.length*4];
                long encodeTime = 0;
                long decodeTime = 0;
                double ratio = 0;
                double compressed_size = 0;


                int length = 0;

                long s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime2; repeat++) {
                    length =  BOSEncoderImprove(data2_arr, dataset_block_size.get(file_i), encoded_result);
                }

                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime2);
                compressed_size += length;
                double ratioTmp = compressed_size / (double) (data1.size() * Integer.BYTES);
                ratio += ratioTmp;
                s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime2; repeat++)
                    BOSDecoderImprove(encoded_result);
                e = System.nanoTime();
                decodeTime += ((e - s) / repeatTime2);


                String[] record = {
                        f.toString(),
                        "RLE+BOS-M",
                        String.valueOf(encodeTime),
                        String.valueOf(decodeTime),
                        String.valueOf(data1.size()),
                        String.valueOf(compressed_size),
                        String.valueOf(ratio)
                };
                writer.writeRecord(record);
                System.out.println(ratio);
//                break;
            }
            writer.close();
        }
    }

}
