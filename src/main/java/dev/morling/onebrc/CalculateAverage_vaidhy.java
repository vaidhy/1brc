/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.LongVector;
import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Supplier;

public class CalculateAverage_vaidhy<I, T> {

    private static final class HashEntry {
        private long startAddress;
        private long endAddress;
        private long hash;
        private long suffix;
        private int next;
        IntSummaryStatistics value;
    }

    private static class PrimitiveHashMap {
        private final long[] keys;
        private final IntSummaryStatistics[] values;

        private final int[] nextIter;

        private final int twoPow;

        private final int tableSize;

        private int next = -1;

        PrimitiveHashMap(int twoPow) {
            this.twoPow = twoPow;
            this.tableSize = 1 << twoPow;
            this.keys = new long[tableSize * 16];
            this.values = new IntSummaryStatistics[tableSize];
            this.nextIter = new int[tableSize];

            for (int i = 0; i < tableSize; i++) {
                this.values[i] = new IntSummaryStatistics();
            }
        }

        public IntSummaryStatistics find(LongVector key, long hash) {
            int h = Long.hashCode(hash);
            int i = (h ^ (h >> twoPow)) & (tableSize - 1);

            LongVector entryVector = LongVector.fromArray(LongVector.SPECIES_128,
                    keys, i * 16);

            if (entryVector.eq(key).allTrue()) {
                return values[i];
            }

            if (entryVector.lane(0) == 0) {
                key.intoArray(keys, i * 16);
                nextIter[i] = next;
                next = i;
                return values[i];
            }

            i++;
            if (i == tableSize) {
                i = 0;
            }

            do {
                entryVector = LongVector.fromArray(LongVector.SPECIES_128,
                        keys, i * 16);

                if (entryVector.eq(key).allTrue()) {
                    return values[i];
                }

                if (entryVector.lane(0) == 0) {
                    key.intoArray(keys, i * 16);
                    nextIter[i] = next;
                    next = i;
                    return values[i];
                }

                i++;
                if (i == tableSize) {
                    i = 0;
                }
            } while (i != hash);

            return null;
        }

        // private static boolean compareEntryKeys(long startAddress, long endAddress, HashEntry entry) {
        // long entryIndex = entry.startAddress;
        // long lookupIndex = startAddress;
        //
        // for (; (lookupIndex + 7) < endAddress; lookupIndex += 8) {
        // if (UNSAFE.getLong(entryIndex) != UNSAFE.getLong(lookupIndex)) {
        // return false;
        // }
        // entryIndex += 8;
        // }
        // // for (; lookupIndex < endAddress; lookupIndex++) {
        // // if (UNSAFE.getByte(entryIndex) != UNSAFE.getByte(lookupIndex)) {
        // // return false;
        // // }
        // // entryIndex++;
        // // }
        //
        // return true;
        // }

        public Iterable<Map.Entry<String, IntSummaryStatistics>> entrySet() {
            return () -> new Iterator<>() {
                int scan = next;

                @Override
                public boolean hasNext() {
                    return scan != -1;
                }

                @Override
                public Map.Entry<String, IntSummaryStatistics> next() {
                    IntSummaryStatistics value = values[scan];

                    ByteVector entryVector = LongVector.fromArray(LongVector.SPECIES_128,
                            keys, scan * 16)
                            .reinterpretAsBytes();

                    byte[] station = entryVector.toArray();
                    String key = new String(station, 8, station[0], StandardCharsets.UTF_8);

                    scan = nextIter[scan];
                    return new AbstractMap.SimpleEntry<>(key, value);
                }
            };
        }
    }

    private static final String FILE = "./measurements.txt";

    private static final long C1 = 0x87c37b91114253d5L;
    private static final long C2 = 0x4cf5ad432745937fL;
    private static final int R1 = 31;
    private static final long DEFAULT_SEED = 104729;

    private static long simpleHash(long hash, long nextData) {
        // return hash ^ nextData;
        return (hash ^ Long.rotateLeft((nextData * C1), R1)) * C2;
    }

    private static Unsafe initUnsafe() {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            return (Unsafe) theUnsafe.get(Unsafe.class);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static final Unsafe UNSAFE = initUnsafe();

    private static int parseDoubleFromLong(long data, int len) {
        int normalized = 0;
        boolean negative = false;
        for (int i = 0; i < len; i++) {
            long ch = data & 0xff;
            data >>>= 1;
            if (ch == '-') {
                negative = true;
                continue;
            }
            if (ch == '.') {
                continue;
            }
            normalized = (normalized * 10) + (normalized ^ 0x30);
        }
        return negative ? -normalized : normalized;
    }

    private static int parseDouble(long startAddress, long endAddress) {
        int normalized;
        int length = (int) (endAddress - startAddress);
        if (length == 5) {
            normalized = (UNSAFE.getByte(startAddress + 1) ^ 0x30);
            normalized = (normalized << 3) + (normalized << 1) + (UNSAFE.getByte(startAddress + 2) ^ 0x30);
            normalized = (normalized << 3) + (normalized << 1) + (UNSAFE.getByte(startAddress + 4) ^ 0x30);
            normalized = -normalized;
            return normalized;
        }
        if (length == 3) {
            normalized = (UNSAFE.getByte(startAddress) ^ 0x30);
            normalized = (normalized << 3) + (normalized << 1) + (UNSAFE.getByte(startAddress + 2) ^ 0x30);
            return normalized;
        }

        if (UNSAFE.getByte(startAddress) == '-') {
            normalized = (UNSAFE.getByte(startAddress + 1) ^ 0x30);
            normalized = (normalized << 3) + (normalized << 1) + (UNSAFE.getByte(startAddress + 3) ^ 0x30);
            normalized = -normalized;
            return normalized;
        }
        else {
            normalized = (UNSAFE.getByte(startAddress) ^ 0x30);
            normalized = (normalized << 3) + (normalized << 1) + (UNSAFE.getByte(startAddress + 1) ^ 0x30);
            normalized = (normalized << 3) + (normalized << 1) + (UNSAFE.getByte(startAddress + 3) ^ 0x30);
            return normalized;
        }
    }

    interface MapReduce<I> {

        void process(LongVector key, long hash, int temperature);

        I result();
    }

    private final FileService fileService;
    private final Supplier<MapReduce<I>> chunkProcessCreator;
    private final Function<List<I>, T> reducer;

    interface FileService {
        long length();

        long address();

        MemorySegment segment();
    }

    CalculateAverage_vaidhy(FileService fileService,
                            Supplier<MapReduce<I>> mapReduce,
                            Function<List<I>, T> reducer) {
        this.fileService = fileService;
        this.chunkProcessCreator = mapReduce;
        this.reducer = reducer;
    }

    static class LineStream {
        private final long fileEnd;
        private final long chunkEnd;

        private long position;
        private long hash;

        private long suffix;

        private final ByteBuffer buf = ByteBuffer
                .allocate(8)
                .order(ByteOrder.LITTLE_ENDIAN);

        public LineStream(FileService fileService, long offset, long chunkSize) {
            long fileStart = fileService.address();
            this.fileEnd = fileStart + fileService.length();
            this.chunkEnd = fileStart + offset + chunkSize;
            this.position = fileStart + offset;
            this.hash = 0;
        }

        public boolean hasNext() {
            return position <= chunkEnd;
        }

        public long findSemi() {
            long h = DEFAULT_SEED;
            buf.rewind();

            for (long i = position; i < fileEnd; i++) {
                byte ch = UNSAFE.getByte(i);
                if (ch == ';') {
                    int discard = buf.remaining();
                    buf.rewind();
                    long nextData = (buf.getLong() << discard) >>> discard;
                    this.suffix = nextData;
                    this.hash = simpleHash(h, nextData);
                    position = i + 1;
                    return i;
                }
                if (buf.hasRemaining()) {
                    buf.put(ch);
                }
                else {
                    buf.flip();
                    long nextData = buf.getLong();
                    h = simpleHash(h, nextData);
                    buf.rewind();
                }
            }
            this.hash = h;
            this.suffix = buf.getLong();
            position = fileEnd;
            return fileEnd;
        }

        public long skipLine() {
            for (long i = position; i < fileEnd; i++) {
                byte ch = UNSAFE.getByte(i);
                if (ch == 0x0a) {
                    position = i + 1;
                    return i;
                }
            }
            position = fileEnd;
            return fileEnd;
        }

        public long findTemperature() {
            position += 3;
            for (long i = position; i < fileEnd; i++) {
                byte ch = UNSAFE.getByte(i);
                if (ch == 0x0a) {
                    position = i + 1;
                    return i;
                }
            }
            position = fileEnd;
            return fileEnd;
        }
    }

    private static final long START_BYTE_INDICATOR = 0x0101_0101_0101_0101L;
    private static final long END_BYTE_INDICATOR = START_BYTE_INDICATOR << 7;

    private static final long NEW_LINE_DETECTION = START_BYTE_INDICATOR * '\n';

    private static final long SEMI_DETECTION = START_BYTE_INDICATOR * ';';

    private static final long ALL_ONES = 0xffff_ffff_ffff_ffffL;

    // Influenced by roy's SIMD as a register technique.
    private int findByte(long data, long pattern, int readOffsetBits) {
        data >>>= readOffsetBits;
        long match = data ^ pattern;
        long mask = (match - START_BYTE_INDICATOR) & ((~match) & END_BYTE_INDICATOR);

        if (mask == 0) {
            // Not Found
            return -1;
        }
        else {
            // Found
            int trailingZeroes = Long.numberOfTrailingZeros(mask) - 7;
            return readOffsetBits + trailingZeroes;
        }
    }

    private int findByteOctet(long data, long pattern) {
        long match = data ^ pattern;
        long mask = (match - START_BYTE_INDICATOR) & ((~match) & END_BYTE_INDICATOR);

        if (mask == 0) {
            // Not Found
            return -1;
        }
        else {
            // Found
            return Long.numberOfTrailingZeros(mask) >>> 3;
        }
    }

    private int findSemi(long data, int readOffsetBits) {
        return findByte(data, SEMI_DETECTION, readOffsetBits);
    }

    private int findNewLine(long data, int readOffsetBits) {
        return findByte(data, NEW_LINE_DETECTION, readOffsetBits);
    }

    private void bigWorker(long offset, long chunkSize, MapReduce<I> lineConsumer) {
        long baseAddress = fileService.address();
        long chunkStart = offset + baseAddress;
        long chunkEnd = chunkStart + chunkSize;
        long fileEnd = baseAddress + fileService.length();
        long stopPoint = Math.min(chunkEnd + 1, fileEnd);

        long[] keyStorage = new long[16];

        boolean skip = offset != 0;
        for (long position = chunkStart; position < stopPoint;) {

            if (skip) {
                long data = UNSAFE.getLong(position);
                int newLinePosition = findByteOctet(data, NEW_LINE_DETECTION);
                if (newLinePosition != -1) {
                    skip = false;
                    position = position + newLinePosition + 1;
                }
                else {
                    position = position + 8;
                }
                continue;
            }

            long stationStart = position;
            long stationEnd = -1;
            long hash = DEFAULT_SEED;
            long suffix = 0;
            int keyIndex = 1;
            do {
                long data = UNSAFE.getLong(position);
                int semiPosition = findByteOctet(data, SEMI_DETECTION);
                if (semiPosition != -1) {
                    stationEnd = position + semiPosition;
                    position = stationEnd + 1;

                    if (semiPosition != 0) {
                        suffix = data & (ALL_ONES >>> (64 - (semiPosition << 3)));
                    }
                    else {
                        suffix = UNSAFE.getLong(position - 8);
                    }
                    hash = simpleHash(hash, suffix);
                    keyStorage[keyIndex++] = suffix;
                    Arrays.fill(keyStorage, keyIndex, keyStorage.length, 0);
                    keyStorage[0] = stationEnd - stationStart;
                    break;
                }
                else {
                    keyStorage[keyIndex++] = data;
                    hash = simpleHash(hash, data);
                    position = position + 8;
                }
            } while (true);

            int temperature = 0;
            {
                byte ch = UNSAFE.getByte(position);
                boolean negative = false;
                if (ch == '-') {
                    negative = true;
                    position++;
                }
                while (true) {
                    position++;
                    if (ch == '\n') {
                        break;
                    }
                    if (ch != '.') {
                        temperature *= 10;
                        temperature += (ch ^ '0');
                    }
                    ch = UNSAFE.getByte(position);
                }
                if (negative) {
                    temperature = -temperature;
                }
            }

            LongVector key = LongVector.fromArray(LongVector.SPECIES_128, keyStorage, 0);

            lineConsumer.process(key, hash, temperature);
        }
    }

    private void smallWorker(long offset, long chunkSize, MapReduce<I> lineConsumer) {
        byte[] keyStorage = new byte[128];
        LineStream lineStream = new LineStream(fileService, offset, chunkSize);

        if (offset != 0) {
            if (lineStream.hasNext()) {
                // Skip the first line.
                lineStream.skipLine();
            }
            else {
                // No lines then do nothing.
                return;
            }
        }
        while (lineStream.hasNext()) {
            long keyStartAddress = lineStream.position;
            long keyEndAddress = lineStream.findSemi();
            int keyLen = (int) (keyEndAddress - keyStartAddress);
            Arrays.fill(keyStorage, 0, keyStorage.length, (byte) 0);

            keyStorage[0] = (byte) keyLen;
            int index;
            for (index = 8; index < keyLen + 8; index++) {
                keyStorage[index] = UNSAFE.getByte(keyStartAddress + index - 8);
            }

            long keyHash = lineStream.hash;
            long valueStartAddress = lineStream.position;
            long valueEndAddress = lineStream.findTemperature();
            int temperature = parseDouble(valueStartAddress, valueEndAddress);
            // System.out.println("Small worker!");

            LongVector keyVector = ByteVector.fromArray(ByteVector.SPECIES_128, keyStorage, 0)
                    .reinterpretAsLongs();
            lineConsumer.process(keyVector, keyHash, temperature);
        }
    }

    // file size = 7
    // (0,0) (0,0) small chunk= (0,7)
    // a;0.1\n

    public T master(int shards, ExecutorService executor) {
        List<Future<I>> summaries = new ArrayList<>();
        long len = fileService.length();

        if (len > 128) {
            long bigChunk = Math.floorDiv(len, shards);
            long bigChunkReAlign = bigChunk & 0xffff_ffff_ffff_fff8L;

            long smallChunkStart = bigChunkReAlign * shards;
            long smallChunkSize = len - smallChunkStart;

            for (long offset = 0; offset < smallChunkStart; offset += bigChunkReAlign) {
                MapReduce<I> mr = chunkProcessCreator.get();
                final long transferOffset = offset;
                Future<I> task = executor.submit(() -> {
                    bigWorker(transferOffset, bigChunkReAlign, mr);
                    return mr.result();
                });
                summaries.add(task);
            }

            MapReduce<I> mrLast = chunkProcessCreator.get();
            Future<I> lastTask = executor.submit(() -> {
                smallWorker(smallChunkStart, smallChunkSize - 1, mrLast);
                return mrLast.result();
            });
            summaries.add(lastTask);
        }
        else {

            MapReduce<I> mrLast = chunkProcessCreator.get();
            Future<I> lastTask = executor.submit(() -> {
                smallWorker(0, len - 1, mrLast);
                return mrLast.result();
            });
            summaries.add(lastTask);
        }

        List<I> summariesDone = summaries.stream()
                .map(task -> {
                    try {
                        return task.get();
                    }
                    catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                })
                .toList();
        return reducer.apply(summariesDone);
    }

    static class DiskFileService implements FileService {
        private final MemorySegment segment;

        DiskFileService(String fileName) throws IOException {
            FileChannel fileChannel = FileChannel.open(Path.of(fileName),
                    StandardOpenOption.READ);
            long fileSize = fileChannel.size();
            this.segment = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0,
                    fileSize, Arena.global());
        }

        @Override
        public long length() {
            return segment.byteSize();
        }

        @Override
        public long address() {
            return segment.address();
        }

        @Override
        public MemorySegment segment() {
            return segment;
        }
    }

    private static class ChunkProcessorImpl implements MapReduce<PrimitiveHashMap> {

        // 1 << 14 > 10,000 so it works
        private final PrimitiveHashMap statistics = new PrimitiveHashMap(14);

        @Override
        public void process(LongVector key, long hash, int temperature) {
            IntSummaryStatistics entry = statistics.find(key, hash);
            if (entry == null) {
                throw new IllegalStateException("Hash table too small :(");
            }
            entry.accept(temperature);
        }

        @Override
        public PrimitiveHashMap result() {
            return statistics;
        }
    }

    public static void main(String[] args) throws IOException {
        DiskFileService diskFileService = new DiskFileService(FILE);

        CalculateAverage_vaidhy<PrimitiveHashMap, Map<String, IntSummaryStatistics>> calculateAverageVaidhy = new CalculateAverage_vaidhy<>(
                diskFileService,
                ChunkProcessorImpl::new,
                CalculateAverage_vaidhy::combineOutputs);

        int proc = 2 * Runtime.getRuntime().availableProcessors();

        ExecutorService executor = Executors.newFixedThreadPool(proc);
        Map<String, IntSummaryStatistics> output = calculateAverageVaidhy.master(proc, executor);
        executor.shutdown();

        Map<String, String> outputStr = toPrintMap(output);
        System.out.println(outputStr);
    }

    private static Map<String, String> toPrintMap(Map<String, IntSummaryStatistics> output) {

        Map<String, String> outputStr = new TreeMap<>();
        for (Map.Entry<String, IntSummaryStatistics> entry : output.entrySet()) {
            IntSummaryStatistics stat = entry.getValue();
            outputStr.put(entry.getKey(),
                    STR."\{stat.getMin() / 10.0}/\{Math.round(stat.getAverage()) / 10.0}/\{stat.getMax() / 10.0}");
        }
        return outputStr;
    }

    private static Map<String, IntSummaryStatistics> combineOutputs(
                                                                    List<PrimitiveHashMap> list) {

        Map<String, IntSummaryStatistics> output = new HashMap<>(10000);
        for (PrimitiveHashMap map : list) {
            for (Map.Entry<String, IntSummaryStatistics> entry : map.entrySet()) {
                if (entry.getValue() != null) {
                    output.compute(entry.getKey(), (ignore, val) -> {
                        if (val == null) {
                            return entry.getValue();
                        }
                        else {
                            val.combine(entry.getValue());
                            return val;
                        }
                    });
                }
            }
        }

        return output;
    }

    private static String unsafeToString(long startAddress, long endAddress) {
        byte[] keyBytes = new byte[(int) (endAddress - startAddress)];
        for (int i = 0; i < keyBytes.length; i++) {
            keyBytes[i] = UNSAFE.getByte(startAddress + i);
        }
        return new String(keyBytes, StandardCharsets.UTF_8);
    }
}
