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

    private static class PrimitiveHashMap {
        private final long[] keys;
        private final IntSummaryStatistics[] values;

        private final int[] nextIter;

        private final int twoPow;

        private final int tableSize;

        private int next = -1;

        private MemorySegment segment;

        PrimitiveHashMap(int twoPow, MemorySegment segment) {
            this.twoPow = twoPow;
            this.tableSize = 1 << twoPow;
            this.keys = new long[tableSize * 16];
            this.values = new IntSummaryStatistics[tableSize];
            this.nextIter = new int[tableSize];
            this.segment = segment;

            for (int i = 0; i < tableSize; i++) {
                this.values[i] = new IntSummaryStatistics();
            }
        }

        public IntSummaryStatistics find(long startAddress, long endAddress, long hash, long suffix) {
            int h = Long.hashCode(hash);
            int i = (h ^ (h >> twoPow)) & (tableSize - 1);
            int hashIndex = i;
            int entryLength = (int) (endAddress - startAddress);
            do {
                int keyIndex = i << 4;
                long keyPrefix = keys[keyIndex];

                if (keyPrefix == 0) {
                    keys[keyIndex] = hash;
                    keys[keyIndex + 1] = entryLength;
                    int k;
                    int writeIndex = 2 + keyIndex;
                    for (k = 0; k + 7 < entryLength; k += 8) {
                        long keyValue = UNSAFE.getLong(startAddress + k);
                        keys[writeIndex++] = keyValue;
                    }
                    keys[writeIndex] = suffix;

                    nextIter[i] = next;
                    next = i;

                    return this.values[i];
                }

                if (compareEntryKeys(startAddress, endAddress, hash, suffix, keyIndex)) {
                    return this.values[i];
                }

                i++;
                if (i == tableSize) {
                    i = 0;
                }
            } while (i != hashIndex);

            return null;
        }

        private boolean compareEntryKeys(long startAddress, long endAddress, long hash, long suffix, int keyIndex) {
            int entryIndex = keyIndex;
            long lookupIndex = startAddress;
            if (keys[entryIndex++] != hash) {
                return false;
            }
            int entryLength = (int) keys[entryIndex++];
            int lookupLength = (int) (endAddress - startAddress);
            if (entryLength != lookupLength) {
                return false;
            }

            // @ Vaidhy, uncomment this to enable vectorized comparison!
            // if (lookupLength > LongVector.SPECIES_PREFERRED.vectorByteSize()) {
            // LongVector fromSegment = LongVector.fromMemorySegment(LongVector.SPECIES_PREFERRED,
            // segment, startAddress - segment.address(), ByteOrder.nativeOrder());
            //
            // LongVector fromKeyStore = LongVector.fromArray(LongVector.SPECIES_PREFERRED,
            // keys, entryIndex);
            //
            // if (!fromSegment.eq(fromKeyStore).allTrue()) {
            // return false;
            // }
            //
            // lookupIndex += fromSegment.byteSize();
            // entryIndex += fromSegment.length();
            // }

            for (; (lookupIndex + 7) < endAddress; lookupIndex += 8) {
                if (UNSAFE.getLong(lookupIndex) != keys[entryIndex++]) {
                    return false;
                }
            }
            return keys[entryIndex] == suffix;
        }

        public Iterable<Map.Entry<String, IntSummaryStatistics>> entrySet() {
            return () -> new Iterator<>() {
                int scan = next;

                @Override
                public boolean hasNext() {
                    return scan != -1;
                }

                @Override
                public Map.Entry<String, IntSummaryStatistics> next() {
                    int keyIndex = scan << 4;
                    byte[] outputArr = new byte[128];
                    ByteBuffer buf = ByteBuffer.wrap(outputArr)
                            .order(ByteOrder.nativeOrder());
                    int length = (int) keys[keyIndex + 1];
                    for (int i = 0; i < 14; i++) {
                        buf.putLong(keys[keyIndex + 2 + i]);
                    }

                    String key = new String(outputArr, 0, length, StandardCharsets.UTF_8);
                    IntSummaryStatistics value = values[scan];

                    scan = nextIter[scan];

                    return new AbstractMap.SimpleEntry<>(
                            key,
                            value);
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

        void process(long keyStartAddress, long keyEndAddress, long hash, long suffix, int temperature);

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
            long s = 0;

            buf.rewind();

            for (long i = position; i < fileEnd; i++) {
                byte ch = UNSAFE.getByte(i);
                if (ch == ';') {
                    int discardBits = buf.remaining();
                    buf.rewind();
                    if (discardBits != 8) {
                        long nextData = (buf.getLong() << (8 * discardBits)) >>> (8 * discardBits);
                        this.suffix = nextData;
                        this.hash = simpleHash(h, nextData);
                    }
                    else {
                        this.suffix = s;
                        this.hash = h;
                    }
                    position = i + 1;
                    return i;
                }
                if (!buf.hasRemaining()) {
                    buf.flip();
                    long nextData = buf.getLong();
                    h = simpleHash(h, nextData);
                    s = nextData;
                    buf.rewind();
                }
                buf.put(ch);
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
        long chunkStart = offset + fileService.address();
        long chunkEnd = chunkStart + chunkSize;
        long fileEnd = fileService.address() + fileService.length();
        long stopPoint = Math.min(chunkEnd + 1, fileEnd);

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
            do {
                long data = UNSAFE.getLong(position);
                int semiPosition = findByteOctet(data, SEMI_DETECTION);
                if (semiPosition != -1) {
                    stationEnd = position + semiPosition;
                    position = stationEnd + 1;

                    if (semiPosition != 0) {
                        suffix = data & (ALL_ONES >>> (64 - (semiPosition << 3)));
                        hash = simpleHash(hash, suffix);
                    }
                    // else {
                    //// suffix = UNSAFE.getLong(position - 8);
                    // }
                    break;
                }
                else {
                    hash = simpleHash(hash, data);
                    suffix = data;
                    position = position + 8;
                }
            } while (true);
            int temperature = 0;
            {
                byte ch = UNSAFE.getByte(position++);
                boolean negative = false;
                if (ch == '-') {
                    negative = true;
                    ch = UNSAFE.getByte(position++);
                }
                while (ch != '\n') {
                    if (ch != '.') {
                        temperature *= 10;
                        temperature += (ch ^ '0');
                    }
                    ch = UNSAFE.getByte(position++);
                }
                if (negative) {
                    temperature = -temperature;
                }
            }

            lineConsumer.process(stationStart, stationEnd, hash, suffix, temperature);
        }
    }

    private void smallWorker(long offset, long chunkSize, MapReduce<I> lineConsumer) {
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
            long keyHash = lineStream.hash;
            long suffix = lineStream.suffix;
            long valueStartAddress = lineStream.position;
            long valueEndAddress = lineStream.findTemperature();
            int temperature = parseDouble(valueStartAddress, valueEndAddress);
            lineConsumer.process(keyStartAddress, keyEndAddress, keyHash, suffix, temperature);
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
        private final long fileSize;

        private final MemorySegment segment;

        DiskFileService(String fileName) throws IOException {
            FileChannel fileChannel = FileChannel.open(Path.of(fileName),
                    StandardOpenOption.READ);
            this.fileSize = fileChannel.size();
            this.segment = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0,
                    fileSize, Arena.global());
        }

        @Override
        public long length() {
            return fileSize;
        }

        @Override
        public long address() {
            return segment.address();
        }

        public MemorySegment segment() {
            return segment;
        }
    }

    private static class ChunkProcessorImpl implements MapReduce<PrimitiveHashMap> {

        // 1 << 14 > 10,000 so it works
        private final PrimitiveHashMap statistics;

        private ChunkProcessorImpl(FileService fileService) {
            this.statistics = new PrimitiveHashMap(14, fileService.segment());
        }

        @Override
        public void process(long keyStartAddress, long keyEndAddress, long hash, long suffix, int temperature) {
            IntSummaryStatistics entry = statistics.find(keyStartAddress, keyEndAddress, hash, suffix);
            // System.out.println(STR."\{unsafeToString(keyStartAddress, keyEndAddress)} --> \{temperature}");
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
                () -> new ChunkProcessorImpl(diskFileService),
                CalculateAverage_vaidhy::combineOutputs);

        int proc = Runtime.getRuntime().availableProcessors();

        ExecutorService executor = Executors.newFixedThreadPool(proc);
        Map<String, IntSummaryStatistics> output = calculateAverageVaidhy.master(2 * proc, executor);
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
