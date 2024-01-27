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

import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.foreign.Arena;
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
        private long suffix;
        private int next;
        IntSummaryStatistics value;
    }

    private static class PrimitiveHashMap {
        private final HashEntry[] entries;
        private final long[] hashes;

        private final int twoPow;
        private int next = -1;

        PrimitiveHashMap(int twoPow) {
            this.twoPow = twoPow;
            this.entries = new HashEntry[1 << twoPow];
            this.hashes = new long[1 << twoPow];
            for (int i = 0; i < entries.length; i++) {
                this.entries[i] = new HashEntry();
            }
        }

        public HashEntry find(long startAddress, long endAddress, long hash, long suffix) {
            int len = entries.length;
            int h = Long.hashCode(hash);
            int i = (h ^ (h >> twoPow)) & (len - 1);
            long lookupLength = endAddress - startAddress;

            long hashEntry = hashes[i];
            if (hashEntry == 0) {
                HashEntry entry = entries[i];
                entry.startAddress = startAddress;
                entry.endAddress = endAddress;
                hashes[i] = hash;
                entry.next = next;
                entry.suffix = suffix;
                this.next = i;
                return entry;
            }

            if (hashEntry == hash) {
                HashEntry entry = entries[i];
                if (entry.suffix == suffix) {
                    long entryLength = entry.endAddress - entry.startAddress;
                    if (entryLength == lookupLength) {
                        boolean found = compareEntryKeys(startAddress, endAddress, entry);
                        if (found) {
                            return entry;
                        }
                    }
                }
            }

            i++;
            if (i == len) {
                i = 0;
            }

            do {
                hashEntry = hashes[i];
                if (hashEntry == 0) {
                    HashEntry entry = entries[i];
                    entry.startAddress = startAddress;
                    entry.endAddress = endAddress;
                    hashes[i] = hash;
                    entry.next = next;
                    entry.suffix = suffix;
                    this.next = i;
                    return entry;
                }

                if (hashEntry == hash) {
                    HashEntry entry = entries[i];
                    if (entry.suffix == suffix) {
                        long entryLength = entry.endAddress - entry.startAddress;
                        if (entryLength == lookupLength) {
                            boolean found = compareEntryKeys(startAddress, endAddress, entry);
                            if (found) {
                                return entry;
                            }
                        }
                    }
                }
                i++;
                if (i == len) {
                    i = 0;
                }
            } while (i != hash);
            return null;
        }

        private static boolean compareEntryKeys(long startAddress, long endAddress, HashEntry entry) {
            long entryIndex = entry.startAddress;
            long lookupIndex = startAddress;

            for (; (lookupIndex + 7) < endAddress; lookupIndex += 8) {
                if (UNSAFE.getLong(entryIndex) != UNSAFE.getLong(lookupIndex)) {
                    return false;
                }
                entryIndex += 8;
            }
            // for (; lookupIndex < endAddress; lookupIndex++) {
            // if (UNSAFE.getByte(entryIndex) != UNSAFE.getByte(lookupIndex)) {
            // return false;
            // }
            // entryIndex++;
            // }

            return true;
        }

        public Iterable<HashEntry> entrySet() {
            return () -> new Iterator<>() {
                int scan = next;

                @Override
                public boolean hasNext() {
                    return scan != -1;
                }

                @Override
                public HashEntry next() {
                    HashEntry entry = entries[scan];
                    scan = entry.next;
                    return entry;
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
                    }
                    else {
                        suffix = UNSAFE.getLong(position - 8);
                    }
                    hash = simpleHash(hash, suffix);
                    break;
                }
                else {
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

            lineConsumer.process(stationStart, stationEnd, hash, suffix, temperature);
        }
    }

    private void bigWorkerAligned(long offset, long chunkSize, MapReduce<I> lineConsumer) {
        long fileStart = fileService.address();
        long chunkEnd = fileStart + offset + chunkSize;
        long newRecordStart = fileStart + offset;
        long position = fileStart + offset;
        long fileEnd = fileStart + fileService.length();

        int nextReadOffsetBits = 0;

        long data = UNSAFE.getLong(position);

        if (offset != 0) {
            boolean foundNewLine = false;
            for (; position < chunkEnd; position += 8) {
                data = UNSAFE.getLong(position);
                int newLinePositionBits = findNewLine(data, nextReadOffsetBits);
                if (newLinePositionBits != -1) {
                    nextReadOffsetBits = newLinePositionBits + 8;
                    newRecordStart = position + (nextReadOffsetBits >>> 3);

                    if (nextReadOffsetBits == 64) {
                        position += 8;
                        nextReadOffsetBits = 0;
                        data = UNSAFE.getLong(position);
                    }
                    foundNewLine = true;
                    break;
                }
            }
            if (!foundNewLine) {
                return;
            }
        }

        boolean newLineToken = false;
        // false means looking for semi Colon
        // true means looking for new line.

        long stationEnd = offset;

        long hash = DEFAULT_SEED;
        long prevRelevant = 0;
        int prevBits = 0;
        long suffix = 0;

        long crossPoint = Math.min(chunkEnd + 1, fileEnd);

        while (true) {
            if (newLineToken) {
                int newLinePositionBits = findNewLine(data, nextReadOffsetBits);
                if (newLinePositionBits == -1) {
                    nextReadOffsetBits = 0;
                    position += 8;
                    data = UNSAFE.getLong(position);
                }
                else {
                    long temperatureEnd = position + (newLinePositionBits >>> 3);
                    int temperature = parseDouble(stationEnd + 1, temperatureEnd);
                    lineConsumer.process(newRecordStart, stationEnd, hash, suffix, temperature);
                    newLineToken = false;

                    nextReadOffsetBits = newLinePositionBits + 8;
                    newRecordStart = temperatureEnd + 1;

                    if (newRecordStart >= crossPoint) {
                        break;
                    }

                    hash = DEFAULT_SEED;

                    if (nextReadOffsetBits == 64) {
                        nextReadOffsetBits = 0;
                        position += 8;
                        data = UNSAFE.getLong(position);
                    }
                }
            }
            else {
                int semiPositionBits = findSemi(data, nextReadOffsetBits);

                if (semiPositionBits == -1) {
                    long currRelevant = data >>> (nextReadOffsetBits - prevBits);

                    prevRelevant = prevRelevant | currRelevant;
                    int newPrevBits = prevBits + (64 - nextReadOffsetBits);

                    if (newPrevBits >= 64) {
                        hash = simpleHash(hash, prevRelevant);

                        prevBits = (newPrevBits - 64);
                        if (prevBits != 0) {
                            prevRelevant = (data >>> (64 - prevBits));
                        }
                        else {
                            prevRelevant = 0;
                        }
                    }
                    else {
                        prevBits = newPrevBits;
                    }

                    nextReadOffsetBits = 0;
                    position += 8;
                    data = UNSAFE.getLong(position);
                }
                else {
                    // currentData = xxxx_x;aaN
                    if (semiPositionBits != 0) {
                        long currRelevant = (data & (ALL_ONES >>> (64 - semiPositionBits))) >>> nextReadOffsetBits;
                        // 0000_00aa

                        // 0aaa_0000;
                        long currUsable = currRelevant << prevBits;

                        long toHash = prevRelevant | currUsable;

                        hash = simpleHash(hash, toHash);

                        int newPrevBits = prevBits + (semiPositionBits - nextReadOffsetBits);
                        if (newPrevBits >= 64) {
                            suffix = currRelevant >>> (64 - prevBits);
                            hash = simpleHash(hash, suffix);
                        }
                        else {
                            suffix = toHash;
                        }
                    }
                    else {
                        suffix = prevRelevant;
                        hash = simpleHash(hash, prevRelevant);
                    }

                    prevRelevant = 0;
                    prevBits = 0;

                    stationEnd = position + (semiPositionBits >>> 3);
                    nextReadOffsetBits = semiPositionBits + 8;
                    newLineToken = true;

                    if (nextReadOffsetBits == 64) {
                        nextReadOffsetBits = 0;
                        position += 8;
                        data = UNSAFE.getLong(position);
                    }
                }
            }
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
            // System.out.println("Small worker!");
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
        private final long mappedAddress;

        DiskFileService(String fileName) throws IOException {
            FileChannel fileChannel = FileChannel.open(Path.of(fileName),
                    StandardOpenOption.READ);
            this.fileSize = fileChannel.size();
            this.mappedAddress = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0,
                    fileSize, Arena.global()).address();
        }

        @Override
        public long length() {
            return fileSize;
        }

        @Override
        public long address() {
            return mappedAddress;
        }
    }

    private static class ChunkProcessorImpl implements MapReduce<PrimitiveHashMap> {

        // 1 << 14 > 10,000 so it works
        private final PrimitiveHashMap statistics = new PrimitiveHashMap(14);

        @Override
        public void process(long keyStartAddress, long keyEndAddress, long hash, long suffix, int temperature) {
            HashEntry entry = statistics.find(keyStartAddress, keyEndAddress, hash, suffix);
            if (entry == null) {
                throw new IllegalStateException("Hash table too small :(");
            }
            if (entry.value == null) {
                entry.value = new IntSummaryStatistics();
            }
            entry.value.accept(temperature);
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
            for (HashEntry entry : map.entrySet()) {
                if (entry.value != null) {
                    String keyStr = unsafeToString(entry.startAddress, entry.endAddress);

                    output.compute(keyStr, (ignore, val) -> {
                        if (val == null) {
                            return entry.value;
                        }
                        else {
                            val.combine(entry.value);
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
