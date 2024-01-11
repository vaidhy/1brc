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
import java.lang.foreign.MemorySegment;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class CalculateAverage_vaidhy<T> {

    private static final String FILE = "./measurements.txt";

    private static final Unsafe UNSAFE = initUnsafe();

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

    private final FileService fileService;
    private final Supplier<MapReduce<T>> chunkProcessCreator;
    private final Function<List<T>, T> reducer;

    public interface MapReduce<T> {

        void accept(EfficientString station, int temperature);

        IntSummaryStatistics getSummaryStatistics(EfficientString station);

        T result();
    }

    public interface FileService {
        long length();

        MemorySegment getMemory();
    }

    public CalculateAverage_vaidhy(FileService fileService,
                                   Supplier<MapReduce<T>> mapReduce,
                                   Function<List<T>, T> reducer) {
        this.fileService = fileService;
        this.chunkProcessCreator = mapReduce;
        this.reducer = reducer;
    }

    /*
     * Reads from a given offset till the end, it calls server in
     * blocks of scanSize whenever cursor passes the current block.
     * Typically when hasNext() is called. hasNext() is efficient
     * in the sense calling second time is cheap if next() is not
     * called in between. Cheap in the sense no call to server is
     * made.
     * Space complexity = O(scanSize)
     */

    /**
     * Reads lines from a given character stream, hasNext() is always
     * efficient, all work is done only in next().
     */
    // Space complexity: O(max line length) in next() call, structure is O(1)
    // not counting charStream as it is only a reference, we will count that
    // in worker space.
    static class LineStream {
        private final long chunkEnd;
        private final long fileLength;
        private long offset;
        private final MemorySegment mmapSegment;

        private final byte[] buffer;
        private int bufferIndex;
        private int bufferLimit;
        private final MemorySegment bufferSegment;

        public LineStream(FileService fileService, long offset, long length, int scanSize) {
            this.fileLength = fileService.length();
            this.mmapSegment = fileService.getMemory();
            this.offset = offset;
            this.chunkEnd = offset + length;
            this.buffer = new byte[scanSize];
            this.bufferIndex = 0;
            this.bufferLimit = 0;
            this.bufferSegment = MemorySegment.ofArray(buffer);
        }

        public boolean hasNext() {
            long realOffset = offset + bufferIndex;
            return realOffset <= chunkEnd &&
                    realOffset < fileLength;
        }

        private boolean readNextBuffer() {
            offset += bufferIndex;
            bufferLimit = (int) Math.min(buffer.length, fileLength - offset);
            MemorySegment.copy(mmapSegment, offset,
                    bufferSegment, 0, bufferLimit);
            bufferIndex = 0;
            return bufferLimit != 0;
        }

        EfficientString until(byte ch) {
            int i = bufferIndex;
            int bufferStart = bufferIndex;
            loop: while (true) {
                for (; i < bufferLimit; i++) {
                    if (buffer[i] == ch) {
                        break loop;
                    }
                }
                if (!readNextBuffer()) {
                    break;
                }
                bufferStart = 0;
                i = 0;
            }
            bufferIndex = i + 1;
            return new EfficientString(buffer, bufferStart, i, 0);
        }
    }

    // Space complexity: O(scanSize) + O(max line length)
    public void worker(long offset, long chunkSize, int scanSize, MapReduce<T> lineConsumer) {
        LineStream lineStream = new LineStream(fileService, offset, chunkSize, scanSize);

        if (offset != 0) {
            if (lineStream.hasNext()) {
                // Skip the first line.
                EfficientString skip = lineStream.until((byte) '\n');
                // System.out.println(STR."Skip: \{skip}");
            }
            else {
                // No lines then do nothing.
                return;
            }
        }
        while (lineStream.hasNext()) {
            EfficientString station = lineStream.until((byte) ';');
            EfficientString stationWithHash = station.withHash();
            IntSummaryStatistics stats = lineConsumer.getSummaryStatistics(stationWithHash);
            EfficientString temperature = lineStream.until((byte) '\n');
            stats.accept(parseDouble(temperature));
        }
    }

    // Space complexity: O(number of workers), not counting
    // workers space assuming they are running in different hosts.
    public T master(long chunkSize, int scanSize, ExecutorService executor) {
        long len = fileService.length();
        List<Future<T>> summaries = new ArrayList<>();

        for (long offset = 0; offset < len; offset += chunkSize) {
            long workerLength = Math.min(len, offset + chunkSize) - offset;
            MapReduce<T> mr = chunkProcessCreator.get();
            final long transferOffset = offset;
            Future<T> task = executor.submit(() -> {
                worker(transferOffset, workerLength, scanSize, mr);
                return mr.result();
            });
            summaries.add(task);
        }
        List<T> summariesDone = summaries.stream()
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

    /// SAMPLE CANDIDATE CODE ENDS

    static class DiskFileService implements FileService {

        private final FileChannel fileChannel;
        private final MemorySegment memorySegment;

        DiskFileService(String fileName) throws IOException {
            this.fileChannel = FileChannel.open(Path.of(fileName),
                    StandardOpenOption.READ);
            this.memorySegment = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0,
                    fileChannel.size(), Arena.global());
        }

        @Override
        public long length() {
            try {
                return this.fileChannel.size();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public MemorySegment getMemory() {
            return memorySegment;
        }
    }

    public record EfficientString(byte[] arr, int from, int to, int hash) {

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (o instanceof EfficientString eso) {
                return Arrays.equals(arr, from, to,
                        eso.arr, eso.from, eso.to);
            } else {
                return false;
            }
        }

        @Override
        public String toString() {
            return new String(Arrays.copyOfRange(arr, from, to),
                    StandardCharsets.UTF_8);
        }

        public EfficientString copy() {
            return new EfficientString(Arrays.copyOfRange(arr, from, to),
                    0, to - from, hash);
        }

        public EfficientString withHash() {
            int h = 0;
            for (int i = from; i < to; i++) {
                h = (h * 31) ^ arr[i];
            }
            if (arr[from] == '\n' ) {
                System.out.println("Broken1");
            }
            return new EfficientString(arr, from, to, h);
        }
    }

    private static final EfficientString EMPTY = new EfficientString(new byte[0], 0, 0, 0);

    public static class ChunkProcessorImpl implements MapReduce<Map<EfficientString, IntSummaryStatistics>> {

        private final Map<EfficientString, IntSummaryStatistics> statistics = new HashMap<>(10000);

        public IntSummaryStatistics getSummaryStatistics(EfficientString station) {
            IntSummaryStatistics stats = statistics.get(station);
            if (stats == null) {
                stats = new IntSummaryStatistics();
                EfficientString stationCopy = station.copy();
                if (stationCopy.arr[0] == '\n') {
                    System.out.println("Broekn");
                }
                statistics.put(stationCopy, stats);
            }
            return stats;
        }

        @Override
        public void accept(EfficientString station, int temperature) {
            IntSummaryStatistics stats = statistics.get(station);
            if (stats == null) {
                stats = new IntSummaryStatistics();
                EfficientString stationCopy = station.copy();
                statistics.put(stationCopy, stats);
            }
            stats.accept(temperature);
        }

        @Override
        public Map<EfficientString, IntSummaryStatistics> result() {
            return statistics;
        }
    }

    private static Map<String, String> toPrintMap(Map<EfficientString, IntSummaryStatistics> output) {

        Map<String, String> outputStr = new TreeMap<>();
        for (Map.Entry<EfficientString, IntSummaryStatistics> entry : output.entrySet()) {
            IntSummaryStatistics stat = entry.getValue();
            outputStr.put(entry.getKey().toString(),
                    (stat.getMin() / 10.0) + "/" +
                            (Math.round(stat.getAverage()) / 10.0) + "/" +
                            (stat.getMax() / 10.0));
        }
        return outputStr;
    }

    private static Map<EfficientString, IntSummaryStatistics> combineOutputs(List<Map<EfficientString, IntSummaryStatistics>> list) {
        Map<EfficientString, IntSummaryStatistics> output = HashMap.newHashMap(10000);
        for (Map<EfficientString, IntSummaryStatistics> map : list) {
            for (Map.Entry<EfficientString, IntSummaryStatistics> entry : map.entrySet()) {
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

        return output;
    }

    private static int parseDouble(EfficientString temp) {
        byte[] value = temp.arr;
        int normalized = 0;
        int index = temp.from;
        boolean sign = true;
        if (value[index] == '-') {
            index++;
            sign = false;
        }
        for (; index < temp.to; index++) {
            byte ch = value[index];
            if (ch != '.') {
                normalized = normalized * 10 + (ch - '0');
            }
        }
        if (!sign) {
            normalized = -normalized;
        }
        return normalized;
    }

    public static void main(String[] args) throws IOException {
        DiskFileService diskFileService = new DiskFileService(FILE);

        CalculateAverage_vaidhy<Map<EfficientString, IntSummaryStatistics>> calculateAverageVaidhy = new CalculateAverage_vaidhy<>(
                diskFileService,
                ChunkProcessorImpl::new,
                CalculateAverage_vaidhy::combineOutputs);

        int proc = Runtime.getRuntime().availableProcessors();
        int shards = 2 * proc;
        long fileSize = diskFileService.length();
        long chunkSize = Math.ceilDiv(fileSize, shards);
        int scanSize = (int) Math.max(128, Math.min(10 * 1024 * 1024, chunkSize));

        Map<EfficientString, IntSummaryStatistics> output;

        try (ExecutorService executor = Executors.newFixedThreadPool(proc)) {
            output = calculateAverageVaidhy.master(chunkSize, scanSize, executor);
        }

        Map<String, String> outputStr = toPrintMap(output);
        System.out.println(outputStr);
    }
}
