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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class CalculateAverage_vaidhy<T> {

    private static final String FILE = "./measurements.txt";

    private final FileService fileService;
    private final Supplier<MapReduce<T>> chunkProcessCreator;
    private final Function<List<T>, T> reducer;

    interface MapReduce<T> extends Consumer<EfficientString> {
        T result();
    }

    interface FileService {
        /**
         * Returns the size of the file in number of characters.
         * (Extra credit: assume byte size instead of char size)
         */
        // Possible implementation for byte case in HTTP:
        // byte size = Content-Length header. using HEAD or empty Range.
        long length();

        /**
         * Returns substring of the file from character indices.
         * Expects 0 <= start <= start + length <= fileSize
         * (Extra credit: assume sub-byte array instead of sub char array)
         */
        // Possible implementation for byte case in HTTP:
        // Using Http Request header "Range", typically used for continuing
        // partial downloads.
        byte[] range(long offset, int length);
    }

    public CalculateAverage_vaidhy(FileService fileService,
                                   Supplier<MapReduce<T>> mapReduce,
                                   Function<List<T>, T> reducer) {
        this.fileService = fileService;
        this.chunkProcessCreator = mapReduce;
        this.reducer = reducer;
    }

    /// SAMPLE CANDIDATE CODE STARTS

    /**
     * Reads from a given offset till the end, it calls server in
     * blocks of scanSize whenever cursor passes the current block.
     * Typically when hasNext() is called. hasNext() is efficient
     * in the sense calling second time is cheap if next() is not
     * called in between. Cheap in the sense no call to server is
     * made.
     */
    // Space complexity = O(scanSize)
    static class ByteStream {

        private final FileService fileService;
        private long offset;
        private final long scanSize;
        private int index = 0;
        private byte[] currentChunk = new byte[0];
        private final long fileLength;

        public ByteStream(FileService fileService, long offset, int scanSize) {
            this.fileService = fileService;
            this.offset = offset;
            this.scanSize = scanSize;
            this.fileLength = fileService.length();
            if (scanSize <= 0) {
                throw new IllegalArgumentException("scan size must be > 0");
            }
            if (offset < 0) {
                throw new IllegalArgumentException("offset must be >= 0");
            }
        }

        public boolean hasNext() {
            while (index >= currentChunk.length) {
                if (offset < fileLength) {
                    int scanWindow = (int) (Math.min(offset + scanSize, fileLength) - offset);
                    currentChunk = fileService.range(offset, scanWindow);
                    offset += scanWindow;
                    index = 0;
                }
                else {
                    return false;
                }
            }
            return true;
        }

        public byte next() {
            return currentChunk[index++];
        }
    }

    /**
     * Reads lines from a given character stream, hasNext() is always
     * efficient, all work is done only in next().
     */
    // Space complexity: O(max line length) in next() call, structure is O(1)
    // not counting charStream as it is only a reference, we will count that
    // in worker space.
    static class LineStream implements Iterator<EfficientString> {
        private final ByteStream byteStream;
        private int readIndex;
        private final long length;

        public LineStream(ByteStream byteStream, long length) {
            this.byteStream = byteStream;
            this.readIndex = 0;
            this.length = length;
        }

        @Override
        public boolean hasNext() {
            return readIndex <= length && byteStream.hasNext();
        }

        @Override
        public EfficientString next() {
            byte[] line = new byte[128];
            int i = 0;
            while (byteStream.hasNext()) {
                byte ch = byteStream.next();
                readIndex++;
                if (ch == 0x0a) {
                    break;
                }
                line[i++] = ch;
            }
            return new EfficientString(line, i);
        }
    }

    // Space complexity: O(scanSize) + O(max line length)
    public void worker(long offset, long chunkSize, int scanSize, Consumer<EfficientString> lineConsumer) {
        ByteStream byteStream = new ByteStream(fileService, offset, scanSize);
        Iterator<EfficientString> lineStream = new LineStream(byteStream, chunkSize);

        if (offset != 0) {
            if (lineStream.hasNext()) {
                // Skip the first line.
                lineStream.next();
            }
            else {
                // No lines then do nothing.
                return;
            }
        }
        while (lineStream.hasNext()) {
            lineConsumer.accept(lineStream.next());
        }
    }

    // Space complexity: O(number of workers), not counting
    // workers space assuming they are running in different hosts.
    public T master(long chunkSize, int scanSize) {
        long len = fileService.length();
        List<ForkJoinTask<T>> summaries = new ArrayList<>();
        ForkJoinPool commonPool = ForkJoinPool.commonPool();

        for (long offset = 0; offset < len; offset += chunkSize) {
            long workerLength = Math.min(len, offset + chunkSize) - offset;
            MapReduce<T> mr = chunkProcessCreator.get();
            final long transferOffset = offset;
            ForkJoinTask<T> task = commonPool.submit(() -> {
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

        DiskFileService(String fileName) throws IOException {
            this.fileChannel = FileChannel.open(Path.of(fileName),
                    StandardOpenOption.READ);
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
        public byte[] range(long offset, int length) {
            byte[] newArr = new byte[length];
            ByteBuffer outputBuffer = ByteBuffer.wrap(newArr);
            try {
                fileChannel.transferTo(offset, length, new WritableByteChannel() {
                    @Override
                    public int write(ByteBuffer src) {
                        int rem = src.remaining();
                        outputBuffer.put(src);
                        return rem;
                    }

                    @Override
                    public boolean isOpen() {
                        return true;
                    }

                    @Override
                    public void close() {
                    }
                });
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            return newArr;
        }
    }

    public static final int DEFAULT_SEED = 104729;
    private static final int C1_32 = 0xcc9e2d51;
    private static final int C2_32 = 0x1b873593;
    private static final int R1_32 = 15;
    private static final int R2_32 = 13;
    private static final int M_32 = 5;
    private static final int N_32 = 0xe6546b64;

    record EfficientString(byte[] arr, int length) {

        /**
         * Gets the little-endian int from 4 bytes starting at the specified index.
         *
         * @param data The data
         * @param index The index
         * @return The little-endian int
         */
        private static int getLittleEndianInt(final byte[] data, final int index) {
            return ((data[index    ] & 0xff)      ) |
                    ((data[index + 1] & 0xff) <<  8) |
                    ((data[index + 2] & 0xff) << 16) |
                    ((data[index + 3] & 0xff) << 24);
        }

        /**
         * Performs the intermediate mix step of the 32-bit hash function {@code MurmurHash3_x86_32}.
         *
         * @param k The data to add to the hash
         * @param hash The current hash
         * @return The new hash
         */
        private static int mix32(int k, int hash) {
            k *= C1_32;
            k = Integer.rotateLeft(k, R1_32);
            k *= C2_32;
            hash ^= k;
            return Integer.rotateLeft(hash, R2_32) * M_32 + N_32;
        }

        /**
         * Performs the final avalanche mix step of the 32-bit hash function {@code MurmurHash3_x86_32}.
         *
         * @param hash The current hash
         * @return The final hash
         */
        private static int fmix32(int hash) {
            hash ^= (hash >>> 16);
            hash *= 0x85ebca6b;
            hash ^= (hash >>> 13);
            hash *= 0xc2b2ae35;
            hash ^= (hash >>> 16);
            return hash;
        }

        @Override
        public int hashCode() {
            int hash = DEFAULT_SEED;
            final int nblocks = length >> 2;

            // body
            for (int i = 0; i < nblocks; i++) {
                final int index = (i << 2);
                final int k = getLittleEndianInt(arr, index);
                hash = mix32(k, hash);
            }

            // tail
            // ************
            // Note: This fails to apply masking using 0xff to the 3 remaining bytes.
            // ************
            final int index = (nblocks << 2);
            int k1 = 0;
            switch (length - index) {
                case 3:
                    k1 ^= arr[index + 2] << 16;
                case 2:
                    k1 ^= arr[index + 1] << 8;
                case 1:
                    k1 ^= arr[index];

                    // mix functions
                    k1 *= C1_32;
                    k1 = Integer.rotateLeft(k1, R1_32);
                    k1 *= C2_32;
                    hash ^= k1;
            }

            hash ^= length;
            return fmix32(hash);        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            EfficientString eso = (EfficientString) o;
            return Arrays.equals(this.arr, 0, this.length,
                    eso.arr, 0, eso.length);
        }
    }

    private static final EfficientString EMPTY = new EfficientString(new byte[0], 0);

    public static class ChunkProcessorImpl implements MapReduce<Map<EfficientString, IntSummaryStatistics>> {

        private final Map<EfficientString, IntSummaryStatistics> statistics = new HashMap<>(10000);

        @Override
        public void accept(EfficientString line) {
            EfficientString station = readStation(line);

            int normalized = parseDoubleNew(line.arr,
                    station.length + 1, line.length);

            updateStats(station, normalized);
        }

        private void updateStats(EfficientString station, int normalized) {
            IntSummaryStatistics stats = statistics.get(station);
            if (stats == null) {
                stats = new IntSummaryStatistics();
                statistics.put(station, stats);
            }
            stats.accept(normalized);
        }

        private static EfficientString readStation(EfficientString line) {
            for (int i = 0; i < line.length; i++) {
                if (line.arr[i] == ';') {
                    return new EfficientString(line.arr, i);
                }
            }
            return EMPTY;
        }

        private static int parseDoubleNew(byte[] value, int offset, int length) {
            int normalized = 0;
            int index = offset;
            boolean sign = true;
            if (value[index] == '-') {
                index++;
                sign = false;
            }
            // boolean hasDot = false;
            for (; index < length; index++) {
                byte ch = value[index];
                if (ch != '.') {
                    normalized = normalized * 10 + (ch - '0');
                }
                // else {
                // hasDot = true;
                // }
            }
            // if (!hasDot) {
            // normalized *= 10;
            // }
            if (!sign) {
                normalized = -normalized;
            }
            return normalized;
        }

        @Override
        public Map<EfficientString, IntSummaryStatistics> result() {
            return statistics;
        }
    }

    public static void main(String[] args) throws IOException {
        DiskFileService diskFileService = new DiskFileService(FILE);

        CalculateAverage_vaidhy<Map<EfficientString, IntSummaryStatistics>> calculateAverageVaidhy = new CalculateAverage_vaidhy<>(
                diskFileService,
                ChunkProcessorImpl::new,
                CalculateAverage_vaidhy::combineOutputs);

        int proc = 2 * ForkJoinPool.commonPool().getParallelism();
        long fileSize = diskFileService.length();
        long chunkSize = Math.ceilDiv(fileSize, proc);
        Map<EfficientString, IntSummaryStatistics> output = calculateAverageVaidhy.master(chunkSize,
                Math.min(10 * 1024 * 1024, (int) chunkSize));
        Map<String, String> outputStr = toPrintMap(output);
        System.out.println(outputStr);
    }

    private static Map<String, String> toPrintMap(Map<EfficientString, IntSummaryStatistics> output) {

        Map<String, String> outputStr = new TreeMap<>();
        for (Map.Entry<EfficientString, IntSummaryStatistics> entry : output.entrySet()) {
            IntSummaryStatistics stat = entry.getValue();
            outputStr.put(new String(
                    Arrays.copyOf(entry.getKey().arr, entry.getKey().length), StandardCharsets.UTF_8),
                    (stat.getMin() / 10.0) + "/" +
                            (Math.round(stat.getAverage()) / 10.0) + "/" +
                            (stat.getMax() / 10.0));
        }
        return outputStr;
    }

    private static Map<EfficientString, IntSummaryStatistics> combineOutputs(List<Map<EfficientString, IntSummaryStatistics>> list) {
        Map<EfficientString, IntSummaryStatistics> output = new HashMap<>(10000);
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
}
