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
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.reflect.Array;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CalculateAverage_anitasv {

    private static final String FILE = "./measurements.txt";

    private static class LongHashEntry<T> {
        long key;
        T value;
        int next;
    }

    private static class LongHashMap<T> {
        private final LongHashEntry<T>[] entries;
        private int next = -1;

        @SuppressWarnings("unchecked")
        private LongHashMap(int capacity) {
            this.entries = (LongHashEntry<T>[]) new LongHashEntry[capacity];
            for (int i = 0; i < entries.length; i++) {
                this.entries[i] = new LongHashEntry<>();
            }
        }

        public LongHashEntry<T> find(long key) {
            int start = Math.floorMod(key, entries.length);
            int index = start;
            do {
                LongHashEntry<T> entry = entries[index];
                if (entry.key == key) {
                    return entry;
                }
                else if (entry.value == null) {
                    entry.key = key;
                    entry.next = next;
                    next = index;
                    return entry;
                }
                index++;
                if (index == entries.length) {
                    index = 0;
                }
            } while (index != start);
            return null;
        }

        public List<T> values() {
            List<T> values = new ArrayList<>();
            int scan = next;
            while (scan != -1) {
                LongHashEntry<T> entry = entries[scan];
                values.add(entry.value);
                scan = entry.next;
            }
            return values;
        }
    }

    private record Shard(MemorySegment mmapMemory,
                         long chunkStart, long chunkEnd) {

        byte get(long address) {
            return mmapMemory.get(ValueLayout.JAVA_BYTE, address);
        }

        long indexOf(long position, byte ch) {
            long len = mmapMemory.byteSize();
            for (; position < len; position++) {
                byte mCh = get(position);
                if (mCh == ch) {
                    return position;
                }
            }
            return -1;
        }

        byte[] getRange(long start, long end) {
            return mmapMemory.asSlice(start, end - start).toArray(ValueLayout.JAVA_BYTE);
        }

        int parseDouble(long start, long end) {
            int normalized = 0;
            boolean sign = true;
            long index = start;
            if (get(index) == '-') {
                index++;
                sign = false;
            }
            boolean hasDot = false;
            for (; index < end; index++) {
                byte ch = get(index);
                if (ch != '.') {
                    normalized = normalized * 10 + (ch - '0');
                } else {
                    hasDot = true;
                }
            }
            if (!hasDot) {
                normalized *= 10;
            }
            if (!sign) {
                normalized = -normalized;
            }
            return normalized;
        }

        public long computeHash(long position, long stationEnd) {
            long hash = 0;
            for (long index = position; index < stationEnd; index++) {
                hash = hash * 31 + get(index);
            }
            return hash;
        }

        public boolean matches(byte[] existingStation, long start, long end) {
            if (existingStation.length != (end - start)) {
                return false;
            }
            for (int i = 0; i < existingStation.length; i++) {
                if (existingStation[i] != get(start + i)) {
                    return false;
                }
            }
            return true;
        }
    }

    private record ResultRow(byte[] station, IntSummaryStatistics statistics) {

        public String toString() {
            return STR."\{new String(station, StandardCharsets.UTF_8)} : \{statToString(statistics)}";
        }
    }

    private static Map<String, IntSummaryStatistics> process(Shard shard) {
        LongHashMap<List<ResultRow>> result = new LongHashMap<>(1 << 14);

        boolean skip = shard.chunkStart != 0;
        for (long position = shard.chunkStart; position < shard.chunkEnd; position++) {
            if (skip) {
                position = shard.indexOf(position, (byte) '\n');
                skip = false;
            }
            else {
                long stationEnd = shard.indexOf(position, (byte) ';');
                long hash = shard.computeHash(position, stationEnd);

                long temperatureEnd = shard.indexOf(stationEnd + 1, (byte) '\n');
                int temperature = shard.parseDouble(stationEnd + 1, temperatureEnd);

                LongHashEntry<List<ResultRow>> entry = result.find(hash);
                if (entry == null) {
                    throw new IllegalStateException("Not enough space in hashmap.");
                }
                List<ResultRow> matches = entry.value;
                if (matches == null) {
                    matches = new ArrayList<>();
                    entry.value = matches;
                }

                boolean found = false;
                for (ResultRow existing : matches) {
                    byte[] existingStation = existing.station();
                    if (shard.matches(existingStation, position, stationEnd)) {
                        existing.statistics.accept(temperature);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    IntSummaryStatistics stats = new IntSummaryStatistics();
                    stats.accept(temperature);
                    ResultRow rr = new ResultRow(shard.getRange(position, stationEnd), stats);
                    matches.add(rr);
                }
                position = temperatureEnd;
            }
        }

        return result.values()
                .stream()
                .flatMap(Collection::stream)
                .map(rr -> new AbstractMap.SimpleImmutableEntry<>(
                        new String(rr.station, StandardCharsets.UTF_8),
                        rr.statistics))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static Map<String, IntSummaryStatistics> combineResults(List<Map<String, IntSummaryStatistics>> list) {

        Map<String, IntSummaryStatistics> output = HashMap.newHashMap(1024);
        for (Map<String, IntSummaryStatistics> map : list) {
            for (Map.Entry<String, IntSummaryStatistics> entry : map.entrySet()) {
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

    private static Map<String, IntSummaryStatistics> master(MemorySegment mmapMemory) {
        long totalBytes = mmapMemory.byteSize();
        int numWorkers = Runtime.getRuntime().availableProcessors();
        long chunkSize = Math.floorDiv(totalBytes, numWorkers);

        return combineResults(IntStream.range(0, numWorkers)
                .parallel()
                .mapToObj(workerId -> {
                    long chunkStart = workerId * chunkSize;
                    long chunkEnd = Math.min(chunkStart + chunkSize + 1, totalBytes);
                    return new Shard(mmapMemory, chunkStart, chunkEnd);
                })
                .map(CalculateAverage_anitasv::process)
                .toList());
    }

    public static Map<String, IntSummaryStatistics> start() throws IOException {
        try (FileChannel fileChannel = FileChannel.open(Path.of(FILE),
                StandardOpenOption.READ)) {
            long fileSize = fileChannel.size();
            MemorySegment mmapMemory = fileChannel.map(
                    FileChannel.MapMode.READ_ONLY,
                    0, fileSize, Arena.global());
            return master(mmapMemory);
        }
    }

    private static Map<String, String> toPrintMap(Map<String, IntSummaryStatistics> output) {
        Map<String, String> outputStr = new TreeMap<>();
        for (Map.Entry<String, IntSummaryStatistics> entry : output.entrySet()) {
            IntSummaryStatistics stat = entry.getValue();
            outputStr.put(entry.getKey(), statToString(stat));
        }
        System.out.println(outputStr.size());
        return outputStr;
    }

    private static String statToString(IntSummaryStatistics stat) {
        return STR."\{stat.getMin() / 10.0}/\{Math.round(stat.getAverage()) / 10.0}/\{stat.getMax() / 10.0}";
    }

    public static void main(String[] args) throws IOException {
        System.out.println(toPrintMap(start()));
    }
}
