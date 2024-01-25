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
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.IntStream;

public class CalculateAverage_idegtiarenko {

    private static final String FILE = "./measurements.txt";
    private static final int MARGIN = 128;//margin to read a record if interrupted by a chunk position

    public static void main(String[] args) throws IOException {

        try (var channel = new RandomAccessFile(FILE, "r").getChannel()) {
            final var fileSize = channel.size();
            final var processors = Runtime.getRuntime().availableProcessors();
            final var maxChunkSize = Integer.MAX_VALUE - MARGIN;//mmap size limit
            final var chunks = Math.toIntExact(Math.max(processors, fileSize / maxChunkSize));
            final var chunkSize = Math.ceilDiv(fileSize, chunks);

            var measurements = new AtomicReferenceArray<SimpleMap>(chunks);
            IntStream.range(0, chunks).parallel().forEach(chunk -> {
                try {
                    var localMeasurements = new SimpleMap(4096);
                    var reader = new BufferReader(channel, chunkSize, fileSize, chunk);
                    if (chunk != 0) {
                        reader.skipTo((byte) '\n');
                    }
                    while (reader.available()) {
                        var station = reader.readStationBefore((byte) ';');
                        var measurement = reader.readMeasurementBefore((byte) '\n');
                        localMeasurements.computeIfAbsent(station, ignored -> new Aggregator()).add(measurement);
                    }
                    measurements.set(chunk, localMeasurements);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            System.out.println(merge(measurements));
        }
    }

    private static final class BufferReader {

        private final MappedByteBuffer buffer;
        private final int limit;
        private int p = 0;

        public BufferReader(FileChannel channel, long chunkSize, long fileSize, int chunk) throws IOException {
            long start = chunk * chunkSize;
            int length = Math.toIntExact(Math.min(chunkSize + MARGIN, fileSize - start));
            this.buffer = channel.map(FileChannel.MapMode.READ_ONLY, start, length);
            this.limit = Math.toIntExact(Math.min(chunkSize, fileSize - start));
        }

        public boolean available() {
            return p < buffer.limit() && p <= limit;
        }

        public void skipTo(byte delimiter) {
            while (buffer.get(p) != delimiter) {
                p++;
            }
            p++;// skip delimiter
        }

        public Station readStationBefore(byte delimiter) {
            int from = p;
            skipTo(delimiter);
            int to = p - 1;
            var buf = new byte[to - from];
            buffer.get(from, buf, 0, to - from);
            return new Station(buf, Arrays.hashCode(buf));
        }

        public int readMeasurementBefore(byte delimiter) {
            boolean positive = true;
            int value = 0;
            if (buffer.get(p) == (byte) '-') {
                positive = false;
                p++;
            }
            while (true) {
                byte b = buffer.get(p);
                p++;
                if (b == delimiter) {
                    break;
                } else if (b == '.') {
                    continue;
                } else {
                    value = 10 * value + (b - (byte) '0');
                }
            }
            return positive ? value : (short) -value;
        }
    }

    private static Map<String, Aggregator> merge(AtomicReferenceArray<SimpleMap> results) {
        var result = new TreeMap<String, Aggregator>();
        for (int i = 0; i < results.length(); i++) {
            results.get(i).forEach((key, value) -> {
                result.merge(key.toString(), value, Aggregator::merge);
            });
        }
        return result;
    }

    private static final class Station {
        private final byte[] bytes;
        private final int hashCode;

        public Station(byte[] bytes, int hashCode) {
            this.bytes = bytes;
            this.hashCode = hashCode;
        }

        public static boolean equals(Station a, Station b) {
            return Arrays.equals(a.bytes, b.bytes);
        }

        @Override
        public String toString() {
            return new String(bytes);
        }
    }

    private static final class Aggregator {
        private int min = Short.MAX_VALUE;
        private int max = Short.MIN_VALUE;
        private long sum = 0;
        private int total = 0;

        public static Aggregator merge(Aggregator a, Aggregator b) {
            var result = new Aggregator();
            result.min = Math.min(a.min, b.min);
            result.max = Math.max(a.max, b.max);
            result.sum = a.sum + b.sum;
            result.total = a.total + b.total;
            return result;
        }

        public void add(int value) {
            if (value < min) {
                min = value;
            }
            if (value > max) {
                max = value;
            }
            sum += value;
            total++;
        }

        public String toString() {
            return round(min) + "/" + round(1.0 * sum / total) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value) / 10.0;
        }
    }

    private static final class SimpleMap {
        private final int size;
        private final Station[] keys;
        private final Aggregator[] values;

        public SimpleMap(int size) {
            this.size = size;
            this.keys = new Station[size];
            this.values = new Aggregator[size];
        }

        public Aggregator computeIfAbsent(Station key, Function<Station, Aggregator> mappingFunction) {
            var cell = key.hashCode & (size - 1);
            Station found = null;
            while ((found = keys[cell]) != null && !Station.equals(key, found)) {
                cell++;
            }
            if (found == null) {
                keys[cell] = key;
                values[cell] = mappingFunction.apply(key);
            }
            return values[cell];
        }

        public void forEach(BiConsumer<Station, Aggregator> consumer) {
            for (int i = 0; i < size; i++) {
                if (keys[i] != null) {
                    consumer.accept(keys[i], values[i]);
                }
            }
        }
    }
}
