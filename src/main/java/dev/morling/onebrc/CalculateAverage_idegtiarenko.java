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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.IntStream;

public class CalculateAverage_idegtiarenko {

    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws IOException {

        var size = new File(FILE).length();
        var chunks = Runtime.getRuntime().availableProcessors();
        var chunkSize = size / chunks;
        var measurements = new AtomicReferenceArray<Map<String, Aggregator>>(chunks);

        IntStream.range(0, chunks).parallel().forEach(chunk -> {
            var m = new HashMap<String, Aggregator>(1024);
            try (var is = new ChunkedInputStream(new BufferedInputStream(new FileInputStream(FILE), 10 * 1024 * 1024))) {
                is.setRange(chunk * chunkSize, (chunk + 1) * chunkSize);
                if (chunk != 0) {
                    is.readUpTo((byte) '\n', (data, from, to) -> {
                        // skip to the first starting position
                    });
                }
                Aggregator[] aggregation = new Aggregator[1];
                while (is.available()) {
                    is.readUpTo((byte) ';', (data, from, to) -> {
                        aggregation[0] = m.computeIfAbsent(new String(data, from, to - from), ignored -> new Aggregator());
                    });
                    is.readUpTo((byte) '\n', (data, from, to) -> {
                        aggregation[0].add(parse(data, from, to));
                    });
                }
                measurements.set(chunk, m);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        System.out.println(merge(measurements));
    }

    private static Map<String, Aggregator> merge(AtomicReferenceArray<Map<String, Aggregator>> results) {
        var result = new TreeMap<String, Aggregator>();
        for (int i = 0; i < results.length(); i++) {
            for (var entry : results.get(i).entrySet()) {
                result.merge(entry.getKey(), entry.getValue(), Aggregator::merge);
            }
        }
        return result;
    }

    private static int parse(byte[] data, int from, int to) {
        boolean negative = false;
        int p = from;
        int value = 0;
        if (data[from] == (byte) '-') {
            p++;
            negative = true;
        }
        while (p < to) {
            if (data[p] != '.') {
                value = 10 * value + (data[p] - (byte) '0');
            }
            p++;
        }
        return negative ? -value : value;
    }

    private static int toInt(long value) {
        return (int) value;
    }

    private static final class ChunkedInputStream implements AutoCloseable {

        private static final int BUFFER_SIZE = 1024 * 1024;

        private final InputStream is;

        private final byte[] b2 = new byte[BUFFER_SIZE];
        private final byte[] b1 = new byte[BUFFER_SIZE];

        private boolean buf = false;

        private long offset = 0;
        private int size = 0;
        private long position;
        private long limit = Long.MAX_VALUE;

        private boolean available = true;

        public ChunkedInputStream(InputStream is) {
            this.is = is;
        }

        public void setRange(long from, long to) throws IOException {
            is.skip(from);
            offset = from;
            position = from;
            limit = to;
        }

        public boolean available() {
            return available && position < limit;
        }

        private byte[] buffer() {
            return buf ? b1 : b2;
        }

        private byte[] previous() {
            return !buf ? b1 : b2;
        }

        private void readNextChunk() throws IOException {
            buf = !buf;
            size = is.read(buffer());
            available = buffer().length == size;
            offset = position;
        }

        public void readUpTo(byte b, DataConsumer consumer) throws IOException {
            long start = position;
            int p = toInt(position - offset);
            while (true) {
                if (p >= size) {
                    readNextChunk();
                    p = 0;
                }

                if (buffer()[p] == b) {
                    break;
                }
                p++;
                position++;
            }
            long end = position;

            if (start >= offset) {
                consumer.consume(buffer(), toInt(start - offset), toInt(end - offset));
            }
            else {
                var data = new byte[toInt(end - start)];
                int sizeInNew = toInt(position - offset);
                int sizeInPrevious = data.length - sizeInNew;

                System.arraycopy(previous(), previous().length - sizeInPrevious, data, 0, sizeInPrevious);
                System.arraycopy(buffer(), 0, data, sizeInPrevious, sizeInNew);
                consumer.consume(data, 0, data.length);
            }
            position++;
        }

        @Override
        public void close() throws IOException {
            is.close();
        }
    }

    @FunctionalInterface
    private interface DataConsumer {
        void consume(byte[] data, int from, int to);
    }

    private static class Aggregator {
        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;
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
}
