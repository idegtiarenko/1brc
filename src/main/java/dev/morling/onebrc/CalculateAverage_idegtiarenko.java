package dev.morling.onebrc;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

public class CalculateAverage_idegtiarenko {

    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws IOException {

        var size = new File(FILE).length();
        var chunks = Runtime.getRuntime().availableProcessors();
        var chunkSize = size / chunks;

        var measurements = new ConcurrentHashMap<String, Aggregator>(1024);

        IntStream.range(0, chunks).parallel().forEach(chunk -> {
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
                        aggregation[0] = measurements.computeIfAbsent(new String(data, from, to - from), ignored -> new Aggregator());
                    });
                    is.readUpTo((byte) '\n', (data, from, to) -> {
                        aggregation[0].add(parse(data, from, to));
                    });
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        System.out.println(new TreeMap<>(measurements));
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

        private static final int BUFFER_SIZE = 1024*1024;

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
            } else {
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

        public synchronized void add(int value) {
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
