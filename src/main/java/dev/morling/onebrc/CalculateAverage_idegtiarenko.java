package dev.morling.onebrc;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.TreeMap;

public class CalculateAverage_idegtiarenko {

    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws IOException {

        var measurements = new HashMap<String, Aggregator>(1000);

        try (var reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(new FileInputStream(FILE), 10 * 1024 * 1024)))) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                int split = line.lastIndexOf(';');
                var station = line.substring(0, split);
                var value = Double.parseDouble(line.substring(split + 1));
                measurements.computeIfAbsent(station, k -> new Aggregator()).add(value);
            }
        }

        System.out.println(new TreeMap<>(measurements));
    }

    private static class Aggregator {
        private double min = Double.MAX_VALUE;
        private double max = Double.MIN_VALUE;
        private double sum = 0;
        private int total = 0;

        public void add(double value) {
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
            return round(min) + "/" + round(sum / total) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }
}
