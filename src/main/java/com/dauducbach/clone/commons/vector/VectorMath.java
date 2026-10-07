package com.dauducbach.clone.commons.vector;

import java.util.ArrayList;
import java.util.List;

/** The embedding space contract shared by persistence and all vector computations. */
public final class VectorMath {
    public static final String MODEL = "gemini-embedding-2";
    public static final int DIMENSION = 768;
    public static final int SCHEMA_VERSION = 1;

    private VectorMath() {
    }

    /** Unknown legacy metadata must be inventoried/repaired, never inferred from dimension. */
    public static void requireCompatible(String model, Integer dimension, Integer schemaVersion) {
        if (!MODEL.equals(model) || dimension == null || dimension != DIMENSION
                || schemaVersion == null || schemaVersion != SCHEMA_VERSION) {
            throw new IllegalArgumentException("Vector embedding model/dimension/schema metadata is unknown or incompatible");
        }
    }

    public static List<Double> normalize(List<Double> vector) {
        if (vector == null || vector.size() != DIMENSION) {
            throw new IllegalArgumentException("Vector must contain exactly " + DIMENSION + " dimensions");
        }
        double scale = 0;
        for (Double value : vector) {
            if (value == null || !Double.isFinite(value)) {
                throw new IllegalArgumentException("Vector values must be non-null and finite");
            }
            scale = Math.max(scale, Math.abs(value));
        }
        if (scale == 0) {
            throw new IllegalArgumentException("Vector norm must be non-zero");
        }
        // Divide first: squaring raw values overflows/underflows for valid finite input.
        double scaledSquaredNorm = 0;
        for (double value : vector) {
            double scaled = value / scale;
            scaledSquaredNorm += scaled * scaled;
        }
        double scaledNorm = Math.sqrt(scaledSquaredNorm);
        List<Double> result = new ArrayList<>(DIMENSION);
        for (double value : vector) {
            result.add((value / scale) / scaledNorm);
        }
        return result;
    }

    public static List<Double> mix(List<Double> first, double firstWeight, List<Double> second, double secondWeight) {
        if (!Double.isFinite(firstWeight) || !Double.isFinite(secondWeight)
                || firstWeight < 0 || secondWeight < 0 || Math.max(firstWeight, secondWeight) == 0) {
            throw new IllegalArgumentException("Weights must be finite, non-negative and not both zero");
        }
        List<Double> firstUnit = normalize(first);
        List<Double> secondUnit = normalize(second);
        double weightScale = Math.max(firstWeight, secondWeight);
        List<Double> mixed = new ArrayList<>(DIMENSION);
        for (int index = 0; index < DIMENSION; index++) {
            mixed.add(firstUnit.get(index) * (firstWeight / weightScale)
                    + secondUnit.get(index) * (secondWeight / weightScale));
        }
        return normalize(mixed);
    }
}
