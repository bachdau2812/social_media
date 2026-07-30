package com.dauducbach.clone.modules.post.constant;

import java.util.Arrays;

/**
 * Post media ratios are stored as horizontal:vertical (width:height).
 */
public enum PostMediaRatio {
    SQUARE("1:1"),
    PORTRAIT_4_5("4:5"),
    PORTRAIT_3_4("3:4"),
    PORTRAIT_9_16("9:16"),
    LANDSCAPE_4_3("4:3"),
    LANDSCAPE_3_2("3:2"),
    LANDSCAPE_16_9("16:9");

    public static final String DEFAULT_VALUE = "4:5";

    private final String value;

    PostMediaRatio(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    public static boolean isSupported(String value) {
        return value != null && Arrays.stream(values())
                .anyMatch(ratio -> ratio.value.equals(value.trim()));
    }

    public static String defaultIfMissing(String value) {
        return isSupported(value) ? value.trim() : DEFAULT_VALUE;
    }
}