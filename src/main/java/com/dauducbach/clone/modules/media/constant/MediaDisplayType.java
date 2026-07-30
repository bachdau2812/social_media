package com.dauducbach.clone.modules.media.constant;

import java.util.ArrayList;
import java.util.List;

public enum MediaDisplayType {
    FEED(1100, 1956, "fit", null),
    POST(1440, 1800, "limit", null),
    COMMENT(720, 900, "fit", null),
    SEARCH_THUMBNAIL(480, 360, "fill", "auto"),
    STORY(1080, 1920, "fill", "auto"),
    AVATAR(256, 256, "fill", "face");

    private final int width;
    private final int height;
    private final String crop;
    private final String gravity;

    MediaDisplayType(int width, int height, String crop, String gravity) {
        this.width = width;
        this.height = height;
        this.crop = crop;
        this.gravity = gravity;
    }

    public int width() {
        return width;
    }

    public int height() {
        return height;
    }

    public List<String> transformations() {
        List<String> directives = new ArrayList<>();
        directives.add("c_" + crop);
        if (gravity != null) {
            directives.add("g_" + gravity);
        }
        directives.add("w_" + width);
        directives.add("h_" + height);
        directives.add("q_auto:good");
        directives.add("f_auto");
        return List.copyOf(directives);
    }
}