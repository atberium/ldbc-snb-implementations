package com.jackwaudby.ldbcimplementations;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum Index {
    PLACE("Place"),
    COMMENT("Comment"),
    ORGANIZATION("Organisation"),
    FORUM("Forum"),
    PERSON("Person"),
    POST("Post"),
    TAG("Tag"),
    TAG_CLASS("TagClass");

    private final String label;
}
