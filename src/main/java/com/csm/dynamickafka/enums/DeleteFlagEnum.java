package com.csm.dynamickafka.enums;

import lombok.Getter;

@Getter
public enum DeleteFlagEnum {
    ENABLE(0, "正常"),
    DISABLE(1, "删除");

    private final Integer code;
    private final String desc;

    DeleteFlagEnum(Integer code, String desc) {
        this.code = code;
        this.desc = desc;
    }
}