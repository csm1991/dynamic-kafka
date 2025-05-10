package com.csm.dynamickafka.enums;

import lombok.Getter;

/**
 * 逻辑删除枚举类
 *
 * @author Simon Cai
 * @version 1.0
 * @since 2025-05-10
 */
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