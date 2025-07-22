package com.hmdp.dto;

import lombok.Data;

import java.util.List;

@Data
public class ScrollResult {
    private List<?> list;   //不知类型，给泛型
    private Long minTime;
    private Integer offset;
}
