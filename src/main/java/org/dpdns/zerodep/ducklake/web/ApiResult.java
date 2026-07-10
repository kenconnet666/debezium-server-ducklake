package org.dpdns.zerodep.ducklake.web;

import lombok.Data;

import java.io.Serial;
import java.io.Serializable;

/**
 * 统一响应壳（与 zadmin 全家桶的 core ApiResult 字段同构，JSON 输出兼容既有消费者/监控面板）。
 * 2026-07-08 本模块与 core 解耦后本地自持，仅保留本模块用到的 success 工厂。
 */
@Data
public class ApiResult<T> implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String code = "0000";
    private String title = "SUCCESS";
    private T message;
    private Object param;

    /**
     * 成功响应（带数据）
     */
    public static <T> ApiResult<T> success(T data) {
        ApiResult<T> result = new ApiResult<>();
        result.setMessage(data);
        return result;
    }
}
