package org.dpdns.zerodep.ducklake.sink;

/** DuckDB information_schema 类型名的轻量归一化工具。 */
public final class DuckType {

    private DuckType() {
    }

    /**
     * 将 information_schema.columns.data_type 归一为原生 reader 使用的类型名。
     * LIST 列递归归一元素类型，避免 TIMESTAMP WITH TIME ZONE[] 与 TIMESTAMPTZ[] 误判漂移。
     */
    public static String normalize(String infoSchemaType) {
        String type = infoSchemaType == null ? "" : infoSchemaType.toUpperCase();
        if (type.endsWith("[]")) {
            return normalize(type.substring(0, type.length() - 2)) + "[]";
        }
        return switch (type) {
            case "TIMESTAMP WITH TIME ZONE" -> "TIMESTAMPTZ";
            case "TIME WITH TIME ZONE" -> "TIMETZ";
            default -> type;
        };
    }
}
