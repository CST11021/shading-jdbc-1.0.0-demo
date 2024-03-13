package com.whz.shardingjdbc.demo;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author 盖伦
 * @Date 2024/3/12
 */
public class ResultSetUtil {

    public static List showResult(ResultSet rs) throws SQLException {

        // 列数
        int numFields = rs.getMetaData().getColumnCount();

        // 字段名
        String[] fieldNames = new String[numFields];
        for (int i = 0; i < numFields; i++) {
            fieldNames[i] = rs.getMetaData().getColumnLabel(i + 1);
        }


        List rows = new ArrayList();
        // 遍历结果
        int rowCount = 1;
        while (rs.next()) {
            Map<String, Object> row = new HashMap<>();

            row.put("序号", rowCount);
            for (int i = 0; i < numFields; i++) {

                Object value = rs.getObject(i + 1);
                if (value != null) {
                    row.put(fieldNames[i], rs.getObject(i + 1));
                }
            }

            rowCount++;
            rows.add(row);
        }

        return rows;
    }

}
