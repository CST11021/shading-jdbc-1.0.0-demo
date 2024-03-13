/**
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package com.whz.shardingjdbc.demo;

import com.alibaba.fastjson.JSONObject;
import com.dangdang.ddframe.rdb.sharding.api.ShardingDataSource;
import com.dangdang.ddframe.rdb.sharding.api.rule.BindingTableRule;
import com.dangdang.ddframe.rdb.sharding.api.rule.DataSourceRule;
import com.dangdang.ddframe.rdb.sharding.api.rule.ShardingRule;
import com.dangdang.ddframe.rdb.sharding.api.rule.TableRule;
import com.dangdang.ddframe.rdb.sharding.api.strategy.database.DatabaseShardingStrategy;
import com.dangdang.ddframe.rdb.sharding.api.strategy.table.TableShardingStrategy;
import org.apache.commons.dbcp.BasicDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public final class Main {

    public static void main(final String[] args) throws SQLException {

        DataSource dataSource = buildShardingDataSource();

        String sql = "SELECT * FROM t_order";

        // ShardingConnection
        Connection conn = dataSource.getConnection();

        // ShardingPreparedStatement
        PreparedStatement statement = conn.prepareStatement(sql);
        // statement.setInt(1, 1001);

        // 四种结果集: IteratorResultSet OrderByResultSet GroupByResultSet AggregationResultSet
        ResultSet rs = statement.executeQuery();

        System.out.println("测试SQL:" + sql + " 返回:");
        System.out.println(JSONObject.toJSONString(ResultSetUtil.showResult(rs)));

        // testOrderBy(dataSource);
        // 测试select
        // testSelect(dataSource);
        // 测试join
        // testJoin(dataSource);
        // 测试group by
        // testGroupBy(dataSource);
    }

    private static void testOrderBy(final DataSource dataSource) throws SQLException {
        String sql = "SELECT * FROM t_order order by user_id asc order_id asc";

        // ShardingConnection
        Connection conn = dataSource.getConnection();

        // ShardingPreparedStatement
        PreparedStatement statement = conn.prepareStatement(sql);

        // 四种结果集: IteratorResultSet OrderByResultSet GroupByResultSet AggregationResultSet
        ResultSet rs = statement.executeQuery();

        System.out.println("测试SQL:" + sql + " 返回:");
        System.out.println(JSONObject.toJSONString(ResultSetUtil.showResult(rs)));
    }


    private static void testSelect(final DataSource dataSource) throws SQLException {

        String sql = "SELECT * FROM t_order where order_id = ? order by order_id desc ";
        Connection conn = dataSource.getConnection();
        PreparedStatement statement = conn.prepareStatement(sql);
        statement.setInt(1, 1001);
        ResultSet rs = statement.executeQuery();
        System.out.println("测试SQL:" + sql + " 返回:");
        while (rs.next()) {
            System.out.println(rs.getInt(1));
            System.out.println(rs.getInt(2));
            System.out.println(rs.getString(3));
        }
    }

    private static void testJoin(final DataSource dataSource) throws SQLException {

        String sql = "SELECT i.* FROM t_order o JOIN t_order_item i ON o.order_id = i.order_id WHERE o.user_id = ? AND o.order_id = ?";
        Connection conn = dataSource.getConnection();
        PreparedStatement statement = conn.prepareStatement(sql);
        statement.setInt(1, 10);
        statement.setInt(2, 1001);
        ResultSet rs = statement.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getInt(1));
            System.out.println(rs.getInt(2));
            System.out.println(rs.getInt(3));
        }
    }

    private static void testGroupBy(final DataSource dataSource) throws SQLException {
        String sql = "SELECT o.user_id, COUNT(*) FROM t_order o JOIN t_order_item i ON o.order_id=i.order_id GROUP BY o.user_id";
        Connection conn = dataSource.getConnection();
        PreparedStatement pstmt = conn.prepareStatement(sql);
        ResultSet rs = pstmt.executeQuery();
        while (rs.next()) {
            System.out.println("user_id: " + rs.getInt(1) + ", count: " + rs.getInt(2));
        }
    }

    private static ShardingDataSource buildShardingDataSource() throws SQLException {

        // build所有的数据源
        DataSourceRule dataSourceRule = buildDataSourceRule();

        // build所有的表
        TableRule orderTableRule = new TableRule("t_order", Arrays.asList("t_order_0", "t_order_1"), dataSourceRule);
        TableRule orderItemTableRule = new TableRule("t_order_item", Arrays.asList("t_order_item_0", "t_order_item_1"), dataSourceRule);

        // build分片规则
        ShardingRule shardingRule = new ShardingRule(
                dataSourceRule,
                Arrays.asList(orderTableRule, orderItemTableRule),
                Arrays.asList(new BindingTableRule(Arrays.asList(orderTableRule, orderItemTableRule))),
                new DatabaseShardingStrategy("user_id", new ModuloDatabaseShardingAlgorithm()),
                new TableShardingStrategy("order_id", new ModuloTableShardingAlgorithm()));
        return new ShardingDataSource(shardingRule);
    }

    private static DataSourceRule buildDataSourceRule() {
        Map<String, DataSource> map = new HashMap<>(2);
        map.put("ds_0", createDataSource("ds_0", "root", "123456"));
        map.put("ds_1", createDataSource("ds_1", "root", "123456"));

        return new DataSourceRule(map);
    }

    private static DataSource createDataSource(final String dataSourceName, String username, String password) {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName(com.mysql.jdbc.Driver.class.getName());
        dataSource.setUrl(String.format("jdbc:mysql://localhost:3306/%s", dataSourceName));
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        return dataSource;
    }
}
