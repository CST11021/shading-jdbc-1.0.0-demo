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

package com.dangdang.ddframe.rdb.sharding.router;

import com.codahale.metrics.Timer.Context;
import com.dangdang.ddframe.rdb.sharding.api.DatabaseType;
import com.dangdang.ddframe.rdb.sharding.api.rule.ShardingRule;
import com.dangdang.ddframe.rdb.sharding.exception.SQLParserException;
import com.dangdang.ddframe.rdb.sharding.exception.ShardingJdbcException;
import com.dangdang.ddframe.rdb.sharding.metrics.MetricsContext;
import com.dangdang.ddframe.rdb.sharding.parser.SQLParserFactory;
import com.dangdang.ddframe.rdb.sharding.parser.result.SQLParsedResult;
import com.dangdang.ddframe.rdb.sharding.parser.result.router.ConditionContext;
import com.dangdang.ddframe.rdb.sharding.parser.result.router.SQLBuilder;
import com.dangdang.ddframe.rdb.sharding.parser.result.router.Table;
import com.dangdang.ddframe.rdb.sharding.router.binding.BindingTablesRouter;
import com.dangdang.ddframe.rdb.sharding.router.mixed.MixedTablesRouter;
import com.dangdang.ddframe.rdb.sharding.router.single.SingleTableRouter;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.List;

/**
 * SQL路由引擎.
 * 
 * @author gaohongtao, zhangiang
 */
@RequiredArgsConstructor
public final class SQLRouteEngine {

    /** 分片规则 */
    private final ShardingRule shardingRule;

    /** 数据库类型 */
    private final DatabaseType databaseType;
    
    /**
     * SQL路由.
     * 
     * @param logicSql 逻辑SQL
     * @return 路由结果
     * @throws SQLParserException SQL解析失败异常
     */
    public SQLRouteResult route(final String logicSql, final List<Object> parameters) throws SQLParserException {
        return routeSQL(parseSQL(logicSql, parameters));
    }

    /**
     * 解析SQL
     *
     * @param logicSql
     * @param parameters
     * @return
     */
    private SQLParsedResult parseSQL(final String logicSql, final List<Object> parameters) {
        Context context = MetricsContext.start("Parse SQL");
        SQLParsedResult result = SQLParserFactory.create(databaseType, logicSql, parameters, shardingRule.getAllShardingColumns()).parse();
        MetricsContext.stop(context);
        return result;
    }

    /**
     * 将解析结果转为路由结果
     *
     * @param parsedResult
     * @return
     */
    private SQLRouteResult routeSQL(final SQLParsedResult parsedResult) {
        Context context = MetricsContext.start("Route SQL");

        SQLRouteResult result = new SQLRouteResult(parsedResult.getMergeContext());

        // 遍历条件
        for (ConditionContext each : parsedResult.getConditionContexts()) {

            // 获取所有相关的逻辑表的表名
            Collection<String> logicTables = Collections2.transform(parsedResult.getRouteContext().getTables(), new Function<Table, String>() {

                @Override
                public String apply(final Table input) {
                    return input.getName();
                }

            });

            Collection<SQLExecutionUnit> sqlExecutionUnits = routeSQL(each, logicTables, parsedResult.getRouteContext().getSqlBuilder());

            // 添加执行单元
            result.getExecutionUnits().addAll(sqlExecutionUnits);
        }

        MetricsContext.stop(context);
        return result;
    }

    /**
     * 将本次执行的SQL分解为多个执行单元
     *
     * @param conditionContext  本次执行涉及的所有条件
     * @param logicTables       本次执行涉及的所有逻辑表
     * @param sqlBuilder        本次执行的SQL
     * @return
     */
    private Collection<SQLExecutionUnit> routeSQL(final ConditionContext conditionContext, final Collection<String> logicTables, final SQLBuilder sqlBuilder) {
        RoutingResult result;

        // 本次执行的SQL仅涉及一个表
        if (1 == logicTables.size()) {
            result = new SingleTableRouter(shardingRule, logicTables.iterator().next(), conditionContext).route();
        } else if (shardingRule.isAllBindingTable(logicTables)) {
            result = new BindingTablesRouter(shardingRule, logicTables, conditionContext).route();
        } else {
            // TODO 可配置是否执行笛卡尔积
            result = new MixedTablesRouter(shardingRule, logicTables, conditionContext).route();
        }

        if (null == result) {
            throw new ShardingJdbcException("Sharding-JDBC: cannot route any result, please check your sharding rule.");
        }

        return result.getSQLExecutionUnits(sqlBuilder);
    }
}
