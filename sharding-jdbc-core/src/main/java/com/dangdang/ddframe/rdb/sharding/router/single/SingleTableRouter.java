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

package com.dangdang.ddframe.rdb.sharding.router.single;

import com.dangdang.ddframe.rdb.sharding.api.ShardingValue;
import com.dangdang.ddframe.rdb.sharding.api.rule.DataNode;
import com.dangdang.ddframe.rdb.sharding.api.rule.ShardingRule;
import com.dangdang.ddframe.rdb.sharding.api.rule.TableRule;
import com.dangdang.ddframe.rdb.sharding.api.strategy.database.DatabaseShardingStrategy;
import com.dangdang.ddframe.rdb.sharding.api.strategy.table.TableShardingStrategy;
import com.dangdang.ddframe.rdb.sharding.parser.result.router.Condition;
import com.dangdang.ddframe.rdb.sharding.parser.result.router.ConditionContext;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/**
 * 单逻辑表的库表路由.
 * 
 * @author gaohongtao, zhangliang
 */
@Slf4j
public final class SingleTableRouter {
    
    /** 逻辑表 */
    private final String logicTable;

    /** 条件上下文 */
    private final ConditionContext conditionContext;

    /** 表规则 */
    private final Optional<TableRule> tableRule;

    /** 分片规则 */
    private final ShardingRule shardingRule;
    
    public SingleTableRouter(final ShardingRule shardingRule, final String logicTable, final ConditionContext conditionContext) {
        this.shardingRule = shardingRule;
        this.logicTable = logicTable;
        this.conditionContext = conditionContext;
        tableRule = shardingRule.findTableRule(logicTable);
    }
    
    /**
     * 路由.
     * 
     * @return 路由结果
     */
    public SingleRoutingResult route() {
        if (!tableRule.isPresent()) {
            log.trace("Can not find table rule of [{}]", logicTable);
            return null;
        }

        Collection<String> routedDataSources = routeDataSources();
        Collection<String> routedTables = routeTables(routedDataSources);

        // 生成路由结果: 计算在哪些数据源上执行哪些真实表
        return generateRoutingResult(routedDataSources, routedTables);
    }
    
    private Collection<String> routeDataSources() {
        DatabaseShardingStrategy strategy = shardingRule.getDatabaseShardingStrategy(tableRule.get());
        List<ShardingValue<?>> databaseShardingValues = getShardingValues(strategy.getShardingColumns());
        logBeforeRoute("database", logicTable, shardingRule.getDataSourceRule().getDataSourceNames(), strategy.getShardingColumns(), databaseShardingValues);
        Collection<String> result = new HashSet<>(strategy.doSharding(shardingRule.getDataSourceRule().getDataSourceNames(), databaseShardingValues));
        logAfterRoute("database", logicTable, result);
        Preconditions.checkState(!result.isEmpty(), "no database route info");
        return result;
    }
    
    private Collection<String> routeTables(final Collection<String> routedDataSources) {
        TableShardingStrategy strategy = shardingRule.getTableShardingStrategy(tableRule.get());
        List<ShardingValue<?>> tableShardingValues = getShardingValues(strategy.getShardingColumns());
        logBeforeRoute("table", logicTable, tableRule.get().getActualTables(), strategy.getShardingColumns(), tableShardingValues);
        Collection<String> result = new HashSet<>(strategy.doSharding(tableRule.get().getActualTableNames(routedDataSources), tableShardingValues));
        logAfterRoute("table", logicTable, result);
        Preconditions.checkState(!result.isEmpty(), "no table route info");
        return result;
    }
    
    private List<ShardingValue<?>> getShardingValues(final Collection<String> shardingColumns) {
        List<ShardingValue<?>> result = new ArrayList<>(shardingColumns.size());
        for (String each : shardingColumns) {
            Optional<Condition> condition = conditionContext.find(logicTable, each);
            if (condition.isPresent()) {
                result.add(getShardingValue(condition.get()));
            }
        }
        return result;
    }
    
    private ShardingValue<?> getShardingValue(final Condition condition) {
        List<Comparable<?>> conditionValues = condition.getValues();
        switch (condition.getOperator()) {
            case EQUAL:
            case IN:
                if (1 == conditionValues.size()) {
                    return new ShardingValue<Comparable<?>>(condition.getColumn().getColumnName(), conditionValues.get(0));
                }
                return new ShardingValue<>(condition.getColumn().getColumnName(), conditionValues);
            case BETWEEN:
                return new ShardingValue<>(condition.getColumn().getColumnName(), Range.range(conditionValues.get(0), BoundType.CLOSED, conditionValues.get(1), BoundType.CLOSED));
            default:
                throw new UnsupportedOperationException(condition.getOperator().getExpression());
        }
    }
    
    private void logBeforeRoute(final String type, final String logicTable, final Collection<?> targets, final Collection<String> shardingColumns, final List<ShardingValue<?>> shardingValues) {
        log.trace("Before {} sharding {} routes db names: {} sharding columns: {} sharding values: {}", type, logicTable, targets, shardingColumns, shardingValues);
    }
    
    private void logAfterRoute(final String type, final String logicTable, final Collection<String> shardingResults) {
        log.trace("After {} sharding {} result: {}", type, logicTable, shardingResults);
    }

    /**
     * 生成路由结果
     *
     * @param routedDataSources
     * @param routedTables
     * @return
     */
    private SingleRoutingResult generateRoutingResult(final Collection<String> routedDataSources, final Collection<String> routedTables) {
        SingleRoutingResult result = new SingleRoutingResult();
        for (DataNode each : tableRule.get().getActualDataNodes(routedDataSources, routedTables)) {
            result.put(each.getDataSourceName(), new SingleRoutingTableFactor(logicTable, each.getTableName()));
        }
        return result;
    }
}
