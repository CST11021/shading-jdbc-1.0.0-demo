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

package com.dangdang.ddframe.rdb.sharding.merger.orderby;

import com.dangdang.ddframe.rdb.sharding.jdbc.AbstractShardingResultSet;
import com.dangdang.ddframe.rdb.sharding.parser.result.merger.MergeContext;
import com.dangdang.ddframe.rdb.sharding.parser.result.merger.OrderByColumn;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 排序结果集处理.
 * 
 * @author gaohongtao, zhangliang
 */
public final class OrderByResultSet extends AbstractShardingResultSet {

    /** 涉及的排序字段 */
    private final List<OrderByColumn> orderByColumns;

    /** 初始化后，会将所有的结果集放到这里，游标遍历结束后，会清空结果集 */
    private final List<ResultSet> effectivedResultSets;

    /** 初始化标记 */
    private boolean initial;
    
    public OrderByResultSet(final List<ResultSet> resultSets, final MergeContext mergeContext) {
        super(resultSets, mergeContext.getLimit());
        orderByColumns = mergeContext.getOrderByColumns();
        effectivedResultSets = new ArrayList<>(resultSets.size());
    }

    /**
     * 是否可以继续访问，每次从每个结果集里获取一条数据，然后进行排序比较
     *
     * @return
     * @throws SQLException
     */
    @Override
    public boolean nextForSharding() throws SQLException {
        if (!initial) {
            initialEffectivedResultSets();
        } else {
            nextEffectivedResultSets();
        }

        OrderByValue choosenOrderByValue = null;
        for (ResultSet each : effectivedResultSets) {
            OrderByValue eachOrderByValue = new OrderByValue(orderByColumns, each);
            if (null == choosenOrderByValue || choosenOrderByValue.compareTo(eachOrderByValue) > 0) {
                choosenOrderByValue = eachOrderByValue;
                setCurrentResultSet(each);
            }
        }

        // nextEffectivedResultSets()方法中，游标遍历结束后，会清空结果集
        return !effectivedResultSets.isEmpty();
    }

    /**
     * 初始化结果集
     *
     * @throws SQLException
     */
    private void initialEffectivedResultSets() throws SQLException {
        for (ResultSet each : getResultSets()) {
            if (each.next()) {
                effectivedResultSets.add(each);
            }
        }
        initial = true;
    }

    /**
     * 指向下一个结果集
     *
     * @throws SQLException
     */
    private void nextEffectivedResultSets() throws SQLException {
        boolean next = getCurrentResultSet().next();
        if (!next) {
            effectivedResultSets.remove(getCurrentResultSet());
        }
    }
}
