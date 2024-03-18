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

package com.dangdang.ddframe.rdb.sharding.merger.iterator;

import com.dangdang.ddframe.rdb.sharding.jdbc.AbstractShardingResultSet;
import com.dangdang.ddframe.rdb.sharding.parser.result.merger.MergeContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * 迭代结果集.
 * 
 * @author zhangliang
 */
public final class IteratorResultSet extends AbstractShardingResultSet {

    /**
     * 多个执行单元，执行的结果会有多个结果集
     *
     * @param resultSets
     * @param mergeContext
     */
    public IteratorResultSet(final List<ResultSet> resultSets, final MergeContext mergeContext) {
        super(resultSets, mergeContext.getLimit());
    }

    /**
     * 将游标指向下一行
     *
     * @return
     * @throws SQLException
     */
    @Override
    protected boolean nextForSharding() throws SQLException {
        if (getCurrentResultSet().next()) {
            return true;
        }

        for (int i = getResultSets().indexOf(getCurrentResultSet()) + 1; i < getResultSets().size(); i++) {
            ResultSet each = getResultSets().get(i);
            if (each.next()) {
                setCurrentResultSet(each);
                return true;
            }
        }
        return false;
    }
}
