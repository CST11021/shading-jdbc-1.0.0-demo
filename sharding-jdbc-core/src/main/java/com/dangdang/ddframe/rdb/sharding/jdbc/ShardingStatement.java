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

package com.dangdang.ddframe.rdb.sharding.jdbc;

import com.dangdang.ddframe.rdb.sharding.executor.StatementExecutor;
import com.dangdang.ddframe.rdb.sharding.jdbc.adapter.AbstractStatementAdapter;
import com.dangdang.ddframe.rdb.sharding.merger.ResultSetFactory;
import com.dangdang.ddframe.rdb.sharding.parser.result.merger.MergeContext;
import com.dangdang.ddframe.rdb.sharding.router.SQLExecutionUnit;
import com.dangdang.ddframe.rdb.sharding.router.SQLRouteEngine;
import com.dangdang.ddframe.rdb.sharding.router.SQLRouteResult;
import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * 支持分片的静态语句对象.
 * 
 * @author gaohongtao
 */
public class ShardingStatement extends AbstractStatementAdapter {
    
    @Getter(AccessLevel.PROTECTED)
    private final ShardingConnection shardingConnection;
    
    @Getter(AccessLevel.PROTECTED)
    private final SQLRouteEngine sqlRouteEngine;
    
    @Getter
    private final int resultSetType;
    
    @Getter
    private final int resultSetConcurrency;
    
    @Getter
    private final int resultSetHoldability;
    
    private final Map<HashCode, Statement> cachedRoutedStatements = new HashMap<>();

    /** 结果merger上下文 */
    @Getter(AccessLevel.PROTECTED)
    @Setter(AccessLevel.PROTECTED)
    private MergeContext mergeContext;

    /** 本次执行SQL返回的结果集 */
    @Getter(AccessLevel.PROTECTED)
    @Setter(AccessLevel.PROTECTED)
    private ResultSet currentResultSet;
    
    public ShardingStatement(final SQLRouteEngine sqlRouteEngine, final ShardingConnection shardingConnection) throws SQLException {
        this(sqlRouteEngine, shardingConnection, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }
    
    public ShardingStatement(final SQLRouteEngine sqlRouteEngine, final ShardingConnection shardingConnection, final int resultSetType, final int resultSetConcurrency) throws SQLException {
        this(sqlRouteEngine, shardingConnection, resultSetType, resultSetConcurrency, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }
    
    public ShardingStatement(final SQLRouteEngine sqlRouteEngine, final ShardingConnection shardingConnection, 
            final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability) throws SQLException {
        super(Statement.class);
        this.shardingConnection = shardingConnection;
        this.sqlRouteEngine = sqlRouteEngine;
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.resultSetHoldability = resultSetHoldability;
    }
    
    @Override
    public Connection getConnection() throws SQLException {
        return shardingConnection;
    }
    
    @Override
    public ResultSet executeQuery(final String sql) throws SQLException {
        if (null != currentResultSet && !currentResultSet.isClosed()) {
            currentResultSet.close();
        }
        currentResultSet = ResultSetFactory.getResultSet(generateExecutor(sql).executeQuery(), mergeContext);
        return currentResultSet;
    }
    
    @Override
    public int executeUpdate(final String sql) throws SQLException {
        return generateExecutor(sql).executeUpdate();
    }
    
    @Override
    public int executeUpdate(final String sql, final int autoGeneratedKeys) throws SQLException {
        return generateExecutor(sql).executeUpdate(autoGeneratedKeys);
    }
    
    @Override
    public int executeUpdate(final String sql, final int[] columnIndexes) throws SQLException {
        return generateExecutor(sql).executeUpdate(columnIndexes);
    }
    
    @Override
    public int executeUpdate(final String sql, final String[] columnNames) throws SQLException {
        return generateExecutor(sql).executeUpdate(columnNames);
    }
    
    @Override
    public boolean execute(final String sql) throws SQLException {
        return generateExecutor(sql).execute();
    }
    
    @Override
    public boolean execute(final String sql, final int autoGeneratedKeys) throws SQLException {
        return generateExecutor(sql).execute(autoGeneratedKeys);
    }
    
    @Override
    public boolean execute(final String sql, final int[] columnIndexes) throws SQLException {
        return generateExecutor(sql).execute(columnIndexes);
    }
    
    @Override
    public boolean execute(final String sql, final String[] columnNames) throws SQLException {
        return generateExecutor(sql).execute(columnNames);
    }
    
    private StatementExecutor generateExecutor(final String sql) throws SQLException {
        StatementExecutor result = new StatementExecutor();
        SQLRouteResult sqlRouteResult = sqlRouteEngine.route(sql, Collections.emptyList());
        mergeContext = sqlRouteResult.getMergeContext();
        for (SQLExecutionUnit each : sqlRouteResult.getExecutionUnits()) {
            result.addStatement(each.getSql(), generateStatement(each.getSql(), each.getDataSource()));
        }
        return result;
    }
    
    private Statement generateStatement(final String sql, final String dataSourceName) throws SQLException {
        HashCode hashCode =  Hashing.md5().newHasher().putString(sql, Charsets.UTF_8).putString(dataSourceName, Charsets.UTF_8).hash();
        if (cachedRoutedStatements.containsKey(hashCode)) {
            return cachedRoutedStatements.get(hashCode);
        }
        Connection connection = shardingConnection.getConnection(dataSourceName);
        Statement result;
        if (0 == resultSetHoldability) {
            result = connection.createStatement(resultSetType, resultSetConcurrency);
        } else {
            result = connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
        }
        replayMethodsInvovation(result);
        cachedRoutedStatements.put(hashCode, result);
        return result;
    }
    
    @Override
    public ResultSet getResultSet() throws SQLException {
        if (null != currentResultSet) {
            return currentResultSet;
        }
        List<ResultSet> resultSets = new ArrayList<>(getRoutedStatements().size());
        for (Statement each : getRoutedStatements()) {
            resultSets.add(each.getResultSet());
        }
        currentResultSet = ResultSetFactory.getResultSet(resultSets, mergeContext);
        return currentResultSet;
    }
    
    @Override
    public Collection<? extends Statement> getRoutedStatements() throws SQLException {
        return cachedRoutedStatements.values();
    }
}
