/*
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

package com.dangdang.ddframe.rdb.sharding.api;

import com.dangdang.ddframe.rdb.sharding.hint.HintManagerHolder;
import com.dangdang.ddframe.rdb.sharding.hint.ShardingKey;
import com.dangdang.ddframe.rdb.sharding.constant.ShardingOperator;
import com.dangdang.ddframe.rdb.sharding.routing.type.hint.DatabaseHintRoutingEngine;
import com.google.common.base.Preconditions;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * 通过线索传递分片值的管理器.
 *
 * @author gaohongtao
 * @author zhangliang
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class HintManager implements AutoCloseable {

    /**
     * 库分片值集合
     */
    private final Map<ShardingKey, ShardingValue<?>> databaseShardingValues = new HashMap<>();

    /**
     * 表分片值集合
     */
    private final Map<ShardingKey, ShardingValue<?>> tableShardingValues = new HashMap<>();

    /**
     * 是否分片暗示
     */
    @Getter
    private boolean shardingHint;
    
    @Getter
    private boolean masterRouteOnly;

    /**
     * 只做库分片
     * {@link DatabaseHintRoutingEngine}
     */
    @Getter
    private boolean databaseShardingOnly;
    
    /**
     * 获取线索分片管理器实例.
     *  每次获取到的都是新的 HintManager，多次赋值需要小心。
     * @return 线索分片管理器实例
     */
    public static HintManager getInstance() {
        HintManager result = new HintManager();
        HintManagerHolder.setHintManager(result);
        return result;
    }
    
    /**
     * 设置分库分片值.
     * 
     * <p>分片操作符为等号.该方法适用于只分库的场景</p>
     * 
     * @param value 分片值
     */
    public void setDatabaseShardingValue(final Comparable<?> value) {
        databaseShardingOnly = true;
        addDatabaseShardingValue(HintManagerHolder.DB_TABLE_NAME, HintManagerHolder.DB_COLUMN_NAME, value);
    }
    
    /**
     * 添加分库分片值.
     * 
     * <p>分片操作符为等号.</p>
     *
     * @param logicTable 逻辑表名称
     * @param shardingColumn 分片键
     * @param value 分片值
     */
    public void addDatabaseShardingValue(final String logicTable, final String shardingColumn, final Comparable<?> value) {
        addDatabaseShardingValue(logicTable, shardingColumn, ShardingOperator.EQUAL, value);
    }
    
    /**
     * 添加分库分片值.
     *
     * @param logicTable 逻辑表名称
     * @param shardingColumn 分片键
     * @param operator 分片操作符
     * @param values 分片值
     */
    public void addDatabaseShardingValue(final String logicTable, final String shardingColumn, final ShardingOperator operator, final Comparable<?>... values) {
        shardingHint = true;
        databaseShardingValues.put(new ShardingKey(logicTable, shardingColumn), getShardingValue(logicTable, shardingColumn, operator, values));
    }
    
    /**
     * 添加分表分片值.
     * 
     * <p>分片操作符为等号.</p>
     *
     * @param logicTable 逻辑表名称
     * @param shardingColumn 分片键
     * @param value 分片值
     */
    public void addTableShardingValue(final String logicTable, final String shardingColumn, final Comparable<?> value) {
        addTableShardingValue(logicTable, shardingColumn, ShardingOperator.EQUAL, value);
    }
    
    /**
     * 添加分表分片值.
     *
     * @param logicTable 逻辑表名称
     * @param shardingColumn 分片键
     * @param operator 分片操作符
     * @param values 分片值
     */
    public void addTableShardingValue(final String logicTable, final String shardingColumn, final ShardingOperator operator, final Comparable<?>... values) {
        shardingHint = true;
        tableShardingValues.put(new ShardingKey(logicTable, shardingColumn), getShardingValue(logicTable, shardingColumn, operator, values));
    }

    /**
     * sharding列只支持=，in和between的操作：
     */
    @SuppressWarnings("unchecked")
    private ShardingValue getShardingValue(final String logicTable, final String shardingColumn, final ShardingOperator operator, final Comparable<?>[] values) {
        Preconditions.checkArgument(null != values && values.length > 0);
        switch (operator) {
            case EQUAL:
                return new ShardingValue<Comparable<?>>(logicTable, shardingColumn, values[0]);
            case IN:
                return new ShardingValue(logicTable, shardingColumn, Arrays.asList(values));
            case BETWEEN:
                return new ShardingValue(logicTable, shardingColumn, Range.range(values[0], BoundType.CLOSED, values[1], BoundType.CLOSED));
            default:
                throw new UnsupportedOperationException(operator.getExpression());
        }
    }
    
    /**
     * 获取分库分片键值.
     * 
     * @param shardingKey 分片键
     * @return 分库分片键值
     */
    public ShardingValue<?> getDatabaseShardingValue(final ShardingKey shardingKey) {
        return databaseShardingValues.get(shardingKey);
    }
    
    /**
     * 获取分表分片键值.
     * 
     * @param shardingKey 分片键
     * @return 分表分片键值
     */
    public ShardingValue<?> getTableShardingValue(final ShardingKey shardingKey) {
        return tableShardingValues.get(shardingKey);
    }
    
    /**
     * 设置数据库操作只路由至主库.
     */
    public void setMasterRouteOnly() {
        masterRouteOnly = true;
    }

    /**
     * 使用完需要去清理，避免下个请求读到遗漏的线程变量。
     */
    @Override
    public void close() {
        HintManagerHolder.clear();
    }
}
