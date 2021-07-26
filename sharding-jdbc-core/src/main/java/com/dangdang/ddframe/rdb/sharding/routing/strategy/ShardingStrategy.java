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

package com.dangdang.ddframe.rdb.sharding.routing.strategy;

import com.dangdang.ddframe.rdb.sharding.api.ShardingValue;
import com.google.common.base.Preconditions;
import lombok.Getter;

import java.util.Collection;
import java.util.Collections;
import java.util.TreeSet;

/**
 * 分片策略.
 * 
 * @author zhangliang
 */
public class ShardingStrategy {

    /**
     * 分片列集合
     */
    @Getter
    private final Collection<String> shardingColumns;
    /**
     * 分片算法   分库策略里指的是库，在分表策略里指的是表。
     */
    private final ShardingAlgorithm shardingAlgorithm;
    
    public ShardingStrategy(final String shardingColumn, final ShardingAlgorithm shardingAlgorithm) {
        this(Collections.singletonList(shardingColumn), shardingAlgorithm);
    }
    
    public ShardingStrategy(final Collection<String> shardingColumns, final ShardingAlgorithm shardingAlgorithm) {
        this.shardingColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        this.shardingColumns.addAll(shardingColumns);
        this.shardingAlgorithm = shardingAlgorithm;
    }
    
    /**
     * 计算静态分片.
     *
     * @param availableTargetNames 所有的可用分片资源（数据库或表）集合
     * @param shardingValues 分片值集合
     * @return 分库、分表后指向的数据源名称集合
     */
    public Collection<String> doStaticSharding(final Collection<String> availableTargetNames, final Collection<ShardingValue<?>> shardingValues) {
        Collection<String> result = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        if (shardingValues.isEmpty()) {
            result.addAll(availableTargetNames);
        } else {
            result.addAll(doSharding(shardingValues, availableTargetNames));
        }
        return result;
    }
    
    /**
     * 计算动态分片.
     *
     * @param shardingValues 分片值集合
     * @return 分库后指向的分片资源集合
     */
    public Collection<String> doDynamicSharding(final Collection<ShardingValue<?>> shardingValues) {
        Preconditions.checkState(!shardingValues.isEmpty(), "Dynamic table should contain sharding value."); // 动态分片必须有分片值
        Collection<String> availableTargetNames = Collections.emptyList();
        Collection<String> result = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        result.addAll(doSharding(shardingValues, availableTargetNames));
        return result;
    }

    /**
     * 计算分片
     *
     * @param shardingValues 分片值（分片键对应的值）集合
     * @param availableTargetNames 所有的可用分片资源（数据库或表）集合
     * @return 分片后指向的分片资源（数据库或表）集合
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Collection<String> doSharding(final Collection<ShardingValue<?>> shardingValues, final Collection<String> availableTargetNames) {
        /**
         * shardingAlgorithm即sharding算法分为三种：NoneKey，SingleKey和MultipleKeys
         */

        // 无片键
        if (shardingAlgorithm instanceof NoneKeyShardingAlgorithm) {
            return Collections.singletonList(((NoneKeyShardingAlgorithm) shardingAlgorithm).doSharding(availableTargetNames, shardingValues.iterator().next()));
        }
        // 单片键
        if (shardingAlgorithm instanceof SingleKeyShardingAlgorithm) {
            // 得到SingleKeyShardingAlgorithm的具体实现，在ShardingStrategy的构造方法中赋值
            SingleKeyShardingAlgorithm<?> singleKeyShardingAlgorithm = (SingleKeyShardingAlgorithm<?>) shardingAlgorithm;
            // ShardingValue就是sharding的列和该列的值，在这里分别为order_id和1000
            ShardingValue shardingValue = shardingValues.iterator().next();
            /**
             * sharding列的类型分为三种：SINGLE，LIST和RANGE
             */
            switch (shardingValue.getType()) {
                /**
                 * 如果是where order_id=1000，那么type就是SINGLE
                 */
                case SINGLE:
                    /**
                     *  doEqualSharding只返回一个值，为了doSharding()返回值的统一，用Collections.singletonList()包装成集合；
                     */
                    return Collections.singletonList(singleKeyShardingAlgorithm.doEqualSharding(availableTargetNames, shardingValue));


                /**
                 * 如果SQL中分表列order_id条件为where order_id in(?, ?)，
                 */
                case LIST:
                    return singleKeyShardingAlgorithm.doInSharding(availableTargetNames, shardingValue);

                /**
                 * 如果SQL中分表列order_id条件为where order_id between in(?, ?)
                 */
                case RANGE:

                    return singleKeyShardingAlgorithm.doBetweenSharding(availableTargetNames, shardingValue);
                default:
                    throw new UnsupportedOperationException(shardingValue.getType().getClass().getName());
            }
        }
        // 多片键
        if (shardingAlgorithm instanceof MultipleKeysShardingAlgorithm) {
            return ((MultipleKeysShardingAlgorithm) shardingAlgorithm).doSharding(availableTargetNames, shardingValues);
        }
        throw new UnsupportedOperationException(shardingAlgorithm.getClass().getName());
    }
}
