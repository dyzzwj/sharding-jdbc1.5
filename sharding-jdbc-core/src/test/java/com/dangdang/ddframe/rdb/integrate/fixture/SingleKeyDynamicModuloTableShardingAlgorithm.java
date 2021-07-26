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

package com.dangdang.ddframe.rdb.integrate.fixture;

import com.dangdang.ddframe.rdb.sharding.api.ShardingValue;
import com.dangdang.ddframe.rdb.sharding.api.strategy.table.SingleKeyTableShardingAlgorithm;
import com.google.common.collect.Range;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.LinkedHashSet;

/**
 * 单片键动态计算分片算法
 */
@RequiredArgsConstructor
public final class SingleKeyDynamicModuloTableShardingAlgorithm implements SingleKeyTableShardingAlgorithm<Integer> {

    /**
     * 表前缀  动态表无需把真实表配置到 TableRule，而是通过分片算法计算出真实表。
     */
    private final String tablePrefix;

    /**
     *  availableTargetNames:动态表都不会使用availableTargetNames这个参数
     * @param availableTargetNames 所有的可用目标名称集合, 一般是数据源或表名称
     * @param shardingValue 分片值
     * @return
     */
    @Override
    public String doEqualSharding(final Collection<String> availableTargetNames, final ShardingValue<Integer> shardingValue) {
        return tablePrefix + shardingValue.getValue() % 10;
    }
    
    @Override
    public Collection<String> doInSharding(final Collection<String> availableTargetNames, final ShardingValue<Integer> shardingValue) {
        Collection<String> result = new LinkedHashSet<>(shardingValue.getValues().size());
        for (Integer value : shardingValue.getValues()) {
            result.add(tablePrefix + value % 10);
        }
        return result;
    }
    
    @Override
    public Collection<String> doBetweenSharding(final Collection<String> availableTargetNames, final ShardingValue<Integer> shardingValue) {
        Collection<String> result = new LinkedHashSet<>(availableTargetNames.size());
        Range<Integer> range = shardingValue.getValueRange();
        for (Integer i = range.lowerEndpoint(); i <= range.upperEndpoint(); i++) {
            result.add(tablePrefix + i % 10);
        }
        return result;
    }
}
