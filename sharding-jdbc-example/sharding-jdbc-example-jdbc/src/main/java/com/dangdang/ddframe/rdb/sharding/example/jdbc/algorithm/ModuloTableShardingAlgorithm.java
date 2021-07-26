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

package com.dangdang.ddframe.rdb.sharding.example.jdbc.algorithm;

import com.dangdang.ddframe.rdb.sharding.api.ShardingValue;
import com.dangdang.ddframe.rdb.sharding.api.strategy.table.SingleKeyTableShardingAlgorithm;
import com.google.common.collect.Range;

import java.util.Collection;
import java.util.LinkedHashSet;

/**
 * 余数基偶分表算法
 */
public final class ModuloTableShardingAlgorithm implements SingleKeyTableShardingAlgorithm<Integer> {

    /**
     * 计算分片
     * 如果SQL中分表列order_id条件为where order_id=?
     */
    @Override
    public String doEqualSharding(final Collection<String> tableNames, final ShardingValue<Integer> shardingValue) {
        /**
         *  分析前提，假设预期分到两个表中[t_order_0,t_order_1]， 表的分片键为order_id
         *  且执行的SQL为SELECT o.* FROM t_order o where o.order_id=1001 AND o.user_id=10，那么分表列order_id的值为1001
         */
        //遍历表名[t_order_0,t_order_1]
        for (String each : tableNames) {
            //直到表名是以分表列order_id的值1001对2取模的值即1结尾，那么就是命中的表名，即t_order_1
            if (each.endsWith(shardingValue.getValue() % tableNames.size() + "")) {
                return each;
            }
        }
        throw new UnsupportedOperationException();
    }


    /**
     * 如果SQL中分表列order_id条件为where order_id in(?, ?)
     *
     */
    @Override
    public Collection<String> doInSharding(final Collection<String> tableNames, final ShardingValue<Integer> shardingValue) {
        Collection<String> result = new LinkedHashSet<>(tableNames.size());
        /**
         * 从这里可知，doInSharding()和doEqualSharding()的区别就是doInSharding()时分表列有多个值（shardingValue.getValues()），
         * 例如order_id的值为[1001,1002]，遍历这些值，然后每个值按照doEqualSharding()的逻辑计算表名
         *
         */
        for (Integer value : shardingValue.getValues()) {
            for (String tableName : tableNames) {
                if (tableName.endsWith(value % tableNames.size() + "")) {
                    result.add(tableName);
                }
            }
        }
        return result;
    }

    /**
     * 如果SQL中分表列order_id条件为where order_id between in(?, ?)
     */
    @Override
    public Collection<String> doBetweenSharding(final Collection<String> tableNames, final ShardingValue<Integer> shardingValue) {
        Collection<String> result = new LinkedHashSet<>(tableNames.size());
        Range<Integer> range = shardingValue.getValueRange();
        /**
         *  从这里可知，doBetweenSharding()和doInSharding()的区别就是doBetweenSharding()时分表列的多个值通过shardingValue.getValueRange()得到；
         *  而doInSharding()是通过shardingValue.getValues()得到；
         *
         */
        for (Integer i = range.lowerEndpoint(); i <= range.upperEndpoint(); i++) {
            for (String each : tableNames) {
                if (each.endsWith(i % tableNames.size() + "")) {
                    result.add(each);
                }
            }
        }
        return result;
    }
}
