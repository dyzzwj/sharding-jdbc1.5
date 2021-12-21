package com.dyzwj.zwjsharding.config;

import org.apache.shardingsphere.api.sharding.standard.RangeShardingAlgorithm;
import org.apache.shardingsphere.api.sharding.standard.RangeShardingValue;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

public class MyDBRangeShardingAlgorithm implements RangeShardingAlgorithm<Integer> {


    /**
     * collection ：在分库时值为所有分片库的集合 databaseNames，分表时为对应分片库中所有分片表的集合 tablesNames；取决于开发者的配置 MyDBPreciseShardingAlgorithm
     * preciseShardingValue 为分片属性，其中 logicTableName 为逻辑表，columnName 分片健（字段），value 为从 SQL 中解析出的分片健的值 其中lowerEndpoint 表示起始值， upperEndpoint 表示截止值
     */
    @Override
    public Collection<String> doSharding(Collection<String> collection, RangeShardingValue<Integer> rangeShardingValue) {
        Set<String> result = new LinkedHashSet<>();
        // between and 的起始值
        Integer lower = rangeShardingValue.getValueRange().lowerEndpoint();
        Integer upper = rangeShardingValue.getValueRange().upperEndpoint();
        // 循环范围计算分库逻辑
        for (int i = lower; i < upper; i++) {
            int index = i % collection.size();
            result.add(collection.stream().filter(x -> x.endsWith(String.valueOf(index))).findFirst().get());
        }

        return result;
    }
}
