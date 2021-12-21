package com.dyzwj.zwjsharding.config;

import org.apache.shardingsphere.api.sharding.standard.PreciseShardingAlgorithm;
import org.apache.shardingsphere.api.sharding.standard.PreciseShardingValue;

import java.util.Collection;

public class MyDBPreciseShardingAlgorithm implements PreciseShardingAlgorithm<Integer> {

    /**
     * collection ：在分库时值为所有分片库的集合 databaseNames，分表时为对应分片库中所有分片表的集合 tablesNames；取决于开发者的配置 MyDBPreciseShardingAlgorithm
     * preciseShardingValue 为分片属性，其中 logicTableName 为逻辑表，columnName 分片健（字段），value 为从 SQL 中解析出的分片健的值
     */
    @Override
    public String doSharding(Collection<String> collection, PreciseShardingValue<Integer> preciseShardingValue) {
        /**
         * collection 所有分片库的集合
         * preciseShardingValue 为分片属性，其中 logicTableName 为逻辑表，columnName 分片健（字段），value 为从 SQL 中解析出的分片健的值
         */
        return collection.stream().filter(x -> x.endsWith(String.valueOf(preciseShardingValue.getValue() % collection.size()))).findFirst().get();
    }
}
