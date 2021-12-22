package com.dyzwj.zwjsharding.config;

import org.apache.shardingsphere.api.sharding.complex.ComplexKeysShardingAlgorithm;
import org.apache.shardingsphere.api.sharding.complex.ComplexKeysShardingValue;

import java.util.*;


/**
 * 复合分片策略
 */
public class MyDBComplexKeysShardingAlgorithm implements ComplexKeysShardingAlgorithm<Integer> {

    @Override
    public Collection<String> doSharding(Collection<String> collection, ComplexKeysShardingValue<Integer> complexKeysShardingValue) {

        Collection<Integer> sexValues = this.getShardingValue(complexKeysShardingValue, "sex");
        Collection<Integer> ageValues = this.getShardingValue(complexKeysShardingValue, "age");
        List<String> shardingSuffix = new ArrayList<>();

        // 对两个分片健同时取模的方式分库
        for (Integer sexValue : sexValues) {
            for (Integer ageValue : ageValues) {
                int index = (ageValue + sexValue) % collection.size();
                shardingSuffix.add(collection.stream().filter(x -> x.endsWith(String.valueOf(index))).findFirst().get());
            }
        }
        return shardingSuffix;
    }
    private Collection<Integer> getShardingValue(ComplexKeysShardingValue<Integer> shardingValues, final String key) {
        Collection<Integer> result = new ArrayList<>();
        Map<String, Collection<Integer>> columnNameAndShardingValuesMap = shardingValues.getColumnNameAndShardingValuesMap();
        if (columnNameAndShardingValuesMap.containsKey(key)){
            result.addAll(columnNameAndShardingValuesMap.get(key));
        }
        return result;

    }

}
