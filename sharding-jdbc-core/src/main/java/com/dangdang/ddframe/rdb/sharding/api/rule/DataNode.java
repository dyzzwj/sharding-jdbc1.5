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

package com.dangdang.ddframe.rdb.sharding.api.rule;

import com.google.common.base.Splitter;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.List;

/**
 * 静态分库分表数据单元
 *  数据分片的最小单元，由数据源名称和数据表组成。
 * 例：ds_1.t_order_0。配置时默认各个分片数据库的表结构均相同，直接配置逻辑表和真实表对应关系即可。
 * 如果各数据库的表结果不同，可使用ds.actual_table配置
 *
 * @author zhangliang
 */
@RequiredArgsConstructor
@Getter
@EqualsAndHashCode
@ToString
public class DataNode {
    
    private static final String DELIMITER = ".";

    /**
     * 数据源名 TableRule 对 dataSourceRule 只使用数据源名字，最终执行SQL 使用数据源名字从 ShardingRule 获取数据源连接
     */
    private final String dataSourceName;
    /**
     * 真实表
     */
    private final String tableName;
    
    public DataNode(final String dataNode) {
        List<String> segments = Splitter.on(DELIMITER).splitToList(dataNode);
        dataSourceName = segments.get(0);
        tableName = segments.get(1);
    }
    
    /**
     * 判断字符串是否为合法的分库分表数据单元字符串.
     * 
     * @param dataNodeStr 待判断的字符串
     * @return 是否为合法的分库分表数据单元字符串
     */
    public static boolean isValidDataNode(final String dataNodeStr) {
        return dataNodeStr.contains(DELIMITER) && 2 == Splitter.on(DELIMITER).splitToList(dataNodeStr).size();
    }
}
