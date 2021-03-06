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

package com.dangdang.ddframe.rdb.sharding.parsing.parser.token;

import com.dangdang.ddframe.rdb.sharding.util.SQLUtil;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * 表标记对象.
 *
 * 查询列的表别名：SELECT o.order_id 的 o
 * 查询的表名：SELECT * FROM t_order 的 t_order
 *
 */
@RequiredArgsConstructor
@Getter
@ToString
public final class TableToken implements SQLToken {

    /**
     * 开始位置
     */
    private final int beginPosition;
    /**
     * 表达式
     */
    private final String originalLiterals;
    
    /**
     * 获取表名称.
     * 
     * @return 表名称
     */
    public String getTableName() {
        return SQLUtil.getExactlyValue(originalLiterals);
    }
}
