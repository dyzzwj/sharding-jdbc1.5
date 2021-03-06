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

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.LinkedList;
import java.util.List;

/**
 * 选择项标记对象.
 * 目前有三个地方产生：
 * 1. AVG 查询额外 COUNT 和 SUM  AVG查询列：SELECT AVG(price) FROM t_order 的 AVG(price)
 * 2. GROUP BY 不在 查询字段，额外查询该字段 SELECT COUNT(order_id) FROM t_order GROUP BY user_id 的 user_id
 * 3. ORDER BY 不在 查询字段，额外查询该字段 SELECT order_id FROM t_order ORDER BY create_time 的 create_time
 * 4、自增主键未在插入列中：INSERT INTO t_order(nickname) VALUES ... 中没有自增列 order_id
 *
 * @author zhangliang
 */
@RequiredArgsConstructor
@Getter
@ToString
public final class ItemsToken implements SQLToken {

    /**
     * SQL 开始位置
     */
    private final int beginPosition;
    /**
     * 字段名数组
     */
    private final List<String> items = new LinkedList<>();
}
