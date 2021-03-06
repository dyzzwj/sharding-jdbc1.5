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

/**
 * 排序标记对象.
 *
 * 有 GROUP BY 条件，无 ORDER BY 条件：SELECT COUNT(*) FROM t_order GROUP BY order_id 的 order_id
 * @author zhangliang
 */
@RequiredArgsConstructor
@Getter
public final class OrderByToken implements SQLToken {

    /**
     * SQL 所在开始位置
     */
    private final int beginPosition;
}
