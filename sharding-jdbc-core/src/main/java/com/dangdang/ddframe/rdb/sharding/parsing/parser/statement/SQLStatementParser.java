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

package com.dangdang.ddframe.rdb.sharding.parsing.parser.statement;

/**
 * SQL语句解析器.
 *
 * SQLParsingEngine 调用 StatementParser 解析 SQL。
 * StatementParser 调用 SQLParser 解析 SQL 表达式。
 * SQLParser 调用 Lexer 解析 SQL 词法。
 *
 * @author zhangliang
 */
public interface SQLStatementParser {
    
    /**
     * 解析SQL语句.
     *
     * @return 解析结果
     */
    SQLStatement parse();
}
