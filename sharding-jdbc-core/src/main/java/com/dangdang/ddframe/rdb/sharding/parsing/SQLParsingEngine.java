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

package com.dangdang.ddframe.rdb.sharding.parsing;

import com.dangdang.ddframe.rdb.sharding.api.rule.ShardingRule;
import com.dangdang.ddframe.rdb.sharding.constant.DatabaseType;
import com.dangdang.ddframe.rdb.sharding.parsing.lexer.token.DefaultKeyword;
import com.dangdang.ddframe.rdb.sharding.parsing.lexer.token.Symbol;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.SQLParser;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.dialect.mysql.MySQLParser;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.dialect.oracle.OracleParser;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.dialect.postgresql.PostgreSQLParser;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.dialect.sqlserver.SQLServerParser;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.exception.SQLParsingUnsupportedException;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.statement.SQLStatement;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.statement.ddl.alter.AlterParserFactory;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.statement.ddl.create.CreateParserFactory;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.statement.dml.delete.DeleteParserFactory;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.statement.ddl.drop.DropParserFactory;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.statement.dml.insert.InsertParserFactory;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.statement.dql.select.SelectParserFactory;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.statement.ddl.truncate.TruncateParserFactory;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.statement.dml.update.UpdateParserFactory;
import lombok.RequiredArgsConstructor;

/**
 * SQL????????????.
 * SQLParsingEngine ?????? StatementParser ?????? SQL???
 * StatementParser ?????? SQLParser ?????? SQL ????????????
 * SQLParser ?????? Lexer ?????? SQL ?????????
 *
 * @author zhangliang
 */
@RequiredArgsConstructor
public final class SQLParsingEngine {

    /**
     * ???????????????
     */
    private final DatabaseType dbType;
    /**
     * SQL
     */
    private final String sql;
    /**
     * ????????????
     */
    private final ShardingRule shardingRule;
    
    /**
     * ??????SQL.
     * 
     * @return SQL????????????
     */
    public SQLStatement parse() {
        // ?????? SQL?????????
        SQLParser sqlParser = getSQLParser();
        //
        sqlParser.skipIfEqual(Symbol.SEMI); // ?????? ";"
        if (sqlParser.equalAny(DefaultKeyword.WITH)) { // WITH Syntax
            skipWith(sqlParser);
        }
        // ???????????? SQL??????????????? ??????SQL
        if (sqlParser.equalAny(DefaultKeyword.SELECT)) {
            return SelectParserFactory.newInstance(sqlParser).parse();
        }
        if (sqlParser.equalAny(DefaultKeyword.INSERT)) {
            return InsertParserFactory.newInstance(shardingRule, sqlParser).parse();
        }
        if (sqlParser.equalAny(DefaultKeyword.UPDATE)) {
            return UpdateParserFactory.newInstance(sqlParser).parse();
        }
        if (sqlParser.equalAny(DefaultKeyword.DELETE)) {
            return DeleteParserFactory.newInstance(sqlParser).parse();
        }
        if (sqlParser.equalAny(DefaultKeyword.CREATE)) {
            return CreateParserFactory.newInstance(sqlParser).parse();
        }
        if (sqlParser.equalAny(DefaultKeyword.ALTER)) {
            return AlterParserFactory.newInstance(sqlParser).parse();
        }
        if (sqlParser.equalAny(DefaultKeyword.DROP)) {
            return DropParserFactory.newInstance(sqlParser).parse();
        }
        if (sqlParser.equalAny(DefaultKeyword.TRUNCATE)) {
            return TruncateParserFactory.newInstance(sqlParser).parse();
        }
        throw new SQLParsingUnsupportedException(sqlParser.getLexer().getCurrentToken().getType());
    }
    
    private SQLParser getSQLParser() {
        switch (dbType) {
            case H2:
            case MySQL:
                return new MySQLParser(sql, shardingRule);
            case Oracle:
                return new OracleParser(sql, shardingRule);
            case SQLServer:
                return new SQLServerParser(sql, shardingRule);
            case PostgreSQL:
                return new PostgreSQLParser(sql, shardingRule);
            default:
                throw new UnsupportedOperationException(dbType.name());
        }
    }

    /**
     * ??????  WITH Syntax ??????
     * https://dev.mysql.com/doc/refman/8.0/en/with.html
     *
     * @param sqlParser SQL ?????????
     */
//    WITH
//      cte1 AS (SELECT a, b FROM table1),
//      cte2 AS (SELECT c, d FROM table2)
//    SELECT b, d FROM cte1 JOIN cte2
//    WHERE cte1.a = cte2.c;
    private void skipWith(final SQLParser sqlParser) {
        sqlParser.getLexer().nextToken();
        do {
            sqlParser.skipUntil(DefaultKeyword.AS);
            sqlParser.accept(DefaultKeyword.AS);
            sqlParser.skipParentheses();
        } while (sqlParser.skipIfEqual(Symbol.COMMA));
    }
}
