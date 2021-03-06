/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package com.dangdang.ddframe.rdb.sharding.parsing.lexer;

import com.dangdang.ddframe.rdb.sharding.parsing.lexer.analyzer.CharType;
import com.dangdang.ddframe.rdb.sharding.parsing.lexer.analyzer.Dictionary;
import com.dangdang.ddframe.rdb.sharding.parsing.lexer.analyzer.Tokenizer;
import com.dangdang.ddframe.rdb.sharding.parsing.lexer.token.Assist;
import com.dangdang.ddframe.rdb.sharding.parsing.lexer.token.Token;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * 词法解析器. 顺序解析sql 由于不同数据库遵守 SQL 规范略有不同，所以不同的数据库对应不同的 Lexer
 * 只做词法的解析，不关注上下文，将字符串拆解成 N 个词法
 *
 * Lexer与Parser的关系：
     * SQL ：SELECT * FROM t_user
     * Lexer ：[SELECT] [ * ] [FROM] [t_user]
     * Parser ：这是一条 [SELECT] 查询表为 [t_user] ，并且返回 [ * ] 所有字段的 SQL。
 *
 *  原理：顺序顺序顺序 解析 SQL，将字符串拆解成 N 个词法。
 *
 */
@RequiredArgsConstructor
public class Lexer {

    /**
     * 输出字符串
     * 比如：SQL
     */
    @Getter
    private final String input;
    /**
     * 词法标记字典
     */
    private final Dictionary dictionary;
    /**
     * 解析到 SQL 的 offset
     */
    private int offset;
    /**
     * 当前 词法标记
     */
    @Getter
    private Token currentToken;

    /**
     * 分析下一个词法标记.
     *使用 #skipIgnoredToken() 方法跳过忽略的 Token，通过 #isXXXX() 方法判断好下一个 Token 的类型后，交给 Tokenizer 进行分词返回 Token
     */
    public final void nextToken() {
        skipIgnoredToken();
        if (isVariableBegin()) { // 变量
            currentToken = new Tokenizer(input, dictionary, offset).scanVariable();
        } else if (isNCharBegin()) { // N\
            currentToken = new Tokenizer(input, dictionary, ++offset).scanChars();
        } else if (isIdentifierBegin()) { // Keyword + Literals.IDENTIFIER
            currentToken = new Tokenizer(input, dictionary, offset).scanIdentifier();
        } else if (isHexDecimalBegin()) { // 十六进制
            currentToken = new Tokenizer(input, dictionary, offset).scanHexDecimal();
        } else if (isNumberBegin()) { // 数字（整数+浮点数）
            currentToken = new Tokenizer(input, dictionary, offset).scanNumber();
        } else if (isSymbolBegin()) { // 符号
            currentToken = new Tokenizer(input, dictionary, offset).scanSymbol();
        } else if (isCharsBegin()) { // 字符串，例如："abc"
            currentToken = new Tokenizer(input, dictionary, offset).scanChars();
        } else if (isEnd()) { // 结束
            currentToken = new Token(Assist.END, "", offset);
        } else { // 分析错误，无符合条件的词法标记
            currentToken = new Token(Assist.ERROR, "", offset);
        }
        offset = currentToken.getEndPosition();
        System.out.println("| " + currentToken.getLiterals() + " | "
                +  currentToken.getType().getClass().getSimpleName() + " | " + currentToken.getType() + " | "
                + currentToken.getEndPosition() + " |");
    }

    /**
     * 跳过忽略的词法标记
     * 1. 空格
     * 2. SQL Hint
     * 3. SQL 注释
     */
    private void skipIgnoredToken() {
        // 空格
        offset = new Tokenizer(input, dictionary, offset).skipWhitespace();
        // SQL Hint
        while (isHintBegin()) {
            offset = new Tokenizer(input, dictionary, offset).skipHint();
            offset = new Tokenizer(input, dictionary, offset).skipWhitespace();
        }
        // SQL 注释
        while (isCommentBegin()) {
            offset = new Tokenizer(input, dictionary, offset).skipComment();
            offset = new Tokenizer(input, dictionary, offset).skipWhitespace();
        }
    }

    /**
     * 是否是 SQL Hint
     *
     * @return 是否
     */
    protected boolean isHintBegin() {
        return false;
    }

    /**
     * 是否是 SQL 注释
     *
     * @return 是否
     */
    protected boolean isCommentBegin() {
        char current = getCurrentChar(0);
        char next = getCurrentChar(1);
        // tips：优先级：&& > || ，因此，不需要成对加小括号
        return '/' == current && '/' == next || '-' == current && '-' == next || '/' == current && '*' == next;
    }

    /**
     * 是否是 变量
     * MySQL 与 SQL Server 支持
     *
     * @see Tokenizer#scanVariable()
     * @return 是否
     */
    protected boolean isVariableBegin() {
        return false;
    }

    protected boolean isSupportNChars() {
        return false;
    }

    /**
     * 是否 N\
     * 目前 SQLServer 独有：在 SQL Server 中處理 Unicode 字串常數時，必需為所有的 Unicode 字串加上前置詞 N
     *
     * @see Tokenizer#scanChars()
     * @return 是否
     */
    private boolean isNCharBegin() {
        return isSupportNChars() && 'N' == getCurrentChar(0) && '\'' == getCurrentChar(1);
    }

    /**
     * 是否是 Keyword + Literals.IDENTIFIER
     *
     * @see Tokenizer#scanIdentifier()
     * @return 是否
     */
    private boolean isIdentifierBegin() {
        return isIdentifierBegin(getCurrentChar(0));
    }

    private boolean isIdentifierBegin(final char ch) {
        return CharType.isAlphabet(ch) || '`' == ch || '_' == ch || '$' == ch;
    }

    /**
     * 是否是 十六进制
     *
     * @see Tokenizer#scanHexDecimal()
     * @return 是否
     */
    private boolean isHexDecimalBegin() {
        return '0' == getCurrentChar(0) && 'x' == getCurrentChar(1);
    }

    /**
     * 是否是 数字
     * '-' 需要特殊处理。".2" 被处理成省略0的小数，"-.2" 不能被处理成省略的小数，否则会出问题。
     * 例如说，"SELECT a-.2" 处理的结果是 "SELECT" / "a" / "-" / ".2"
     *
     * @return 是否
     */
    private boolean isNumberBegin() {
        return CharType.isDigital(getCurrentChar(0)) // 数字
                || ('.' == getCurrentChar(0) && CharType.isDigital(getCurrentChar(1)) && !isIdentifierBegin(getCurrentChar(-1)) // 浮点数
                || ('-' == getCurrentChar(0) && ('.' == getCurrentChar(0) || CharType.isDigital(getCurrentChar(1))))); // 负数
    }

    /**
     * 是否是 符号 例如：”{“, “}”, “>=” 等等
     *
     * @see Tokenizer#scanSymbol()
     * @return 是否
     */
    private boolean isSymbolBegin() {
        return CharType.isSymbol(getCurrentChar(0));
    }

    /**
     * 是否是 字符串
     *
     * @see Tokenizer#scanChars()
     * @return 是否
     */
    private boolean isCharsBegin() {
        return '\'' == getCurrentChar(0) || '\"' == getCurrentChar(0);
    }

    private boolean isEnd() {
        return offset >= input.length();
    }

    protected final char getCurrentChar(final int offset) {
        return this.offset + offset >= input.length() ? (char) CharType.EOI : input.charAt(this.offset + offset);
    }

}
