package com.dangdang.ddframe.rdb.sharding.parsing.lexer.token;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * 词法标记.
 *
 * @author zhangliang
 */
@RequiredArgsConstructor
@Getter
public final class Token {

    /**
     * 词法标记类型
     * DefaultKeyword ：词法关键词
     * Literals ：词法字面量标记
     * Symbol ：词法符号标记
     * Assist ：词法辅助标记
     */
    private final TokenType type;
    /**
     * 词法字面量标记
     */
    private final String literals;
    /**
     * {@link #literals} 在 SQL 里的 结束位置
     */
    private final int endPosition;
}
