package tech.ytsaurus.spyt.format.bucketing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Recognizes the hash computed column expressions SPYT can bucket by: {@code function(column, ...)} optionally
 * followed by {@code % buckets}, where buckets is a positive int32 literal with or without the uint64 {@code u}
 * suffix. Columns follow yt/yt/library/query/base/lexer.rl6: a bare identifier that is not a keyword, a
 * backtick-quoted name whose {@code \\}, {@code \`}, {@code \"} and {@code \'} escapes are resolved, or a
 * square-bracket-quoted name taken verbatim. This is deliberately a subset of the YT expression grammar: nested
 * square brackets, other C escapes, string and numeric literals as arguments, casts, nested calls, unary signs,
 * arithmetic around the call and non-hash functions all yield {@link Optional#empty()} rather than a guess.
 */
public final class HashFunctionParser {
    // The only keywords the lexer turns into literal values: a bare true/false/null/inf in argument position is a
    // constant, not a column (lexer.rl6 kw_true/kw_false/kw_null/kw_inf). Any other keyword makes the expression a
    // syntax error in YT, so no table can carry it and it needs no filtering here.
    private static final Set<String> LITERAL_KEYWORDS =
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList("true", "false", "null", "inf")));

    private static final String PLAIN = "[A-Za-z_][A-Za-z0-9_]*";

    private static final String BACKTICK = "`(?:[^`\\\\]|\\\\.)*`";

    private static final String BRACKET = "\\[[^\\[\\]]*\\]";

    private static final String COLUMN = "(?:" + PLAIN + "|" + BACKTICK + "|" + BRACKET + ")";

    private static final Pattern COLUMN_PATTERN = Pattern.compile(COLUMN);

    private static final Pattern CALL_PATTERN = Pattern.compile(
            "\\s*(" + PLAIN + ")\\s*\\(\\s*(" + COLUMN + "(?:\\s*,\\s*" + COLUMN + ")*)\\s*\\)\\s*"
                    + "(?:%\\s*([0-9]+)u?\\s*)?");

    private HashFunctionParser() {
    }

    public static Optional<HashFunctionCall> parse(String expression) {
        if (expression == null) {
            return Optional.empty();
        }
        Matcher call = CALL_PATTERN.matcher(expression);
        if (!call.matches()) {
            return Optional.empty();
        }
        Optional<HashFunction> function = HashFunction.byYtName(call.group(1));
        Optional<List<String>> columns = parseColumns(call.group(2));
        if (!function.isPresent() || !columns.isPresent()) {
            return Optional.empty();
        }
        if (call.group(3) == null) {
            return Optional.of(new HashFunctionCall(function.get(), columns.get()));
        }
        return parseBuckets(call.group(3)).map(buckets -> new HashFunctionCall(function.get(), columns.get(), buckets));
    }

    private static Optional<List<String>> parseColumns(String columnList) {
        List<String> columns = new ArrayList<>();
        Matcher column = COLUMN_PATTERN.matcher(columnList);
        while (column.find()) {
            Optional<String> name = parseColumn(column.group());
            if (!name.isPresent()) {
                return Optional.empty();
            }
            columns.add(name.get());
        }
        return Optional.of(columns);
    }

    private static Optional<String> parseColumn(String token) {
        if (token.startsWith("[")) {
            return nonEmpty(token.substring(1, token.length() - 1));
        }
        if (token.startsWith("`")) {
            return unescape(token.substring(1, token.length() - 1)).flatMap(HashFunctionParser::nonEmpty);
        }
        return LITERAL_KEYWORDS.contains(token.toLowerCase(Locale.ROOT)) ? Optional.empty() : Optional.of(token);
    }

    private static Optional<String> unescape(String quoted) {
        StringBuilder name = new StringBuilder(quoted.length());
        for (int i = 0; i < quoted.length(); i++) {
            char current = quoted.charAt(i);
            if (current != '\\') {
                name.append(current);
                continue;
            }
            char escaped = quoted.charAt(++i);
            if (escaped != '\\' && escaped != '`' && escaped != '"' && escaped != '\'') {
                return Optional.empty();
            }
            name.append(escaped);
        }
        return Optional.of(name.toString());
    }

    private static Optional<String> nonEmpty(String name) {
        return name.isEmpty() ? Optional.empty() : Optional.of(name);
    }

    private static Optional<Integer> parseBuckets(String digits) {
        try {
            int buckets = Integer.parseInt(digits);
            return buckets > 0 ? Optional.of(buckets) : Optional.empty();
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }
}
