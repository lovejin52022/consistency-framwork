package com.zzjj.consistency.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.StringUtils;

import com.zzjj.consistency.model.ConsistencyTaskInstance;

import lombok.extern.slf4j.Slf4j;

/**
 * el表达式解析
 *
 * @author zengjin
 * @date 2023/11/19
 **/
@Slf4j
public class ExpressionUtils {

    private static final String START_MARK = "\\$\\{";

    public static final String RESULT_FLAG = "true";

    /**
     * 重写表达式
     *
     * @param alertExpression 告警表达式
     * @return 告警表达式
     */
    public static String rewriteExpr(final String alertExpression) {
        final String exprExpr = StringUtils.replace(alertExpression, "executeTimes", "#taskInstance.executeTimes");
        final StringJoiner exprJoiner = new StringJoiner("", "${", "}");
        exprJoiner.add(exprExpr);
        return exprJoiner.toString();
    }

    /**
     * 获取指定EL表达式在指定对象中的值
     *
     * @param expr spring el表达式
     * @param dataMap 数据集合 ref -> data 对象引用 -> data
     * @return 结果
     */
    public static String readExpr(String expr, final Map<String, Object> dataMap) {
        try {
            expr = ExpressionUtils.formatExpr(expr);
            // 表达式的上下文,
            final EvaluationContext context = new StandardEvaluationContext();
            // 为了让表达式可以访问该对象, 先把对象放到上下文中
            for (final Map.Entry<String, Object> entry : dataMap.entrySet()) {
                // key -> ref value -> iterator.next().getValue()
                context.setVariable(entry.getKey(), entry.getValue());
            }
            final SpelExpressionParser parser = new SpelExpressionParser();
            final Expression expression = parser.parseExpression(expr, new TemplateParserContext());
            return expression.getValue(context, String.class);
        } catch (final Exception e) {
            ExpressionUtils.log.error("解析表达式{}时，发生异常", expr, e);
            return "";
        }
    }

    /**
     * 构造数据map
     *
     * @param object 要访问的数据模型
     * @return 数据map
     */
    public static Map<String, Object> buildDataMap(final Object object) {
        final Map<String, Object> dataMap = new HashMap<>(1);
        dataMap.put("taskInstance", object);
        return dataMap;
    }

    /**
     * 对表达式进行格式化 ${xxx.name} -> #{xxx.name}
     *
     * @param expr 表达式
     */
    private static String formatExpr(final String expr) {
        return expr.replaceAll(ExpressionUtils.START_MARK, "#{");
    }

    public static void main(final String[] args) {
        final ConsistencyTaskInstance instance = ConsistencyTaskInstance.builder().executeTimes(4).build();
        final Map<String, Object> dataMap = new HashMap<>(2);
        dataMap.put("taskInstance", instance);

        final String expr = "executeTimes > 1 && executeTimes < 5";
        final String executeTimesExpr = StringUtils.replace(expr, "executeTimes", "#taskInstance.executeTimes");
        System.out.println(executeTimesExpr);
        System.out.println(ExpressionUtils.readExpr("${" + executeTimesExpr + "}", dataMap));

        final String expr2 = "executeTimes % 2 == 0";
        final String executeTimesExpr2 = StringUtils.replace(expr2, "executeTimes", "#taskInstance.executeTimes");
        System.out.println(executeTimesExpr2);
        System.out.println(ExpressionUtils.readExpr("${" + executeTimesExpr2 + "}", dataMap));

        System.out.println(ExpressionUtils.readExpr(ExpressionUtils.rewriteExpr(expr), dataMap));
    }

}
