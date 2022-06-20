package com.yyh.whale.project.udf;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author yyhuan
 */
public class JsonSplitUDF extends ScalarFunction {
    private static final int num = 2;
    private static final ObjectMapper mapper = new ObjectMapper();

    public JsonSplitUDF() {
    }

    public String eval(String json, String... s) throws JsonProcessingException {
        ObjectNode jsonNode = (ObjectNode) mapper.readTree(json);
        for (int i = 0; i < s.length; i = i + num) {
            jsonNode.put(s[i], s[i + 1]);
        }
        return jsonNode.toString();
    }
}
