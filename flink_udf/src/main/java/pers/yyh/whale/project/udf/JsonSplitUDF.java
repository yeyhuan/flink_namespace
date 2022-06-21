package pers.yyh.whale.project.udf;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.functions.ScalarFunction;
import scala.Tuple2;

/**
 * @author yyhuan
 * 接收一个json，和要插入json到字段名称和值，返回一个
 */
public class JsonSplitUDF extends ScalarFunction {
    private static final int num = 2;
    private static final ObjectMapper mapper = new ObjectMapper();

    public JsonSplitUDF() {
    }

    /**
     * @param json
     * @param s
     * @return
     * @throws JsonProcessingException
     */
    public String eval(String json, String... s) throws JsonProcessingException {
        ObjectNode jsonNode = (ObjectNode) mapper.readTree(json);
        for (int i = 0; i < s.length; i = i + num) {
            jsonNode.put(s[i], s[i + 1]);
        }
        return jsonNode.toString();
    }

    public String eval(String json, Tuple2<String, String>... s) throws JsonProcessingException {
        ObjectNode jsonNode = (ObjectNode) mapper.readTree(json);
        //for (int i = 0; i < s.length; i = i + num) {
        //    jsonNode.put(s[i], s[i + 1]);
        //}
        for (Tuple2<String, String> tuple2 : s) {
            jsonNode.put(tuple2._1, tuple2._2);
        }
        return jsonNode.toString();
    }
}
