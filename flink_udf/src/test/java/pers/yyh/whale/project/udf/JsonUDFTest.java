package pers.yyh.whale.project.udf;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

public class JsonUDFTest {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    @Test
    public void testJsonSplitUDF() throws Exception {
        tEnv.createTemporaryFunction("CDPJsonSplit", JsonSplitUDF.class);
        Table table = tEnv.sqlQuery("select CDPJsonSplit('{\"id\":\"111111\",\"company_id\":\"12222222\"}','name','zhangsan','age','13')");
        tEnv.toDataStream(table).print();
        env.execute();
    }
}
