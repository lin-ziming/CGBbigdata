package cn.tedu.flink.dataset;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * 针对Source进行练习
 * readTextFile(path)
 * fromCollection(Collection)
 * fromElements(T ...)
 * @author Haitao
 * @date 2020/12/24 16:48
 */
public class SourceTest {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2.获取数据源
//        DataSource<String> source = env.fromElements("hello flink hadoop hello");

//        List<String> list = new ArrayList<>();
//        list.add("hello flink hadoop hello");
//        DataSource<String> source = env.fromCollection(list);

        DataSource<String> source = env.readTextFile("data.txt");
//        DataSource<String> source = env.readTextFile("hdfs://hadoop01:9000/flinkdata");
        //3.转化数据

        source.flatMap(new FlatMapFunction<String, Word>() {
            @Override
            public void flatMap(String value, Collector<Word> out) throws Exception {
                String[] split = value.split(" ");
                Word word = new Word();//代码优化：1.不要在循环内new对象，消耗内存 2.不要用+拼接字符串，也是耗内存
                for (String s : split){
                    word.setWord(s);
                    word.setCount(1);
                    out.collect(word);
                }
            }
        }).groupBy("word").reduce(new ReduceFunction<Word>() {
            @Override
            public Word reduce(Word value1, Word value2) throws Exception {
                value1.setCount(value1.getCount()+value2.getCount());
                return value1;
            }
        }).print();
        //4.输出结果

        //5.触发程序执行

    }
}
