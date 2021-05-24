package cn.tedu.flink.dataset;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 针对source和sink的练习
 * readTextFile(path)
 * fromCollection(Collection)
 * fromElements(T ...)
 *
 * print()
 * writeAsText()
 *
 * @author Haitao
 * @date 2020/12/25 9:19
 */
public class SourceSinkTest {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2.获取数据源
        DataSource<String> source = env.fromElements("dongcc liuyj dongcc liupx qilei qilei");
        //3.转化数据
        source.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] split = s.split(" ");
                for (String word : split){
                    collector.collect(new Tuple2<>(word,1));
                }
            }
        }).groupBy(0).sum(1).print();




//        source.flatMap(new FlatMapFunction<String, Word>() {
//            @Override
//            public void flatMap(String s, Collector<Word> collector) throws Exception {
//                String[] words = s.split(" ");
//                Word word = new Word();
//                for (String w : words){
//                    word.setWord(w);
//                    word.setCount(1);
//                    collector.collect(word);
//                }
//            }
//        }).groupBy("word").reduce(new ReduceFunction<Word>() {
//            @Override
//            public Word reduce(Word v1, Word v2) throws Exception {
//                v1.setCount(v1.getCount() + v2.getCount());
//                return v1;
//            }
//        })
////                .print();
//        .writeAsText("result.txt").setParallelism(1);

        //flink默认会调用可见资源的所有
        //4.输出结果

        //5.触发程序执行

//        env.execute();

    }
}

