package cn.tedu.flink.dataset;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

/**
 * 针对数据转化Transformation的练习
 *
 * @author Haitao
 * @date 2020/12/25 10:46
 */
public class TransformationTest {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2.获取数据源
        DataSource<String> source1 = env.fromElements("0,董长春,男","1,刘沛霞,女","2,程晓宇,女","3,王海涛,男");
        DataSource<String> source2 = env.readTextFile("data.txt");
        //3.转化数据

//----------------------------------------------------------------------------------------------------
        /*
        * outerJoin:外连接
        * */
        MapOperator<String, Tuple3<String, String, String>> input1
                = source1.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] s = value.split(",");
                return new Tuple3<>(s[0], s[1], s[2]);
            }
        });
        MapOperator<String, Tuple3<String, String, String>> input2
                = source2.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] s = value.split(",");
                return new Tuple3<>(s[0], s[1], s[2]);
            }
        });
        input1.leftOuterJoin(input2)
                .where(1)
                .equalTo(1)
                .with(new JoinFunction<Tuple3<String, String, String>,
                        Tuple3<String, String, String>,
                        Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> join(
                    Tuple3<String, String, String> first,
                    Tuple3<String, String, String> second) throws Exception {
                return new Tuple3<>(first.f1,first.f2,second == null ? "不详" : second.f2);
            }
        }).print();

//-------------------------------------------------------------------------------------
//        MapOperator<String, Tuple3<String, String, String>> input1
//                = source1.map(new MapFunction<String, Tuple3<String, String, String>>() {
//            @Override
//            public Tuple3<String, String, String> map(String value) throws Exception {
//                String[] s = value.split(",");
//                return new Tuple3<>(s[0], s[1], s[2]);
//            }
//        });
//        MapOperator<String, Tuple3<String, String, String>> input2
//                = source2.map(new MapFunction<String, Tuple3<String, String, String>>() {
//            @Override
//            public Tuple3<String, String, String> map(String value) throws Exception {
//                String[] s = value.split(",");
//                return new Tuple3<>(s[0], s[1], s[2]);
//            }
//        });
//        input1.join(input2)
//                .where(0)
//                .equalTo(0)
//                .projectFirst(1,2)
//                .projectSecond(2)
//                .print();

//-------------------------------------------------------------------------------
        /*
         * distinct:去重
         * project: 字段挑选
         * */
//        source.map(x -> {
//            String[] s = x.split(",");
//            return new Tuple4<String,String,String,String>(s[0],s[1],s[2],s[3]);
//        }).returns(Types.TUPLE(Types.STRING,Types.STRING,Types.STRING,Types.STRING))
//                .distinct(1)
//                .project(1,3)
//                .print();

//        source.map(new MapFunction<String, Tuple4<String, String, String, String>>() {
//            @Override
//            public Tuple4<String, String, String, String> map(String value) throws Exception {
//                String[] s = value.split(",");
//                return new Tuple4<>(s[0],s[1],s[2],s[3]);
//            }
//        }).distinct(1).project(1,3).print();

//-------------------------------------------------------------
        /*
        * sum: 求和
        * 1,2,3,4,5 --> 15
        * */
//        source.map((x)-> {return new Tuple1<>(x);})
//                .returns(Types.TUPLE(Types.INT))
//                .sum(0).print();

//        source.map(new MapFunction<Integer, Tuple1<Integer>>() {
//            @Override
//            public Tuple1<Integer> map(Integer value) throws Exception {
//                return new Tuple1<>(value);
//            }
//        }).sum(0).print();


//----------------------------------------------------------------------
        /*
        * reduceGroup: 归并，与reduce功能相同，可以输出多个结果
        * 1,2,3,4,5 --> 1,3,6,10,15
        * */
//        source.reduceGroup(new GroupReduceFunction<Integer, Integer>() {
//            @Override
//            public void reduce(Iterable<Integer> values, Collector<Integer> out) throws Exception {
//                Integer sum = 0;
//                for (Integer value : values) {
//                    out.collect(sum += value);
//                }
//            }
//        }).print();


//---------------------------------------------------------
        /*
        * reduce: 归并，通过将数据集中的数据两两合并，以达到最终将所有数据合并的目的
        * 1,2,3,4,5 --> 15 求和
        * */
//        source.reduce(new ReduceFunction<Integer>() {
//            @Override
//            public Integer reduce(Integer value1, Integer value2) throws Exception {
//                return value1 + value2;
//            }
//        }).print();


//----------------------------------------------------------------------------
        /*
        * filter: 过滤，只返回结果为true的数据
        * 1,2,3,4,5 --> 1,3,5 将所有偶数过滤掉
        * */
//        source.filter(new FilterFunction<Integer>() {
//            @Override
//            public boolean filter(Integer value) throws Exception {
//                return value % 2 != 0;
//            }
//        }).print();


//------------------------------------------------------------------------
        /*
        * flatMap: 进一出多(0,1,n)
        * */
       /* source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] split = value.split(",");
                for (String s : split) {
                    out.collect(s);
                }
            }
        }).print();*/


//----------------------------------------------------------------
        /*
        * map：进一出一
        * */
        // 1,2,3,4,5 --> 10,20,30,40,50
//        source.map(x -> x * 10).print();
        //4.输出结果

        //5.触发程序执行(print不需要)

    }
}
