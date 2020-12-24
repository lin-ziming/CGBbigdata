package cn.tedu.flink.dataset;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

/**
 * Flink入门程序
 * @author Haitao
 * @date 2020/12/24 15:59
 */
public class HelloFlink {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2.获取数据源
        DataSource<Integer> source = env.fromElements(1, 2, 3, 4, 5);
        //3.转化数据
        source.map(x -> x * 10)
        //4.输出结果
                .print();
        //5.触发任务执行 //暂时不实现


    }
}
