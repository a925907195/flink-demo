package wikiedits;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;


public class Test {

    public static void main(String[] args) throws Exception{

        ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();


        // Create a dataset of numbers
        DataSet<Integer> numbers = env.fromElements(1, 2, 3, 4, 5, 6, 7);
        // Square every number
        DataSet<Integer> result = numbers.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer integer) throws Exception {
                System.out.println(integer);
                return integer * integer;
            }
        });
        // Leave only even
        numbers.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer integer) throws Exception {
                System.out.println(integer);
                return integer % 2 == 0;
            }
        });
        numbers.print();
        env.execute();

    }

}
