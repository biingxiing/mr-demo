
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

public class DemoTask {

    //入口方法
    public static void main(String[] args) throws IOException {
        JobConf jobConf = new JobConf(DemoTask.class);

        jobConf.setJobName("work-count");

        //mr 任务输出key的类型
        jobConf.setOutputKeyClass(Text.class);
        //mr 任务输出value的类型
        jobConf.setOutputValueClass(IntWritable.class);

        //map 任务类
        jobConf.setMapperClass(MapTask.class);
        //执行完任务之后 在节点进行combine操作的类
        jobConf.setCombinerClass(ReduceTask.class);
        //reduce 任务类
        jobConf.setReducerClass(ReduceTask.class);

        //任务输入格式转化
        jobConf.setInputFormat(TextInputFormat.class);
        //任务输出格式转化
        jobConf.setOutputFormat(TextOutputFormat.class);

        //源文件输入目录
        String sourceFile = "/recom/test/text.txt";
        FileInputFormat.setInputPaths(jobConf, new Path(sourceFile));
        //结果文件输出目录
        String outFile = "/recom/test/result";
        FileOutputFormat.setOutputPath(jobConf, new Path(outFile));

        RunningJob result = JobClient.runJob(jobConf);
        if (result.isSuccessful()) {
            System.out.println("task run success, source file:" + sourceFile + ", out file:" + outFile);
        } else {
            System.out.println("task run fail");
        }
    }

    /**
     * mapper class 对数据进行切分 分组
     */
    public static class MapTask extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

        private static final IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable longWritable, Text text, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            String value = text.toString();
            String[] values = value.split("");
            for (int i = 0; i < value.length(); i++) {
                String v = values[i];
                word.set(v);
                outputCollector.collect(word, one);
            }
        }
    }

    /**
     * reduce class 框架会把key相同的聚合到一起 然后进行计数
     */
    public static class ReduceTask extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text text, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            int sum = 0;
            while (iterator.hasNext()) {
                sum += iterator.next().get();

            }
            outputCollector.collect(text, new IntWritable(sum));
        }
    }
}
