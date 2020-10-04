package cc.yuerblog.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class RawMR {
    private static class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, new LongWritable(1));  // (内容,1次)
        }
    }

    private static class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable v : values) {
                count += v.get();
            }
            context.write(key, new LongWritable(count));
        }
    }

    public void run(Configuration conf) throws Exception {
        Path input = new Path("/raw.gz");
        Path output = new Path("/raw-mr");

        // 删除之前的结果
        FileSystem.get(conf).delete(output, true);

        // 创建Job
        Job job = Job.getInstance(conf, "RawMR");
        job.setJarByClass(RawMR.class); // mapper/reducer实现在该类内部，需要设置这个

        /// 输入
        TextInputFormat.addInputPath(job, input);   // 文件路径
        job.setInputFormatClass(TextInputFormat.class); // 文件格式
        job.setMapperClass(Mapper.class);   // mapper实现
        job.setMapOutputKeyClass(Text.class);   // 中间key
        job.setMapOutputValueClass(LongWritable.class); // 中间value

        // 输出
        TextOutputFormat.setOutputPath(job, output);    // 文件路径
        TextOutputFormat.setCompressOutput(job, true);  // 开启压缩
        TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);    // gzip压缩
        job.setOutputFormatClass(TextOutputFormat.class);   // 文件格式
        job.setReducerClass(Reducer.class); // reducer实现
        job.setOutputKeyClass(Text.class);  // 结果key
        job.setOutputValueClass(LongWritable.class);    // 结果value

        // 等待任务执行，打印详情
        job.waitForCompletion(true);
    }
}
