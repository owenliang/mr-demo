package cc.yuerblog.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class SequenceMR {
    private static class Mapper extends org.apache.hadoop.mapreduce.Mapper<Text, Text, Text, LongWritable> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Text row = new Text(String.format("%s=%s", key.toString(), value.toString()));
            context.write(row, new LongWritable(1));  // (内容,1次)
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
        Path input = new Path("/sequence.txt");
        Path output = new Path("/sequence-mr");

        // 删除之前的结果
        FileSystem.get(conf).delete(output, true);

        // 创建Job
        Job job = Job.getInstance(conf, "SequenceMR");
        job.setJarByClass(SequenceMR.class); // mapper/reducer实现在该类内部，需要设置这个

        /// 输入
        SequenceFileInputFormat.addInputPath(job, input);   // 文件路径
        job.setInputFormatClass(SequenceFileInputFormat.class); // 文件格式
        job.setMapperClass(SequenceMR.Mapper.class);   // mapper实现
        job.setMapOutputKeyClass(Text.class);   // 中间key
        job.setMapOutputValueClass(LongWritable.class); // 中间value

        // 输出
        SequenceFileOutputFormat.setOutputPath(job, output);    // 文件路径
        SequenceFileOutputFormat.setCompressOutput(job, true);  // 开启压缩
        SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);    // gzip压缩
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK); // 块级压缩
        job.setOutputFormatClass(SequenceFileOutputFormat.class);   // 文件格式
        job.setReducerClass(SequenceMR.Reducer.class); // reducer实现
        job.setOutputKeyClass(Text.class);  // 结果key
        job.setOutputValueClass(LongWritable.class);    // 结果value

        // 等待任务执行，打印详情
        job.waitForCompletion(true);
    }
}
