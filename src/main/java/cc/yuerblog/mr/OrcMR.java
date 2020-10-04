package cc.yuerblog.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.orc.*;
import org.apache.orc.mapred.OrcKey;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;
import org.apache.orc.mapreduce.OrcInputFormat;
import org.apache.orc.mapreduce.OrcOutputFormat;

import java.io.IOException;

// https://orc.apache.org/docs/mapreduce.html
public class OrcMR {
    private static TypeDescription schema;
    static {
        // 数据结构描述
        schema = TypeDescription.createStruct();
        schema.addField("id", TypeDescription.createInt());
        schema.addField("name", TypeDescription.createString());
    }

    private static class Mapper extends org.apache.hadoop.mapreduce.Mapper<NullWritable, OrcStruct, OrcKey, OrcValue> {
        @Override
        protected void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {
            IntWritable id = (IntWritable)value.getFieldValue("id");
            Text name = (Text)value.getFieldValue("name");
            System.out.printf("%d -> %s", id.get(), name.toString());

            context.write(new OrcKey(value), new OrcValue(new LongWritable(1)));  // struct -> 1
        }
    }

    private static class Reducer extends org.apache.hadoop.mapreduce.Reducer<OrcKey, OrcValue, NullWritable, OrcStruct> {
        @Override
        protected void reduce(OrcKey key, Iterable<OrcValue> values, Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(), (OrcStruct)key.key); // 原样输出orc对象
        }
    }

    public void run(Configuration conf) throws Exception {
        Path input = new Path("/orc.txt");
        Path output = new Path("/orc-mr");

        // 删除之前的结果
        FileSystem.get(conf).delete(output, true);

        // 创建Job
        Job job = Job.getInstance(conf, "OrcMR");
        job.setJarByClass(OrcMR.class); // mapper/reducer实现在该类内部，需要设置这个

        /// 输入
        OrcInputFormat.addInputPath(job, input);   // 文件路径
        job.setInputFormatClass(OrcInputFormat.class); // 文件格式
        job.setMapperClass(OrcMR.Mapper.class);   // mapper实现
        job.setMapOutputKeyClass(OrcKey.class);   // 中间key
        job.setMapOutputValueClass(OrcValue.class); // 中间value
        job.getConfiguration().set(OrcConf.MAPRED_SHUFFLE_KEY_SCHEMA.getAttribute(), schema.toString());    // 中间key schema
        job.getConfiguration().set(OrcConf.MAPRED_SHUFFLE_VALUE_SCHEMA.getAttribute(), TypeDescription.createLong().toString());    // 中间value schema

        // 输出
        OrcOutputFormat.setOutputPath(job, output);    // 文件路径
        OrcOutputFormat.setCompressOutput(job, true);  // 开启压缩
        OrcOutputFormat.setOutputCompressorClass(job, GzipCodec.class);    // gzip压缩
        job.setOutputFormatClass(OrcOutputFormat.class);   // 文件格式
        job.setReducerClass(OrcMR.Reducer.class); // reducer实现
        job.setOutputKeyClass(NullWritable.class);  // 结果key
        job.setOutputValueClass(OrcStruct.class);    // 结果value
        job.getConfiguration().set(OrcConf.MAPRED_OUTPUT_SCHEMA.getAttribute(), schema.toString()); // 输出orc文件schema

        // 等待任务执行，打印详情
        job.waitForCompletion(true);


        ///////////// 查看MR输出

        // 读打开
        Reader in = OrcFile.createReader(new Path("/orc-mr/part-r-00000.orc"), OrcFile.readerOptions(conf));
        // 解析schema
        VectorizedRowBatch inBatch = in.getSchema().createRowBatch();
        // 流解析文件
        RecordReader rows = in.rows();
        while (rows.nextBatch(inBatch)) {   // 读1个batch
            // 列式读取
            LongColumnVector idCol = (LongColumnVector)inBatch.cols[0]; // id 列
            BytesColumnVector nameCol = (BytesColumnVector)inBatch.cols[1]; // name列
            for (int i = 0; i < inBatch.size; i++) {
                System.out.printf("[Orc-MR-output] id=%d name=%s\n", idCol.vector[i], new String(nameCol.vector[i], nameCol.start[i], nameCol.length[i]));
            }
        }
        rows.close();
    }
}
