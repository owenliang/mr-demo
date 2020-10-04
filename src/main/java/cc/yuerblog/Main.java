package cc.yuerblog;

import cc.yuerblog.mr.AvroMR;
import cc.yuerblog.mr.RawMR;
import cc.yuerblog.mr.SequenceMR;
import org.apache.hadoop.conf.Configuration;

public class Main {
    // MapReduce官方: https://hadoop.apache.org/docs/r1.2.1/mapred_tutorial.html
    // inputformat解释：https://www.doudianyun.com/2018/11/mapreduce-inputformat/

    // mapreduce核心理解：
    // mapper输入的文件格式需要通过对应的inputformat来进行分片与解析，输入必须兼容到Key,value的方式。
    // mapper输出没有文件格式（mr框架内置），只需要key和value可序列化即可（writeable)，比如avro对象序列化就是一种。
    // reducer输入key和value是writeable的
    // reducer输出的文件格式需要通过outputformat指定，其作用是兼容key,value到具体文件格式，比如输出为avro container file格式。

    public static void main(String[] args) {
        Configuration conf = new Configuration();

        try {
            // 文本文件(带压缩)MR
            RawMR rawMR = new RawMR();
            rawMR.run(conf);

            // sequencefile的MR
            SequenceMR seqMR = new SequenceMR();
            seqMR.run(conf);

            // Avro的MR
            AvroMR avroMR = new AvroMR();
            avroMR.run(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
