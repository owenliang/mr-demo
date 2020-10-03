package cc.yuerblog;

import org.apache.hadoop.conf.Configuration;
import cc.yuerblog.mr.RawMR;

import java.io.IOException;

public class Main {
    // MapReduce官方: https://hadoop.apache.org/docs/r1.2.1/mapred_tutorial.html
    // inputformat解释：https://www.doudianyun.com/2018/11/mapreduce-inputformat/
    public static void main(String []args) {
        Configuration conf = new Configuration();

        try {
            // 文本文件(带压缩)MR
            RawMR rawMR = new RawMR();
            rawMR.run(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
