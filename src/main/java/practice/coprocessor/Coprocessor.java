package practice.coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import practice.util.TableTool;

import java.io.IOException;

/**
 * Created by nanhuirong on 16-10-5.
 * 协处理器
 * 执行Region级操作, 将计算放到服务器端执行, 减少返回客户端的数据量
 */
public class Coprocessor {
    public static void main(String[] args) {

    }

    /**
     * 检查特定get请求的Region Observer
     */
    public void checkGetObserver()throws IOException{
        TableTool tableTool = new TableTool();
        Configuration conf = tableTool.getConf();
        FileSystem fs = FileSystem.get(conf);
        //获取一个包含协处理器实现的Jar
        Path path = new Path(fs.getUri() + Path.SEPARATOR + "test.jar");
        HTableDescriptor tableDescriptor = new HTableDescriptor("FILTER_TABLE");
        tableDescriptor.addFamily(new HColumnDescriptor("colfam1"));
        tableDescriptor.setValue("COPROCESSOR$1", path.toString() + "|" + RegionObserverExample.class.getCanonicalName());
    }
}
