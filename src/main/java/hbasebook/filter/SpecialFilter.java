package hbasebook.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by nanhuirong on 16-7-22.
 * 专用过滤器
 * 直接继承FilterBase
 */
public class SpecialFilter {
    public static final String TEST_TABLE = "testtable";
    public static Configuration conf = null;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "311b-node-2,310b-node-1");
    }

    /**
     * 单列值过滤器
     * 一列的值决定一行是否过滤掉
     *
     *
     * 单列排除过滤器 继承自单列过滤器
     * 参考列不会出现在结果中
     *
     * 前缀过滤器
     *
     * 分页过滤器
     * @throws IOException
     */
    public static void SingleColumnValueFilter()throws IOException{
        HTable table = new HTable(conf, TEST_TABLE);

        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("colfam1"), Bytes.toBytes("col-1"), CompareFilter.CompareOp.NOT_EQUAL, new SubstringComparator("val-1"));
        //如果参考列不存在, 默认保留在输出的结果中, 下述配置可以将其过滤掉
        filter.setFilterIfMissing(true);
        table.close();
    }



    public static void main(String[] args)throws IOException {

    }

}
