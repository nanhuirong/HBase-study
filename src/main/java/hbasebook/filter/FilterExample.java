package hbasebook.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by nanhuirong on 16-7-22.
 * 比较过滤器
 */
public class FilterExample {
    public static final String TEST_TABLE = "testtable";
    public static Configuration conf = null;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "311b-node-2,310b-node-1");
    }

    /**
     * 行过滤器
     */
    public static void RowFilter()throws IOException{
        HTable table = new HTable(conf, TEST_TABLE);
        Scan scan = new Scan();
//        Filter filter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("row-1")));
//        scan.setFilter(filter);
//        ResultScanner resultScanner = table.getScanner(scan);
//        for (Result result: resultScanner){
//            System.out.println(result);
//        }
//        resultScanner.close();


        //使用正则进行过滤, 基于字符串的比较器更消耗资源
        Filter filter1 = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(".*-5"));
        scan.setFilter(filter1);
        int count = 0;
        ResultScanner resultScanner1 = table.getScanner(scan);
        for (Result result: resultScanner1){
//            System.out.println( Bytes.toString(result.getRow()));
            count++;
        }
        System.out.println(count);
        resultScanner1.close();
        //基于字串进行匹配
        Filter filter2 = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("-5"));
        scan.setFilter(filter2);
        ResultScanner resultScanner2 = table.getScanner(scan);
        count = 0;
        for (Result result: resultScanner2){
//            System.out.println( Bytes.toString(result.getRow()));
            count++;
        }
        System.out.println(count);
        resultScanner2.close();

        table.close();
    }

    /**
     * 列族过滤器, 过滤出符合条件的列族, 可用于Scan与Get
     * get还得指定需要的列族
     * @throws IOException
     */
    public static void FamilyFilter()throws IOException{
        HTable table = new HTable(conf, TEST_TABLE);
        Filter filter = new FamilyFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(Bytes.toBytes("colfam3")));

    }

    /**
     * 列名过滤器
     * Scan Get
     * @throws IOException
     */
    public static void QualifierFilter()throws IOException{
        Filter filter = new QualifierFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(Bytes.toBytes("col-2")));
    }

    /**
     * 值过滤器
     * @throws IOException
     */
    public static void ValueFilter()throws IOException{
        Filter filter = new ValueFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(Bytes.toBytes(".4")));
    }

    /**
     * 参考列顾虑器
     *
     * @param args
     * @throws IOException
     */





    public static void main(String[] args)throws IOException{
        RowFilter();
    }
}
