package practice.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import practice.util.TableTool;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by nanhuirong on 16-9-24.
 * get 和 Scan 都可以指定列族, 列, 时间戳, 版本号等限制条件 来减少得到的数据量
 * 但是这样的指定是非常粗力度的, 因而催生出了过滤器这样的组件
 *
 * 所有的过滤器在服务器端生效, 保证被过滤的数据不会被传送到客户端
 *
 * 如果在客户端进行过滤, 自己编写过滤条件, 这种情况下, 服务器会将所有的数据传送到客户端
 *
 *
 * 过滤器：基本接口Filter和FilterBase抽象类, 保证被过滤掉的数据不会传送到客户端(过滤器在服务器端生效);
 * 过滤器的层次结构:Filter接口和FilterBase抽象类
 *                CompareFilter类, 比FilterBase多了一个compare()方法, (CompareFilter.Compare.LESS诸如此种用法)
 *                  比较运算符:LESS, LESS_OR_QUEAL, EQUAL, NOT_EQUAL, GREATER_OR_EQUAL, GREATER, NO_OP(排除一切值)
 *                  比较器(comparator):继承自WritableByteComparable, 实现了Writable和Comparable接口
 *                      1.BinaryComparator    使用Bytes.CompareTo()比较当前值和阈值
 *                      2.BinaryPrefixComparator  使用Bytes.CompareTo()比较当前值和阈值, 但是从左端开始前缀匹配
 *                      3.NullComparator  不做匹配, 判断当前只是不是NULL
 *                      4.BitComparator   通过BitwiseOp类提供的按位与(AND), 或(OR), 异或(XOR)执行位级比较
 *                      5.RegexStringComparator   根据正则表达式匹配
 *                      6.SubstringComparator     根据阈值和表中数据当作String实例, 通过contains方法进行匹配
 *                      (4, 5, 6只能与EQUAL, NOT_EQUAL进行匹配)
 *                      (基于字符串的比较5, 6会比基于字节的比较更慢, 因为每次比较需要将给定的值转化为字符串)
 *                  (上述两个参数为下面过滤器的参数)
 *                  1.行过滤器RowFilter
 *                  2.列族过滤器FamilyFilter,
 *                  3.列过滤器QualifierFilter
 *                  4.值过滤器ValueFilter
 *                  5.参考列过滤器DependentColumnFilter, (目前对这个过滤器还是有点懵)
 *                专用过滤器:直接继承自FilterBase
 *                  1.单列值过滤器SingleColumnValueFilter:用一列的值决定整行数据是否被过滤, 可以通过设定如果不包含参考列是如何处理, 以及版本号的问题
 *                  2.单列排除过滤器SingleColumnValueExcludeFilter, 参考列不被包含在结果中, 剩余内容与1相同
 *                  3.前缀过滤器PrefixFilter, 所有与前缀匹配的行都会返回
 *                  4.分页过滤器PageFilter,
 *                  5.行健过滤器KeyOnlyFilter
 *                  6.首次行健过滤器FirstOnlyFilter:访问一行数据的第一列(HBase隐式排列, 按字典排序)
 *                  7.包含结束的过滤器InclusiveStopFilter, 终止行被排除在外
 *                  8.时间戳过滤器TimestampsFilter
 *                  9.列计数过滤器ColumnCountGetFilter, 设置每行最多取回多少个列
 *                  10.列分页过滤器ColumnPaginationFilter, 对一行的所有列进行分页
 *                  11.列前缀过滤器ColumnPrefixFilter
 *                  12.随机行过滤器RandomRowFilter
 *                附加过滤器
 *                  1.跳转过滤器SkipFilter, 当一列数据需要过滤掉时, 整行数据都被过滤掉
 *                  2.全匹配过滤器WhileMatchFilter,
 *
 *                FilterList实现Filter接口, 使过滤器可以组合使用
 *                  FilterList.Operator枚举值
 *                      1.MUST_PASS_ALL, 当所有过滤器都包含该值时, 结构彩绘返回
 *                      2.MUST_PASS_ONE, 当只要有一个过滤器满足时, 就返回数据
 *
 *                自定义过滤器: 实现Filter接口或者FilterBase类
 *
 *
 *
 *
 * HBase过滤器的实践, 熟悉各个过滤器的使用, 加深对过滤器的理解
 */

public class FilterPractice {
    private TableTool tableTool;

    public FilterPractice(String tableName) {
        tableTool = new TableTool(tableName);
    }

    public static void main(String[] args)throws IOException {
        String tableName = "FILTER_TABLE";
        FilterPractice practice = new FilterPractice(tableName);
        //测试HTable实例
//        practice.HTableTest();
//        practice.createTable();
//        practice.putData();

        //测试行健过滤器
//        practice.RowFilter();

        //测试列族过滤器
//        practice.FamilyFilter();

        //测试列过滤器
//        practice.QualifierFilter();

        //测试值过滤器
//        practice.ValueFilter();

        //参考列过滤器
//        practice.DependentColumnFilter();

        //测试单列值过滤器
//        practice.SingleColumnValueFilter();

        //测试参考列排除过滤器
//        practice.SingleColumnValueExcludeFilter();

        //前缀过滤器
//        practice.PrefixFilter();

        //测试分页过滤器
//        practice.PageFilter();

        //测试行健过滤器
        practice.KeyOnlyFilter();

        //测试首次行健过滤其
//        practice.FirstKeyOnlyFilter();

        //测试包含结束的过滤器
//        practice.InclusiveStopFilter();

        //测试列计数过滤器
//        practice.ColumnCountGetFilter();

        //测试列分页计数器
//        practice.ColumnPaginationFilter();

        //测试列前缀过滤器
//        practice.ColumnPrefixFilter();

        //测试随机行过滤
        practice.RandomRowFilter();
    }


    /**
     * 建立HBase表格, 并往表哥中添加数据
     * 共建立了5个列族
     * //删除表格
     * disable 'FILTER_TABLE'
     * drop 'FILTER_TABLE'
     */
    public  void createTable()throws IOException{
        HBaseAdmin admin = new HBaseAdmin(tableTool.getConf());
        HTableDescriptor descriptor = new HTableDescriptor(Bytes.toBytes(tableTool.getTableName()));
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("colfam1");
        descriptor.addFamily(columnDescriptor);
        columnDescriptor = new HColumnDescriptor(Bytes.toBytes("colfam2"));
        descriptor.addFamily(columnDescriptor);
        columnDescriptor = new HColumnDescriptor(Bytes.toBytes("colfam3"));
        descriptor.addFamily(columnDescriptor);
        columnDescriptor = new HColumnDescriptor(Bytes.toBytes("colfam4"));
        descriptor.addFamily(columnDescriptor);
        columnDescriptor = new HColumnDescriptor(Bytes.toBytes("colfam5"));
        descriptor.addFamily(columnDescriptor);
        admin.createTable(descriptor);

        boolean isSuccess = admin.isTableAvailable(Bytes.toBytes(tableTool.getTableName()));
        System.out.println("建表操作是否成功:" + isSuccess);
    }
    /**
     * 往表格中添加数据, 批量添加, 单个Put效率太低
     * 添加100条数据
     * get 'FILTER_TABLE', 'row-00' shell察看添加的数据
     */
    public void putData(){
        HTable table = tableTool.getTable();
        try {

            List<Put> list = new LinkedList<Put>();
            //数字左边自动补零
            NumberFormat numberFormat = NumberFormat.getInstance();
            numberFormat.setGroupingUsed(false);
            numberFormat.setMaximumIntegerDigits(2);
            numberFormat.setMinimumIntegerDigits(2);
            for (int i = 0; i < 100; i++){
                String rowKey = "row-" + numberFormat.format(i);
                Put put = new Put(Bytes.toBytes(rowKey));
                //每个列族添加5列数据
                for (int j = 1; j <= 5; j++){
                    for (int k = 1; k <= 5; k++){
                        put.add(Bytes.toBytes("colfam" + j), Bytes.toBytes("col" + k), Bytes.toBytes("value" + k));
                    }
                }
                list.add(put);
            }
            table.put(list);
            table.close();
            System.out.println("表格插入完毕");
            list.clear();
        }catch (IOException e){
            e.printStackTrace();
        }

    }

    /**
     * 操作HTable
     * 注意使用完table实例调用close方法, 隐士调用客户端的缓冲写操作
     */
    public void HTableTest()throws IOException{
        HTable table = tableTool.getTable();
        //获取表名
        System.out.println("表名" + table.getTableName());
        Configuration conf =  table.getConfiguration();
        System.out.println("HTable实例的配置" + conf);
        //表示一个表结构
        HTableDescriptor descriptor = table.getTableDescriptor();
        System.out.println(descriptor);
        //检查表在Zookeeper中是否启用
//        System.out.println(HTable.isTableEnabled(table.getTableName()));
        //得到startKey, 按照Region进行切分
        byte[][] startKeys = table.getStartKeys();
        System.out.println("输出表的起始行健");
        for (int i = 0; i < startKeys.length; i++){
            System.out.println(Bytes.toString(startKeys[i]));
        }
        //得到Endkey
        byte[][] stopKeys = table.getEndKeys();
        System.out.println("输出表的终止行健");
        for (int i = 0; i < startKeys.length; i++){
            System.out.println(Bytes.toString(startKeys[i]));
        }
        System.out.println("输出表的起始, 终止行健");
        //得到起始, 终止行健对
        Pair<byte[][], byte[][]> startEndKeys = table.getStartEndKeys();
        startKeys = startEndKeys.getFirst();
        stopKeys = startEndKeys.getSecond();
        for (int i = 0; i < startKeys.length; i++){
            System.out.println(Bytes.toString(startKeys[i]) + "\t" + Bytes.toString(stopKeys[i]));
        }

//        table.clearRegionCache();
        //得到Region信息
        HRegionLocation hRegionLocation = table.getRegionLocation("row-00");
        System.out.println("row-00的Region信息" + hRegionLocation);
        table.close();




    }

    /**
     * RowFilter 行健过滤器
     * 通过行健过滤器挑选特定的行
     */
    public void RowFilter()throws IOException{
        HTable table = tableTool.getTable();
        Scan scan = new Scan();
        //如果不指定列, 则返回所有的列
        scan.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col1"));
        Filter filter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("row-10")));
        scan.setFilter(filter);
        ResultScanner resultScanner = table.getScanner(scan);
        //返回的结果默认按字典排序
        for (Result result : resultScanner){
            System.out.println(result);
        }
        resultScanner.close();
    }

    /**
     * FamilyFilter列族过滤器
     * 返回每一行中符合条件的列族
     * 在Get 或者Scan中添加列族等条件相当于在服务器端进行了挑选, 减少了服务器向客户端数据量的传输, 减少得到的数据量
     * 过滤器相当于是更细力度的限制条件, 被过滤掉的数据不会被传送到客户端
     */
    public void FamilyFilter()throws IOException{
        HTable table = tableTool.getTable();
        Scan scan = new Scan();
        Filter filter = new FamilyFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(Bytes.toBytes("colfam3")));
        scan.setFilter(filter);
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner){
            System.out.println(result);
        }
        resultScanner.close();
        System.out.println("****************************************************");
        System.out.println("****************************************************");
        System.out.println("****************************************************");
        System.out.println("****************************************************");
        System.out.println("****************************************************");
        System.out.println("单行Get过滤器测试");
        Get get = new Get(Bytes.toBytes("row-00"));
//        get.setFilter(filter);
//        Result result = table.get(get);
//        System.out.println(result);
        Filter filter1 = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("colfam3")));
//        get.setFilter(filter1);
        get.addFamily(Bytes.toBytes("colfam5"));
        Result result = table.get(get);
        System.out.println(result);

    }

    /**
     * QualifierFilter 列名过滤器
     */
    public void QualifierFilter()throws IOException{
        System.out.println("列名过滤器测试");
        HTable table = tableTool.getTable();
        Scan scan = new Scan();
        Filter filter = new QualifierFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("col1")));
        scan.setFilter(filter);
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner){
            System.out.println(result);
        }
        resultScanner.close();
        System.out.println("****************************************************");
        System.out.println("****************************************************");
        System.out.println("****************************************************");
        System.out.println("****************************************************");
        System.out.println("****************************************************");
        System.out.println("单行Get过滤器测试");
        Get get = new Get(Bytes.toBytes("row-00"));
        get.setFilter(filter);
        Result result = table.get(get);
        System.out.println(result);

    }

    /**
     * 值过滤器
     */
    public void ValueFilter()throws IOException{
        HTable table = tableTool.getTable();
        Scan scan = new Scan();
        Filter filter = new ValueFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("value3")));
        scan.setFilter(filter);
        ResultScanner resultScanner = table.getScanner(scan);
        for(Result result : resultScanner){
            System.out.println(result);
        }
        resultScanner.close();
        System.out.println("****************************************************");
        System.out.println("****************************************************");
        System.out.println("****************************************************");
        System.out.println("****************************************************");
        System.out.println("****************************************************");
        Get get = new Get(Bytes.toBytes("row-00"));
        get.setFilter(filter);
        Result result = table.get(get);
        System.out.println(result);

    }

    /**
     * 参考列过滤器
     * 用户指定一列, 由这一列控制其他列的过滤, 使用参考列的时间戳
     * 可以理解成一个值过滤器和时间戳过滤器
     */
    public void DependentColumnFilter()throws IOException{
        System.out.println("参考列过滤器");
        //true或者false指定是否丢弃参考列, true为丢弃参考列
        Filter filter = new DependentColumnFilter(Bytes.toBytes("colfam3"), Bytes.toBytes("col3"), true,
                CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("value3")));
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner resultScanner = tableTool.getTable().getScanner(scan);
        for(Result result : resultScanner){
            System.out.println(result);
        }
        resultScanner.close();
        System.out.println("****************************************************");
        System.out.println("****************************************************");
        System.out.println("****************************************************");
        System.out.println("****************************************************");
        System.out.println("****************************************************");
        Get get = new Get(Bytes.toBytes("row-00"));
        get.setFilter(filter);
        Result result = tableTool.getTable().get(get);
        System.out.println(result);

    }

    /**
     * 单列值过滤器   SingleColumnValueFilter
     * 一列决定一行是否被过滤掉
     */
    public void SingleColumnValueFilter()throws IOException{
        HTable table = tableTool.getTable();
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("colfam1"), Bytes.toBytes("col3"),
                CompareFilter.CompareOp.NOT_EQUAL, new SubstringComparator("value3"));
        //当前参考列不存在时如何处理这一行, true表示过滤掉这一行
        filter.setFilterIfMissing(true);
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner){
            for (KeyValue kv : result.raw()){
                System.out.println(Bytes.toString(kv.getKey()) + "\t" + Bytes.toString(kv.getValue()));
            }
        }
        System.out.println("****************************************************");
        System.out.println("****************************************************");
        System.out.println("****************************************************");
        System.out.println("****************************************************");
        System.out.println("****************************************************");
        Get get = new Get(Bytes.toBytes("row-00"));
        get.setFilter(filter);
        Result result = tableTool.getTable().get(get);
        for (KeyValue kv : result.raw()){
            System.out.println(Bytes.toString(kv.getKey()) + "\t" + Bytes.toString(kv.getValue()));
        }
        resultScanner.close();
        table.close();

    }

    /**
     * 单列值排除过滤器, 参考列不会被包含在客户端的结果中
     */
    public void SingleColumnValueExcludeFilter()throws IOException{
//        System.out.println("----------");
        HTable table = tableTool.getTable();
        SingleColumnValueExcludeFilter filter = new SingleColumnValueExcludeFilter(Bytes.toBytes("colfam1"), Bytes.toBytes("col3"),
                CompareFilter.CompareOp.EQUAL, new SubstringComparator("value"));
        //当前参考列不存在时如何处理这一行, true表示过滤掉这一行
//        filter.setFilterIfMissing(true);
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner){
            for (KeyValue kv : result.raw()){
                System.out.println(Bytes.toString(kv.getKey()) + "\t" + Bytes.toString(kv.getValue()));
            }
        }
        System.out.println("****************************************************");
        System.out.println("****************************************************");
        System.out.println("****************************************************");
        System.out.println("****************************************************");
        System.out.println("****************************************************");
        Get get = new Get(Bytes.toBytes("row-00"));
        get.setFilter(filter);
        Result result = tableTool.getTable().get(get);
        for (KeyValue kv : result.raw()){
            System.out.println(Bytes.toString(kv.getKey()) + "\t" + Bytes.toString(kv.getValue()));
        }
        resultScanner.close();
        table.close();

    }

    /**
     * 前缀过滤器
     * 按照字典排序查找, 遇到被前缀大的行时扫描结束
     */
    public void PrefixFilter()throws IOException{
        HTable table = tableTool.getTable();
        Filter filter = new PrefixFilter(Bytes.toBytes("row-0"));
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner){
            System.out.println(result);
        }
        resultScanner.close();
        table.close();
    }

    /**
     * 分页过滤器
     * 对扫描结果按行进行分页, 需要指定PageSize参数, 控制每页返回的行数
     * 客户端代码会记录本次扫描的最后一行, 在下一次扫描时把上次扫描的最后一行最为当前起始行, 同时保持相同的过滤属性, 然后依次进行迭代
     *
     */
    public void PageFilter()throws IOException{

        byte[] POSTFIX = new byte[]{0x00};


        HTable table = tableTool.getTable();
        Filter filter = new PageFilter(10);
        int totalRows = 0;
        //上一次扫描的最后的RowKey
        byte[] lastRow = null;
        while (true){
            Scan scan = new Scan();
            scan.setFilter(filter);
            if (lastRow != null){
                byte[] startRow = Bytes.add(lastRow, POSTFIX);
                System.out.println("*************************************************");
                System.out.println("*************************************************");
                System.out.println("*************************************************");
                System.out.println("*************************************************");
                System.out.println("*************************************************");
                //HBase中的行健按照字典排序, 因此返回的结果也是如此排序, 并且其实行被包含在结果中
                //拼接一个0字节(长度为0的数组)到之前的行健, 这样可以保证最后返回的行在本论不被包含,
                System.out.println("本次扫描起始行健:\t" + Bytes.toString(startRow));
                scan.setStartRow(startRow);
            }
            ResultScanner resultScanner = table.getScanner(scan);
            //记录本次扫描的总记录数
            int localRows = 0;
            Result result;
            while ((result = resultScanner.next()) != null){
                System.out.println(localRows++ + ":" + result);
                totalRows++;
                lastRow = result.getRow();
            }
            resultScanner.close();
            if (localRows == 0){
                break;
            }
        }
        System.out.println("总共扫描的记录数: " + totalRows);
    }

    /**
     * 行健过滤器
     * 只返回KeyValue的Key,
     */
    public void KeyOnlyFilter()throws IOException{
        HTable table = tableTool.getTable();
        //默认为false, 当设置为true时, value的长度为原始长度(false时value的长度是0)
        Filter filter = new KeyOnlyFilter(true);
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner){
            for (Cell cell : result.rawCells()){
                System.out.println(cell + "\t" +
                        (cell.getValueLength() > 0 ? (Bytes.toInt(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength())) : "n/a"));
            }
        }
        scanner.close();
        table.close();

    }


    /**
     * 首次行健过滤器
     * 用户只需要访问一行中的第一列, 通常用来做行数统计, 只需检查这一行数据是否存在
     * 当当前这一行的第一列完成之后, 后通知Region服务器结束对当前行的扫描
     */
    public void FirstKeyOnlyFilter()throws IOException{
        HTable table = tableTool.getTable();
        Filter filter = new FirstKeyOnlyFilter();
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        int rowCount = 0;
        for (Result result : scanner){
            for (Cell cell : result.rawCells()) {
                System.out.println("Cell: " + cell + ", Value: " +
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                                cell.getValueLength()));
            }
            rowCount++;
        }
        System.out.println(rowCount);
        scanner.close();
        table.close();

    }

    /**
     * 包含结束的过滤器
     */
    public void InclusiveStopFilter()throws IOException{
        HTable table = tableTool.getTable();
        Filter filter = new InclusiveStopFilter(Bytes.toBytes("row-05"));
        Scan scan = new Scan();
        scan.setFilter(filter);
        scan.setStartRow(Bytes.toBytes("row-02"));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner){
            System.out.println(result);
        }
        scanner.close();
        table.close();
    }

    /**
     *时间戳过滤器
     * 当用户需要对扫描结果进行版本控制时, 用户需要传入一个装载时间戳的List实例
     * 版本:  一列数据在一个特定时间的值,
     */
    public void TimestampsFilter()throws IOException{
        HTable table = tableTool.getTable();
        //包含时间戳的List实例
        List<Long> timeStamps = new ArrayList<Long>();
        timeStamps.add(5L);
        timeStamps.add(10L);
        timeStamps.add(15L);
        //设置方法一, 返回5, 10, 15 对应的数据
        Filter filter = new TimestampsFilter(timeStamps);
        Scan scan = new Scan();
        scan.setFilter(filter);

        //加入范围界定, 因而之返回10对应的数据
        scan.setTimeRange(8, 12);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner){
            System.out.println(result);
        }
        scanner.close();
        table.close();

    }

    /**
     * 列限定计数器
     * 限定每行返回的列的限制
     * 当一行的列数达到限制时, 过滤器会停止整个扫描过程, 不太适合做Scan, 反而适合Get
     */
    public void ColumnCountGetFilter()throws IOException{
        HTable table = tableTool.getTable();
        //没一行最多返回3列数据, 由于都大于3, 因此只扫描一行就返回,
        Filter filter = new ColumnCountGetFilter(3);
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner){
            System.out.println(result);
        }
        scanner.close();
        table.close();
    }

    /**
     * 列分页计数器
     * 与pageFilter相类似
     * ColumnPaginationFilter(int limit, int offest)对一行的所有列进行分页, 跳过所有偏移量 < offset的列, 并包含之后所有偏移量在limit之前(包含limit)
     * 的列
     */
    public void ColumnPaginationFilter()throws IOException{
        HTable table = tableTool.getTable();
        //第15列开始包含两列
        Filter filter = new ColumnPaginationFilter(2, 15);
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner){
            System.out.println(result);
        }
        scanner.close();
        table.close();
    }

    /**
     * 列前缀过滤器
     * 类似与PrefixFilter, 对列名称进行前缀过滤
     */
    public void ColumnPrefixFilter()throws IOException{
        HTable table = tableTool.getTable();
        Filter filter = new ColumnPrefixFilter(Bytes.toBytes("col4"));
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner){
            System.out.println(result);
        }
        scanner.close();
        table.close();
    }

    /**
     * 随机行过滤器
     * 其实算是一个非常粗略的采样
     */
    public void RandomRowFilter()throws IOException{
        HTable table = tableTool.getTable();
        //0.0 -- 1.0 之间, > 1.0 代表数据包含所有的行
        Filter filter = new RandomRowFilter(0.2f);
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        int rowCount = 0;
        for (Result result : scanner){
            rowCount++;
        }
        System.out.println(rowCount);
        scanner.close();
        table.close();
    }

    /**
     * 跳转过滤器
     * 封装了一个用户提供的过滤器, 当被包装的过滤器遇到一个需要过滤的KeyValue实例时, 用户可以扩展并过滤整行数据
     * 即: 发现一行的一列需要过滤时, 过滤整行数据
     */
    public void SkipFilter(){
        Filter filter = new ValueFilter(CompareFilter.CompareOp.NOT_EQUAL, new BinaryComparator(Bytes.toBytes("value3")));
        Filter skipFilter = new SkipFilter(filter);

    }

    /**
     * 全匹配过滤器
     * 当一条数据被过滤掉时, 或结束整个扫描操作
     */
    public void WhileMatchFilter(){

    }










}
