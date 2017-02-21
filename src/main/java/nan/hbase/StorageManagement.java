package nan.hbase;

import nan.Application.Metrics;
import nan.Application.Netflow;
import nan.detection.AttackEvent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by nanhuirong on 16-5-18.
 */
public class StorageManagement {
    public final static String EDU_ATTACK_EVENT = "edu_attack_event";
    public final static String EDU_REALTIME_DDOS_METRICS = "edu_realtime_ddos_metrics";
    public static final String APPLICATION_NETFLOW = "Application_netflow";
    public static final String APPLICATION_METRICS = "Application_metrics";
    public final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static Configuration conf = null;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "311b-node-2,310b-node-1");
    }

    /**
     * 将DNS 的netflow 五元组存储到HBASE
     * @param netflow
     * @throws Exception
     */
    public void addApplicationNetflow(Netflow netflow)throws Exception{
        HTable table = new HTable(conf, APPLICATION_NETFLOW);
        String rowKey = netflow.getTime() + "-" + netflow.getSrcIp() + "-" + netflow.getDstIp() + "-" + netflow.getProtocol();
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes("default_cf"), Bytes.toBytes("time"), Bytes.toBytes(String.valueOf(netflow.getTime())));
        put.add(Bytes.toBytes("default_cf"), Bytes.toBytes("srcIp"), Bytes.toBytes(String.valueOf(netflow.getSrcIp())));
        put.add(Bytes.toBytes("default_cf"), Bytes.toBytes("srcPort"), Bytes.toBytes(String.valueOf(netflow.getSrcPort())));
        put.add(Bytes.toBytes("default_cf"), Bytes.toBytes("dstIp"), Bytes.toBytes(String.valueOf(netflow.getDstIp())));
        put.add(Bytes.toBytes("default_cf"), Bytes.toBytes("dstPort"), Bytes.toBytes(String.valueOf(netflow.getDstPort())));
        put.add(Bytes.toBytes("default_cf"), Bytes.toBytes("protocal"), Bytes.toBytes(String.valueOf(netflow.getProtocol())));
        table.put(put);
    }

    /**
     * 将DNS 的netFlow 五元组从HBase 取出
     * @param regex
     * @return
     * @throws Exception
     */
    public List<Netflow> getApplicationNetflow(String regex)throws Exception{
        HTable table = new HTable(conf, APPLICATION_NETFLOW);
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regex));
        List<Netflow> list = new ArrayList<Netflow>();
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner resultScanner = table.getScanner(scan);
        for(Result res : resultScanner){
            String time = Bytes.toString(res.getValue(Bytes.toBytes("default_cf"),  Bytes.toBytes("time")));
            String srcIp = Bytes.toString(res.getValue(Bytes.toBytes("default_cf"),  Bytes.toBytes("srcIp")));
            String srcPort = Bytes.toString(res.getValue(Bytes.toBytes("default_cf"),  Bytes.toBytes("srcPort")));
            String dstIp = Bytes.toString(res.getValue(Bytes.toBytes("default_cf"),  Bytes.toBytes("dstIp")));
            String dstPort = Bytes.toString(res.getValue(Bytes.toBytes("default_cf"),  Bytes.toBytes("dstPort")));
            String protocol = Bytes.toString(res.getValue(Bytes.toBytes("default_cf"),  Bytes.toBytes("protocal")));
            Netflow netflow = new Netflow(time, srcIp, srcPort, dstIp, dstPort, protocol);
            list.add(netflow);
        }
        return list;
    }

    /**
     * 将应用(53)的一分钟统计添加到HBase
     * @param metrics
     * @throws Exception
     */
    public void addApplicationMetrics(Metrics metrics)throws Exception{
        HTable table = new HTable(conf, APPLICATION_METRICS);
        String rowKey = metrics.getTime() + "-" + "53";
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes("default_cf"), Bytes.toBytes("time"), Bytes.toBytes(String.valueOf(metrics.getTime())));
        put.add(Bytes.toBytes("default_cf"), Bytes.toBytes("bytes"), Bytes.toBytes(String.valueOf(metrics.getBytes())));
        put.add(Bytes.toBytes("default_cf"), Bytes.toBytes("packets"), Bytes.toBytes(String.valueOf(metrics.getPackets())));
        put.add(Bytes.toBytes("default_cf"), Bytes.toBytes("links"), Bytes.toBytes(String.valueOf(metrics.getLinks())));
        table.put(put);
    }

    /**
     * 将应用(如53)的原始NetFlow数据从Hbase取出
     * @param regex
     * @return
     * @throws Exception
     */
    public Metrics getApplicationMetrics(String regex) throws Exception{
        Metrics metrics = new Metrics();
        HTable table = new HTable(conf, APPLICATION_METRICS);
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regex));
//        List<Metrics> list = new ArrayList<Metrics>();
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner resultScanner = table.getScanner(scan);
        for(Result res : resultScanner){
            String time = Bytes.toString(res.getValue(Bytes.toBytes("default_cf"), Bytes.toBytes("time")));
            String bytes = Bytes.toString(res.getValue(Bytes.toBytes("default_cf"), Bytes.toBytes("bytes")));
            String packets = Bytes.toString(res.getValue(Bytes.toBytes("default_cf"), Bytes.toBytes("packets")));
            String links = Bytes.toString(res.getValue(Bytes.toBytes("default_cf"), Bytes.toBytes("links")));
            metrics = new Metrics(time, bytes, packets, links);
//            list.add(metrics);
        }
        return metrics;
    }

    /**
     * 将攻击时间从hbase取出
     * @return
     * @throws Exception
     */
    public List<AttackEvent> getAttackFromHbase(String regex)throws Exception{
        List<AttackEvent> list = new ArrayList<AttackEvent>();
        HTable table = new HTable(conf, EDU_ATTACK_EVENT);
        Scan scan = new Scan();
//        String ip = "\\d+\\.\\d+\\.\\d+\\.\\d+";
//        String dateTmp = sdf.format(new Date());
////        String date ="2016-03-18 \\d{2}:\\d{2}:\\d{2}";
//        /**
//         * 3-18 系统当前时间
//         */
//        String currentDate = sdf.format(new Date());
//        String split = currentDate.split(" ")[1];
//        String[] split1 = split.split(":");
//        String split2 = split1[0] + ":" +split1[1];
////        String date = "2016-03-18 16:40:\\d{2}";
////        String date = "2016-03-18 " + split2 + ":\\d{2}";
//        /**
//         * 当前时间提前4分钟
//         */
////        Calendar calendar = Calendar.getInstance();
////        calendar.add(12, -4);
////        String time = sdf.format(calendar.getTime());
////        String[] timeSplit = time.split(" ");
////        String yearMonthDay = timeSplit[0];
////        String[] timeSplit1 = timeSplit[1].split(":");
////        String hourMinute = timeSplit1[0] + ":" + timeSplit1[1] + ":" + "\\d{2}";
////        String date = yearMonthDay + " " + hourMinute;
//        String date = "2016-05-15 14:\\{2}:\\{2}";
//
//        String num = "\\d";
//        String regex = ip + "-" + ip + "-" + date + "-" + num;
        //96.46.1.193-59.67.159.71-2016-05-11 09:49:38-3
        //\d+\.\d+\.\d+\.\d+-\d+\.\d+\.\d+\.\d+-2015-05-11 \\d{2}:\\d{2}:\\d{2}-\\d
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regex));
        scan.setFilter(filter);
        ResultScanner resultScanner = table.getScanner(scan);
        for(Result res : resultScanner){
//            System.out.println("------------------------------------");
//            for(KeyValue kv : res.list()){
//                String rowKey = Bytes.toString(kv.getRow());
//                String cf = Bytes.toString(kv.getFamily());
//                System.out.println(kv);
//            }
//            System.out.println("------------------------------------");
//            System.out.println(res);
            byte[] res_date = res.getValue(Bytes.toBytes("default_cf"), Bytes.toBytes("date"));
            byte[] res_srcIp = res.getValue(Bytes.toBytes("default_cf"), Bytes.toBytes("srcIp"));
            byte[] res_srcPort = res.getValue(Bytes.toBytes("default_cf"), Bytes.toBytes("srcPort"));
            byte[] res_dstIp = res.getValue(Bytes.toBytes("default_cf"), Bytes.toBytes("dstIp"));
            byte[] res_dstPort = res.getValue(Bytes.toBytes("default_cf"), Bytes.toBytes("dstPort"));
            byte[] res_protocal = res.getValue(Bytes.toBytes("default_cf"), Bytes.toBytes("protocal"));
            byte[] res_flag = res.getValue(Bytes.toBytes("default_cf"), Bytes.toBytes("flag"));
            byte[] res_typeDescription = res.getValue(Bytes.toBytes("default_cf"), Bytes.toBytes("typeDescription"));
            byte[] res_typeCode = res.getValue(Bytes.toBytes("default_cf"), Bytes.toBytes("typeCode"));
            String record = Bytes.toString(res_date) + "|" +
                    Bytes.toString(res_srcIp) + "|" +
                    Bytes.toString(res_srcPort) + "|" +
                    Bytes.toString(res_dstIp) + "|" +
                    Bytes.toString(res_dstPort) + "|" +
                    Bytes.toString(res_protocal) + "|" +
                    Bytes.toString(res_typeDescription) + "|" +
                    Bytes.toString(res_typeCode);
//            System.out.println(record);

            String date1 = Bytes.toString(res_date);
            String srcIp =  Bytes.toString(res_srcIp);
            String srcPort = Bytes.toString(res_srcPort);
            String dstIp = Bytes.toString(res_dstIp);
            String dstPort = Bytes.toString(res_dstPort);
            String protocal = Bytes.toString(res_protocal);
            String flag = Bytes.toString(res_flag);
            String typeDescription = Bytes.toString(res_typeDescription);
            String typeCode = Bytes.toString(res_typeCode);
            AttackEvent event = new AttackEvent(date1, srcIp, srcPort, dstIp, dstPort, protocal, flag, typeDescription, typeCode);
            list.add(event);
        }
        resultScanner.close();
        return list;
    }

    /**
     * 将Hbase一分钟流量的统计值取出
     * @return
     * @throws Exception
     */
    public nan.detection.Metrics getMetricsFromHbae(String regex)throws Exception{
        /**
         * 当前时间提前4分钟
         */
//        Calendar calendar = Calendar.getInstance();
//        calendar.add(12, -4);
//        String time = sdf.format(calendar.getTime());
//        String[] timeSplit = time.split(" ");
//        String yearMonthDay = timeSplit[0];
//        String[] timeSplit1 = timeSplit[1].split(":");
//        String hourMinute = timeSplit1[0] + ":" + timeSplit1[1] + ":" + "00";
//        String date = yearMonthDay + " " + hourMinute;

//        /**
//         * 2016-05-13 09:30:00
//         * 测试时间
//         */
//        String date = "2016-05-13 09:30:00";

        HTable table = new HTable(conf, EDU_REALTIME_DDOS_METRICS);
        Get get = new Get(Bytes.toBytes(regex));
        Result result = table.get(get);
        String timeWindow = Bytes.toString(result.getValue(Bytes.toBytes("default"), Bytes.toBytes("time_window")));
        int record_creation_num = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("default"), Bytes.toBytes("record_creation_num"))));

        int ntp_num = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("udp"), Bytes.toBytes("ntp_num"))));
        double ntp_to_flow_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("udp"), Bytes.toBytes("ntp_to_flow_ratio"))));
        double ntp_to_udp_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("udp"), Bytes.toBytes("ntp_to_udp_ratio"))));
        String ntp_src_top_20 = Bytes.toString(result.getValue(Bytes.toBytes("udp"), Bytes.toBytes("ntp_src_top_20")));
        String ntp_dst_top_20 = Bytes.toString(result.getValue(Bytes.toBytes("udp"), Bytes.toBytes("ntp_dst_top_20")));

        int syn_num = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("syn_num"))));

        double syn_to_flow_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("syn_to_flow_ratio"))));
        double syn_to_tcp_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("syn_to_tcp_ratio"))));
        int syn_src_less_5 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("syn_src_less_5"))));
        int syn_src_less_10 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("syn_src_less_10"))));
        int syn_src_less_20 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("syn_src_less_20"))));
        int syn_src_large_20 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("syn_src_large_20"))));
        int syn_dst_less_5 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("syn_dst_less_5"))));
        int syn_dst_less_10 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("syn_dst_less_10"))));
        int syn_dst_less_20 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("syn_dst_less_20"))));
        int syn_dst_large_20 =Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("syn_dst_large_20"))));
        double syn_src_less_5_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("syn_src_less_5_ratio"))));
        double syn_src_less_10_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("syn_src_less_10_ratio"))));
        double syn_src_less_20_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("syn_src_less_20_ratio"))));
        double syn_src_large_20_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("syn_src_large_20_ratio"))));
        double syn_dst_less_5_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("syn_dst_less_5_ratio"))));
        double syn_dst_less_10_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("syn_dst_less_10_ratio"))));
        double syn_dst_less_20_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("syn_dst_less_20_ratio"))));
        double syn_dst_large_20_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("syn_dst_large_20_ratio"))));
        String syn_src_top_20 = Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("syn_src_top_20")));
        String syn_dst_top_20 = Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("syn_dst_top_20")));

        int ack_num = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("ack_num"))));
        double ack_to_flow_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("ack_to_flow_ratio"))));
        double ack_to_tcp_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("ack_to_tcp_ratio"))));
        int ack_src_less_5 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("ack_src_less_5"))));
        int ack_src_less_10 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("ack_src_less_10"))));
        int ack_src_less_20 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("ack_src_less_20"))));
        int ack_src_large_20 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("ack_src_large_20"))));
        int ack_dst_less_5 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("ack_dst_less_5"))));
        int ack_dst_less_10 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("ack_dst_less_10"))));
        int ack_dst_less_20 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("ack_dst_less_20"))));
        int ack_dst_large_20 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("ack_dst_large_20"))));
        double ack_src_less_5_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("ack_src_less_5_ratio"))));
        double ack_src_less_10_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("ack_src_less_10_ratio"))));
        double ack_src_less_20_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("ack_src_less_20_ratio"))));
        double ack_src_large_20_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("ack_src_large_20_ratio"))));
        double ack_dst_less_5_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("ack_dst_less_5_ratio"))));
        double ack_dst_less_10_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("ack_dst_less_10_ratio"))));
        double ack_dst_less_20_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("ack_dst_less_20_ratio"))));
        double ack_dst_large_20_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("ack_dst_large_20_ratio"))));
        String ack_src_top_20 = Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("ack_src_top_20")));
        String ack_dst_top_20 = Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("ack_dst_top_20")));


        int fin_num = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("fin_num")))) ;
        double fin_to_flow_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("fin_to_flow_ratio"))));
        double fin_to_tcp_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("fin_to_tcp_ratio"))));
        int fin_src_less_5 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("fin_src_less_5"))));
        int fin_src_less_10 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("fin_src_less_10"))));
        int fin_src_less_20 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("fin_src_less_20"))));
        int fin_src_large_20 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("fin_src_large_20"))));
        int fin_dst_less_5 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("fin_dst_less_5"))));
        int fin_dst_less_10 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("fin_dst_less_10"))));
        int fin_dst_less_20 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("fin_dst_less_20"))));
        int fin_dst_large_20 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("fin_dst_large_20"))));
        double fin_src_less_5_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("fin_src_less_5_ratio"))));
        double fin_src_less_10_ratio =Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("fin_src_less_10_ratio"))));
        double fin_src_less_20_ratio =Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("fin_src_less_20_ratio"))));
        double fin_src_large_20_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("fin_src_large_20_ratio"))));
        double fin_dst_less_5_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("fin_dst_less_5_ratio"))));
        double fin_dst_less_10_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("fin_dst_less_10_ratio"))));
        double fin_dst_less_20_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("fin_dst_less_20_ratio"))));
        double fin_dst_large_20_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("fin_dst_large_20_ratio"))));
        String fin_src_top_20 = Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("fin_src_top_20")));
        String fin_dst_top_20 = Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("fin_dst_top_20")));


        int udp_num = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("udp"), Bytes.toBytes("udp_num"))));
//        System.out.println("<----------------------udp-num------->" + udp_num);
        double udp_to_flow_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("udp"), Bytes.toBytes("udp_to_flow_ratio"))));
//        System.out.println("<----------------------udp_to_flow_ratio------->" + udp_to_flow_ratio);
        int udp_src_less_5 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("udp"), Bytes.toBytes("udp_src_less_5"))));
//        System.out.println("<----------------------udp_src_less_5------->" + udp_src_less_5);
        int udp_src_less_10 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("udp"), Bytes.toBytes("udp_src_less_10"))));
        int udp_src_less_20 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("udp"), Bytes.toBytes("udp_src_less_20"))));
        int udp_src_large_20 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("udp"), Bytes.toBytes("udp_src_large_20"))));
        int udp_dst_less_5 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("udp"), Bytes.toBytes("udp_dst_less_5"))));
        int udp_dst_less_10 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("udp"), Bytes.toBytes("udp_dst_less_10"))));
        int udp_dst_less_20 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("udp"), Bytes.toBytes("udp_dst_less_20"))));
        int udp_dst_large_20 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("udp"), Bytes.toBytes("udp_dst_large_20"))));
        double udp_src_less_5_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("udp"), Bytes.toBytes("udp_src_less_5_ratio"))));
        double udp_src_less_10_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("udp"), Bytes.toBytes("udp_src_less_10_ratio"))));
        double udp_src_less_20_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("udp"), Bytes.toBytes("udp_src_less_20_ratio"))));
        double udp_src_large_20_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("udp"), Bytes.toBytes("udp_src_large_20_ratio"))));
        double udp_dst_less_5_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("udp"), Bytes.toBytes("udp_dst_less_5_ratio"))));
        double udp_dst_less_10_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("udp"), Bytes.toBytes("udp_dst_less_10_ratio"))));
        double udp_dst_less_20_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("udp"), Bytes.toBytes("udp_dst_less_20_ratio"))));
        double udp_dst_large_20_ratio =Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("udp"), Bytes.toBytes("udp_dst_large_20_ratio"))));
        String udp_src_top_20 = Bytes.toString(result.getValue(Bytes.toBytes("udp"), Bytes.toBytes("udp_src_top_20")));
        String udp_dst_top_20 = Bytes.toString(result.getValue(Bytes.toBytes("udp"), Bytes.toBytes("udp_dst_top_20")));


        int icmp_num = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("icmp"), Bytes.toBytes("icmp_num"))));
        double icmp_to_flow_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("icmp"), Bytes.toBytes("icmp_to_flow_ratio"))));
        int icmp_src_less_5 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("icmp"), Bytes.toBytes("icmp_src_less_5"))));
        int icmp_src_less_10 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("icmp"), Bytes.toBytes("icmp_src_less_10"))));
        int icmp_src_less_20 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("icmp"), Bytes.toBytes("icmp_src_less_20"))));
        int icmp_src_large_20 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("icmp"), Bytes.toBytes("icmp_src_large_20"))));
        int icmp_dst_less_5 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("icmp"), Bytes.toBytes("icmp_dst_less_5"))));
        int icmp_dst_less_10 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("icmp"), Bytes.toBytes("icmp_dst_less_10"))));
        int icmp_dst_less_20 =Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("icmp"), Bytes.toBytes("icmp_dst_less_20"))));
        int icmp_dst_large_20 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("icmp"), Bytes.toBytes("icmp_dst_large_20"))));
        double icmp_src_less_5_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("icmp"), Bytes.toBytes("icmp_src_less_5_ratio"))));
        double icmp_src_less_10_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("icmp"), Bytes.toBytes("icmp_src_less_10_ratio"))));
        double icmp_src_less_20_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("icmp"), Bytes.toBytes("icmp_src_less_20_ratio"))));
        double icmp_src_large_20_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("icmp"), Bytes.toBytes("icmp_src_large_20_ratio"))));
        double icmp_dst_less_5_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("icmp"), Bytes.toBytes("icmp_dst_less_5_ratio"))));
        double icmp_dst_less_10_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("icmp"), Bytes.toBytes("icmp_dst_less_10_ratio"))));
        double icmp_dst_less_20_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("icmp"), Bytes.toBytes("icmp_dst_less_20_ratio"))));
        double icmp_dst_large_20_ratio =Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("icmp"), Bytes.toBytes("icmp_dst_large_20_ratio"))));
        String icmp_src_top_20 = Bytes.toString(result.getValue(Bytes.toBytes("icmp"), Bytes.toBytes("icmp_src_top_20")));
        String icmp_dst_top_20 = Bytes.toString(result.getValue(Bytes.toBytes("icmp"), Bytes.toBytes("icmp_dst_top_20")));


        int inval_flag_num = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("inval_flag_num"))));
        double inval_flag_to_flow_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("inval_flag_to_flow_ratio"))));
        double inval_flag_to_tcp_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("inval_flag_to_tcp_ratio"))));
        int inval_flag_src_less_5 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("inval_flag_src_less_5"))));
        int inval_flag_src_less_10 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("inval_flag_src_less_10"))));
        int inval_flag_src_less_20 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("inval_flag_src_less_20"))));
        int inval_flag_src_large_20 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("inval_flag_src_large_20"))));
        int inval_flag_dst_less_5 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("inval_flag_dst_less_5"))));
        int inval_flag_dst_less_10 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("inval_flag_dst_less_10"))));
        int inval_flag_dst_less_20 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("inval_flag_dst_less_20"))));
        int inval_flag_dst_large_20 = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("inval_flag_dst_large_20"))));
        double inval_flag_src_less_5_ratio =Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("inval_flag_src_less_5_ratio"))));
        double inval_flag_src_less_10_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("inval_flag_src_less_10_ratio"))));
        double inval_flag_src_less_20_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("inval_flag_src_less_20_ratio"))));
        double inval_flag_src_large_20_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("inval_flag_src_large_20_ratio"))));
        double inval_flag_dst_less_5_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("inval_flag_dst_less_5_ratio"))));
        double inval_flag_dst_less_10_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("inval_flag_dst_less_10_ratio"))));
        double inval_flag_dst_less_20_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("inval_flag_dst_less_20_ratio"))));
        double inval_flag_dst_large_20_ratio = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("inval_flag_dst_large_20_ratio"))));
        String inval_flag_src_top_20 = Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("inval_flag_src_top_20")));
        String inval_flag_dst_top_20 = Bytes.toString(result.getValue(Bytes.toBytes("tcp"), Bytes.toBytes("inval_flag_dst_top_20")));

        nan.detection.Metrics metrics = new nan.detection.Metrics(timeWindow, record_creation_num, ntp_num, ntp_to_flow_ratio,
                ntp_to_udp_ratio, ntp_src_top_20, ntp_dst_top_20, syn_num,
                syn_to_flow_ratio,  syn_to_tcp_ratio, syn_src_less_5,  syn_src_less_10,
                syn_src_less_20,  syn_src_large_20, syn_dst_less_5,  syn_dst_less_10,
                syn_dst_less_20,  syn_dst_large_20,  syn_src_less_5_ratio, syn_src_less_10_ratio,
                syn_src_less_20_ratio,  syn_src_large_20_ratio,  syn_dst_less_5_ratio,
                syn_dst_less_10_ratio,  syn_dst_less_20_ratio,  syn_dst_large_20_ratio,
                syn_src_top_20,  syn_dst_top_20,  ack_num,  ack_to_flow_ratio,
                ack_to_tcp_ratio, ack_src_less_5,  ack_src_less_10, ack_src_less_20,
                ack_src_large_20,  ack_dst_less_5, ack_dst_less_10,  ack_dst_less_20,
                ack_dst_large_20,  ack_src_less_5_ratio,  ack_src_less_10_ratio,
                ack_src_less_20_ratio,  ack_src_large_20_ratio,  ack_dst_less_5_ratio,
                ack_dst_less_10_ratio,  ack_dst_less_20_ratio,  ack_dst_large_20_ratio,
                ack_src_top_20,  ack_dst_top_20,  fin_num,  fin_to_flow_ratio,
                fin_to_tcp_ratio, fin_src_less_5,  fin_src_less_10, fin_src_less_20,
                fin_src_large_20,  fin_dst_less_5,  fin_dst_less_10,  fin_dst_less_20,
                fin_dst_large_20, fin_src_less_5_ratio,  fin_src_less_10_ratio,
                fin_src_less_20_ratio, fin_src_large_20_ratio, fin_dst_less_5_ratio,
                fin_dst_less_10_ratio, fin_dst_less_20_ratio, fin_dst_large_20_ratio,
                fin_src_top_20, fin_dst_top_20,  udp_num,  udp_to_flow_ratio,  udp_src_less_5,
                udp_src_less_10,  udp_src_less_20,  udp_src_large_20, udp_dst_less_5,  udp_dst_less_10,
                udp_dst_less_20, udp_dst_large_20,  udp_src_less_5_ratio,  udp_src_less_10_ratio,
                udp_src_less_20_ratio,  udp_src_large_20_ratio,  udp_dst_less_5_ratio,
                udp_dst_less_10_ratio,  udp_dst_less_20_ratio,  udp_dst_large_20_ratio,
                udp_src_top_20, udp_dst_top_20, icmp_num,  icmp_to_flow_ratio,
                icmp_src_less_5, icmp_src_less_10,  icmp_src_less_20,  icmp_src_large_20,
                icmp_dst_less_5, icmp_dst_less_10, icmp_dst_less_20,  icmp_dst_large_20,
                icmp_src_less_5_ratio,  icmp_src_less_10_ratio, icmp_src_less_20_ratio,
                icmp_src_large_20_ratio,  icmp_dst_less_5_ratio,  icmp_dst_less_10_ratio,
                icmp_dst_less_20_ratio, icmp_dst_large_20_ratio, icmp_src_top_20, icmp_dst_top_20,
                inval_flag_num, inval_flag_to_flow_ratio, inval_flag_to_tcp_ratio,  inval_flag_src_less_5,
                inval_flag_src_less_10,  inval_flag_src_less_20,  inval_flag_src_large_20,  inval_flag_dst_less_5,
                inval_flag_dst_less_10, inval_flag_dst_less_20,  inval_flag_dst_large_20, inval_flag_src_less_5_ratio,
                inval_flag_src_less_10_ratio, inval_flag_src_less_20_ratio,  inval_flag_src_large_20_ratio,
                inval_flag_dst_less_5_ratio, inval_flag_dst_less_10_ratio,  inval_flag_dst_less_20_ratio,
                inval_flag_dst_large_20_ratio, inval_flag_src_top_20, inval_flag_dst_top_20);
//        System.out.println(metrics.toString());

        return metrics;
    }

}
