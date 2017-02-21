package nan.Run;

import io.socket.client.IO;
import io.socket.client.Socket;
import nan.Application.Metrics;
import nan.Application.Netflow;
import nan.hbase.StorageManagement;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;

/**
 * Created by nanhuirong on 16-5-31.
 */
public class businessMain {
    public final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final String IP = "\\d+\\.\\d+\\.\\d+\\.\\d+";
    public static final String ENGLISH = "[A-Za-z]+$";
    public static final String NUM = "\\d";


    public String[] getCurrentTime(){
        Calendar calendar = Calendar.getInstance();
        calendar.add(12, -8);
        String time = sdf.format(calendar.getTime());
        System.out.println(time);
        String[] timeSplit = time.split(" ");
        String yearMonthDay = timeSplit[0];
//        String yearMonthDay = "2016-05-14";
        String[] timeSplit1 = timeSplit[1].split(":");
        String hourMinute = timeSplit1[0] + ":" + timeSplit1[1] + ":" + "\\d{2}";
        String hourMinute1 = timeSplit1[0] + ":" + timeSplit1[1] + ":" + "00";
        String[] date = new String[2];
        date[0] = yearMonthDay + " " + hourMinute;
        date[1] = yearMonthDay + " " + hourMinute1;
        return date;

    }
//    public void getTop10IpLinks(List<Netflow> list){
//        Map<>
//    }
    public static void main(String[] args)throws Exception{
        Socket socket = IO.socket("http://59.67.152.239:5001");
        socket.connect();
        businessMain bus = new businessMain();
        while (true){
            StorageManagement storage = new StorageManagement();
//            SendToRemote send = new SendToRemote();
            String[] date = bus.getCurrentTime();
//            //将攻击事件传递给前台
//            String attackEventRegex = IP + "-" + IP + "-" + date[0] + "-" + NUM;
//            List<AttackEvent> attackEventList = storage.getAttackFromHbase(attackEventRegex);
//            System.out.println(attackEventList.size());
////            send.sendAttackToRemote(attackEventList, socket);
//            //将网络统计数据传递给前台
//            String attackMetricsRegex = date[1];
//            nan.detection.Metrics attackMetrics = storage.getMetricsFromHbae(attackMetricsRegex);
////            System.out.println(attackMetrics.toString());
////            send.sendMetricsToRemote(attackMetrics, socket);
//            //将应用统计数据传递给前台
            String applicationMetricsRegex = date[1] + "-53";
            System.out.println(applicationMetricsRegex);
            Metrics applicationMetrics = storage.getApplicationMetrics(applicationMetricsRegex);
            System.out.println(applicationMetrics.getTime() + "\t" + applicationMetrics.getPackets() + "\t" + applicationMetrics.getBytes() + "\t" + applicationMetrics.getLinks());
//            System.out.println(applicationMetrics.toString());
//            send.sendApplicationMetricsToRemote(applicationMetrics, socket);
            //将原始netflow 53的数据传递给前台
            String applicationNetflowRegex = date[0] + "-" + IP + "-" + IP + "-" + ENGLISH;
//            String applicationNetflowRegex = date[0] + "-" + IP + "-" + IP + "-" + ENGLISH;
            List<Netflow> applicationNetflow = storage.getApplicationNetflow(applicationNetflowRegex);
            System.out.println(applicationNetflow.size());
//            for (Netflow netflow: applicationNetflow){
//                System.out.println(netflow.getTime() + "\t" + netflow.getSrcIp() + "\t" +  netflow.getSrcPort() + "\t" + netflow.getDstIp() + "\t" + netflow.getDstPort() + "\t" + netflow.getProtocol());
//            }
//            send.sendApplicationNetflowToRemote(applicationNetflow, socket);
//            send.sendApplicationNetflowToRemote1(applicationNetflow, socket);
            Thread.sleep(60000);
        }

    }
}
