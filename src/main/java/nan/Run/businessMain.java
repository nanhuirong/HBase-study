package nan.Run;

import io.socket.client.IO;
import io.socket.client.Socket;
import nan.Application.Metrics;
import nan.Application.Netflow;
import nan.hbase.StorageManagement;
import nan.remote.SendToRemote;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
//        String yearMonthDay = timeSplit[0];
        String yearMonthDay = "2016-05-14";
        String[] timeSplit1 = timeSplit[1].split(":");
        String hourMinute = timeSplit1[0] + ":" + timeSplit1[1] + ":" + "\\d{2}";
        String hourMinute1 = timeSplit1[0] + ":" + timeSplit1[1] + ":" + "00";
        String[] date = new String[2];
        date[0] = yearMonthDay + " " + hourMinute;
        date[1] = yearMonthDay + " " + hourMinute1;
        return date;

    }

    private boolean isHTTP(String srcPort, String destPort){
        return (srcPort.equals("80") ||
                srcPort.equals("8080") ||
                destPort.equals("80") ||
                destPort.equals("8080")) ? true : false;
    }

    private boolean isDNS(String srcPort, String destPort){
        return (srcPort.equals("53") ||
                destPort.equals("53")) ? true : false;
    }
//    public void getTop10IpLinks(List<Netflow> list){
//        Map<>
//    }
    public static void main(String[] args)throws Exception{
        Socket socketDNS = IO.socket("http://59.67.152.230:5001");
        Socket socketHTTP = IO.socket("http://59.67.152.230:5002");
        socketDNS.connect();
        socketHTTP.connect();
        businessMain bus = new businessMain();
        List<Netflow> dns = new ArrayList<Netflow>();
        List<Netflow> http = new ArrayList<Netflow>();
        while (true){
            StorageManagement storage = new StorageManagement();
            SendToRemote send = new SendToRemote();
            String[] date = bus.getCurrentTime();
            String applicationMetricsRegex = date[1] + "-53";
            System.out.println(applicationMetricsRegex);
            Metrics applicationMetrics = storage.getApplicationMetrics(applicationMetricsRegex);
            System.out.println(applicationMetrics.toString());
            send.sendApplicationMetricsToRemote(applicationMetrics, socketDNS);
            send.sendApplicationMetricsToRemote(applicationMetrics, socketHTTP);
            //将原始netflow 53的数据传递给前台
            String applicationNetflowRegex = date[0] + "-" + IP + "-" + IP + "-" + ENGLISH;
            List<Netflow> applicationNetflow = storage.getApplicationNetflow(applicationNetflowRegex);
            for (Netflow netflow : applicationNetflow){
                if (bus.isDNS(netflow.getSrcPort(), netflow.getDstPort())){
                    dns.add(netflow);
                }
                if (bus.isHTTP(netflow.getSrcPort(), netflow.getDstPort())){
                    http.add(netflow);
                }
            }
            System.out.println(applicationNetflow.size());
            send.sendApplicationNetflowToRemote(dns, socketDNS);
            send.sendApplicationNetflowToRemote(http, socketHTTP);
            dns.clear();
            http.clear();
            Thread.sleep(60000);
        }

    }
}
