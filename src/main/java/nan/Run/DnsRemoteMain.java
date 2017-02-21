package nan.Run;

import io.socket.client.IO;
import io.socket.client.Socket;
import nan.Application.Metrics;
import nan.Application.Netflow;
import nan.hbase.StorageManagement;
import org.json.JSONArray;
import org.json.JSONObject;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;

/**
 * Created by nanhuirong on 16-5-19.
 * 将HBASE 中的Application_netflow Application_metrics 中的数据传递给前端
 */
public class DnsRemoteMain {
    public final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final String IP = "\\d+\\.\\d+\\.\\d+\\.\\d+";

    public static final String ENGLISH = "[A-Za-z]+$";


    public void sendApplicationMetricsToRemote(Metrics metrics, Socket socket)throws Exception{
        JSONObject object = new JSONObject();
        object.put("time", metrics.getTime());
        object.put("bytes", metrics.getBytes());
        object.put("packets", metrics.getPackets());
        object.put("links", metrics.getLinks());
        socket.emit("ApplicationDnsMetrics", object);

    }

    public void sendApplicationNetflowToRemote(List<Netflow> list, Socket socket)throws Exception{

        JSONArray jsonArray = new JSONArray();
        for (Netflow l: list){
            JSONObject object = new JSONObject();
            object.put("time", l.getTime());
            object.put("srcIp", l.getSrcIp());
            object.put("srcPort", l.getSrcPort());
            object.put("dstIp", l.getDstIp());
            object.put("dstPort", l.getDstPort());
            object.put("protocol", l.getProtocol());
            jsonArray.put(object);
        }
        socket.emit("ApplicationDnsNetflow", jsonArray);
    }

    public static void main(String[] args)throws Exception{
        Socket socket = IO.socket("http://59.67.152.239:5001");
        socket.connect();
        while (true){
            DnsRemoteMain dnsRemoteMain = new DnsRemoteMain();
            StorageManagement storageManagement = new StorageManagement();
            Calendar calendar = Calendar.getInstance();
            calendar.add(12, -4);
            String time = sdf.format(calendar.getTime());
            String[] timeSplit = time.split(" ");
//        String yearMonthDay = timeSplit[0];
            String yearMonthDay = "2016-05-14";
            String[] timeSplit1 = timeSplit[1].split(":");
            String hourMinute = timeSplit1[0] + ":" + timeSplit1[1] + ":" + "\\d{2}";
            String date = yearMonthDay + " " + hourMinute;
            List<Netflow> list = storageManagement.getApplicationNetflow(date + "-" + IP + "-" + IP + "-" + ENGLISH);
//            for (Netflow l: list){
//                System.out.println(l.getTime() + "," + l.getSrcIp() + "," + l.getSrcPort() + "," + l.getDstIp() + "," + l.getDstPort() + "," + l.getProtocol());
//            }
            dnsRemoteMain.sendApplicationNetflowToRemote(list, socket);
            Metrics metrics = storageManagement.getApplicationMetrics(date + "-53");
//            System.out.println(metrics.getTime() + "," + metrics.getPackets() + "," + metrics.getBytes() + "," + metrics.getLinks());
            dnsRemoteMain.sendApplicationMetricsToRemote(metrics, socket);
            Thread.sleep(60000);
        }




    }

}
