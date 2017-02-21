package nan.remote;

import io.socket.client.Socket;
import nan.Application.Metrics;
import nan.Application.Netflow;
import nan.detection.AttackEvent;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.List;

/**
 * Created by nanhuirong on 16-5-31.
 * 将数据推送到远程
 */
public class SendToRemote {
    /**
     * 将应用如53端口,一分钟统计一次(time, bytes, packets, links)
     * 以ApplicationDnsMetrics作为关键字发送给远程
     * @param metrics
     * @param socket
     * @throws Exception
     */
    public void sendApplicationMetricsToRemote(Metrics metrics, Socket socket)throws Exception{
        JSONObject object = new JSONObject();
        object.put("time", metrics.getTime());
        object.put("bytes", metrics.getBytes());
        object.put("packets", metrics.getPackets());
        object.put("links", metrics.getLinks());
        socket.emit("ApplicationDnsMetrics", object);
    }

    /**
     *将应用(如53) 的原始netFlow记录发送到远程(time, srcIp, srcPort, dstIp, dstPort, protocol)
     * keyWords ApplicationDnsNetflow
     * @param list
     * @param socket
     * @throws Exception
     */
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
    public void sendApplicationNetflowToRemote1(List<Netflow> list, Socket socket)throws Exception{

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
        socket.emit("ApplicationDnsNetflow1", jsonArray);
    }

    public void sendMetricsToRemote(nan.detection.Metrics metrics, Socket socket)throws Exception{
//        socket.emit("metrics", metrics.toString());
        JSONObject object = new JSONObject();
        object.put("timeWindow", metrics.getTimeWindow());
        object.put("record_creation_num", metrics.getAck_num());
        object.put("ack_src_top_20", metrics.getAck_src_large_20());
        object.put("ack_dst_top_20", metrics.getAck_dst_top_20());
        socket.emit("metrics", object);
    }

    /**
     * 将HBase上的攻击事件传输到远程
     * @param list
     * @throws Exception
     */
    public void sendAttackToRemote(List<AttackEvent> list, Socket socket)throws Exception{
        JSONObject object = new JSONObject();
        JSONArray jsonArray = new JSONArray();
        for (AttackEvent l : list){
            object.put("date", l.date);
            object.put("srcIp", l.srcIp);
            object.put("srcPort", l.srcPort);
            object.put("dstIp", l.dstIp);
            object.put("dstPort", l.dstPort);
            object.put("protocal", l.protocal);
            object.put("flag", l.flag);
            object.put("typeDescription", l.typeDescription);
            object.put("typeCode", l.typeCode);
            jsonArray.put(object);
        }
        socket.emit("events", jsonArray);
    }


}
