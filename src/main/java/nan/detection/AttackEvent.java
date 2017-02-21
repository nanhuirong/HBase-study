package nan.detection;

/**
 * Created by nanhuirong on 16-5-12.
 * 攻击事件
 * 姚鑫师兄存储在HBASE上的数据格式,目前无法改变,基于flag标记位匹配攻击事件
 */
public class AttackEvent {
    public String date;
    public String srcIp;
    public String srcPort;
    public String dstIp;
    public String dstPort;
    public String protocal;
    public String flag;
    public String typeDescription;
    public String typeCode;

    public AttackEvent(String date, String srcIp, String srcPort, String dstIp, String dstPort, String protocal, String flag, String typeDescription, String typeCode) {
        this.date = date;
        this.srcIp = srcIp;
        this.srcPort = srcPort;
        this.dstIp = dstIp;
        this.dstPort = dstPort;
        this.protocal = protocal;
        this.flag = flag;
        this.typeDescription = typeDescription;
        this.typeCode = typeCode;
    }
}
