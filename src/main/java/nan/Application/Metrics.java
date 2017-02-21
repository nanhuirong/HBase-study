package nan.Application;

/**
 * Created by nanhuirong on 16-5-18.
 */
public class Metrics {
    private String time;
    private String packets;
    private String bytes;
    private String links;

    public Metrics() {
    }

    public Metrics(String time, String packets, String bytes, String links) {
        this.time = time;
        this.packets = packets;
        this.bytes = bytes;
        this.links = links;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getPackets() {
        return packets;
    }

    public void setPackets(String packets) {
        this.packets = packets;
    }

    public String getBytes() {
        return bytes;
    }

    public void setBytes(String bytes) {
        this.bytes = bytes;
    }

    public String getLinks() {
        return links;
    }

    public void setLinks(String links) {
        this.links = links;
    }

    @Override
    public String toString() {
        return "time:" + time + "," + "packets:" + packets + "," + "bytes:" + bytes + "," + "links:" + links ;
    }
}
