/**
 * Created by leo on 10/12/16.
 */
public class StringAndInt implements Comparable<StringAndInt> {

    private String tag;
    private int count;

    public StringAndInt(String tag, int count) {
        this.tag = tag;
        this.count = count;
    }

    public String getTag() {
        return tag;
    }

    public int getCount() {
        return count;
    }

    @Override
    public String toString() {
        return tag + " : " + count;
    }

    @Override
    public int compareTo(StringAndInt o) {
        return o.count - count;
    }
}
