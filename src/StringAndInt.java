import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by leo on 10/12/16.
 */
public class StringAndInt implements Comparable<StringAndInt>, Writable {

    private String tag;
    private int count;

    public StringAndInt() {
    }

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

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        Text text = new Text(tag);

        text.write(dataOutput);
        dataOutput.writeInt(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        Text text = new Text();

        text.readFields(dataInput);
        tag = text.toString();
        count = dataInput.readInt();
    }
}
