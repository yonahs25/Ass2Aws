package mle;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class trigramComparator  extends WritableComparator{

    public trigramComparator() {
        super(trigramComparable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        trigramComparable t1 = (trigramComparable) a;
        trigramComparable t2 = (trigramComparable) b;
        return t1.compareTo(t2);
    }
}
