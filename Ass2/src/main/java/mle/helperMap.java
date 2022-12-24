package mle;

import java.util.Collection;
import java.util.Set;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class helperMap extends MapWritable {
    @Override
    public String toString(){
        Set<Writable> keys = this.keySet();
        Collection<Writable> values = this.values();
        return ((LongWritable)keys.iterator().next()).get() + "\t" + ((LongWritable)values.iterator().next()).get();
    }
}
