package mapreduce.hi.api.interval.custom;

import java.io.IOException;

import mapreduce.hi.HIKey;
import mapreduce.hi.HITuple;
import mapreduce.hi.Intervals;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.radiant.cisms.hdfs.seq.HInfoWritable;

public class CustomMapper extends Mapper<LongWritable, HInfoWritable, HIKey, HITuple> {
	private HITuple hiTuple = new HITuple();
	private HIKey out = new HIKey();

	public void map(LongWritable key, HInfoWritable value, Context context)
			throws IOException, InterruptedException {String txt = value.toString();
			// .get will return null if the key is not there
			if (txt == null) {
				// skip this record
				return;
			}
			// Tokenize the string by splitting it up on whitespace into
			// something we can iterate over,
			// then send the tokens away
		
				hiTuple.setObjId(value.getObjId());
				hiTuple.setOid(new Text(value.getOid()));
				hiTuple.setValue(new Text(""+value.getValue()));
				hiTuple.setTime(value.getTime());
				hiTuple.setInterval(new Text(Intervals.HOUR.toString()));
				hiTuple.setCount(1);
				
				// create key [ oid: time ]
				long time = truncate(hiTuple.getTime(),Intervals.HOUR);
				out.setOid(hiTuple.getOid());
				out.setTime(time);
				context.write(out, hiTuple);
			}

	private long truncate(Long time, Intervals inr) {
		return inr.getTime(time);
	}
}