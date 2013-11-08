package mapreduce.hi;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;

public class HIReduce {

	public static class Map extends
		Mapper<LongWritable, Text, HIKey, HITuple> {
		private HIKey out = new HIKey();
		private HITuple hiTuple = new HITuple();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {// Parse the input string into a nice map
			String txt = value.toString();
			// .get will return null if the key is not there
			if (txt == null) {
				// skip this record
				return;
			}
			// Tokenize the string by splitting it up on whitespace into
			// something we can iterate over,
			// then send the tokens away
			StringTokenizer itr = new StringTokenizer(txt,"||");
			
			while (itr.hasMoreTokens()) {
				//read a single line stroed in hdfs
				hiTuple.setObjId(Long.valueOf(itr.nextToken()));
				hiTuple.setOid(new Text(itr.nextToken()));
				hiTuple.setValue(new Text(itr.nextToken()));
				hiTuple.setTime(Long.valueOf(itr.nextToken()));
				hiTuple.setInterval(new Text(Intervals.HOUR.toString()));
				hiTuple.setCount(1);
				
				// create key [ oid: time ]
				long time = truncate(hiTuple.getTime(),Intervals.HOUR);
				out.setOid(hiTuple.getOid());
				out.setTime(time);
				context.write(out, hiTuple);
			}
		}

		private long truncate(Long time, Intervals inr) {
			return inr.getTime(time);
		}
	}

	public static class Reduce extends
			Reducer<HIKey, HITuple, HIKey, HITuple> {
		    	  private HITuple result = null;
		    	  
		    	  public void reduce(HIKey key, Iterable<HITuple> values,  Context context) throws IOException, InterruptedException {
			    	  double sum = 0;
			    	  int count = 0;
			     for (HITuple val : values) {
			    	  if(result == null){
			    		  result= val;
			    	  }
			    	  sum += Double.valueOf(val.getValue().toString());
			    	  count += val.getCount();
		    	  }
			      result.setCount(count);	
		    	  result.setAvg(sum/count);
		    	  context.write(key, result);
		    	  }
		      }

	public static void main(String[] args) throws Exception {
		final String[] a = args;
		UserGroupInformation ugi = UserGroupInformation
				.createRemoteUser("root");

		try {

			ugi.doAs(new PrivilegedExceptionAction<Void>() {

				public Void run() throws Exception {

					Configuration conf = new Configuration();
					conf.set("mapred.job.tracker", "192.168.1.149:9001");
					conf.set("fs.default.name", "hdfs://192.168.1.149:9000");
					conf.set("hadoop.job.ugi", "root");

					String[] otherArgs = new GenericOptionsParser(conf, a)
							.getRemainingArgs();
					if (otherArgs.length != 2) {
						System.err
								.println("Usage: CommentWordCount <in> <out>");
						System.exit(2);
					}
					

					Job job = new Job(conf, "HI_ARCHIVE");
					job.setJarByClass(HIReduce.class);
					job.setMapperClass(Map.class);
					job.setCombinerClass(Reduce.class);
					job.setReducerClass(Reduce.class);
					job.setOutputKeyClass(HIKey.class);
					job.setOutputValueClass(HITuple.class);
					
					FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
					FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
					System.exit(job.waitForCompletion(true) ? 0 : 1);

					return null;
				}
			});

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
