package mapreduce.hi.api.interval;

import java.io.IOException;

import mapreduce.hi.HIKey;
import mapreduce.hi.HITuple;
import mapreduce.hi.api.ChainConfigurator;
import mapreduce.hi.api.Configurator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class ValueConfigurator implements Configurator{

	@Override
	public Job getJob(Configuration conf) throws IOException {
		Job job = new Job(conf, "INTERVAL_LEVEL");
		job.setJarByClass(ChainConfigurator.class);
		job.setMapperClass(ValueMapper.class);
		job.setCombinerClass(ValueReducer.class);
		job.setReducerClass(ValueReducer.class);
		job.setOutputKeyClass(HIKey.class);
		job.setOutputValueClass(HITuple.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		return job;
	}

}
