package mapreduce.hi.api;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import mapreduce.hi.api.interval.ValueConfigurator;
import mapreduce.hi.api.object.ObjectLevelConfigurator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;

public class ChainConfigurator {
	public static Configuration conf;

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		final String[] otherArgs = args;
		UserGroupInformation ugi = UserGroupInformation
				.createRemoteUser("root");

		try {

			ugi.doAs(new PrivilegedExceptionAction<Void>() {

								public Void run() throws Exception {
									
						conf = new Configuration();
						conf.set("mapred.job.tracker", "192.168.1.149:9001");
						conf.set("fs.default.name", "hdfs://192.168.1.149:9000");
				
						conf.set("hadoop.job.ugi", "root");
				
						if (otherArgs.length != 4) {
							System.err
									.println("Usage: Comment <in1 path> <temp file for merging in> <out1 path> <out2 path>");
							System.exit(2);
						}
						// delete temporary location if already exists
						delete(otherArgs[1], conf);
						// delete output if exists
						delete(otherArgs[2], conf);
						// delete output2 if exists
						delete(otherArgs[3], conf);
				
						copyMerge(otherArgs[0], otherArgs[1], conf);
				
						//firstJob(otherArgs);
						
						System.exit(firstJob(otherArgs)?0:1);
						//System.exit(secondJob(otherArgs)?0:1);
						return null;
					}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static boolean secondJob(String[] otherArgs)
			throws IOException, InterruptedException, ClassNotFoundException {
		ObjectLevelConfigurator objectLevelConfigurator = new ObjectLevelConfigurator();
		Job objectJob = objectLevelConfigurator.getJob(conf);

		// CombineInputFormat.addInputPath(job, new Path(otherArgs[0]));
		//FileInputFormat.setInputPathFilter(objectJob, filter)
		FileInputFormat.addInputPath(objectJob, new Path(otherArgs[2]));
		FileOutputFormat.setOutputPath(objectJob, new Path(otherArgs[3]));		
		return objectJob.waitForCompletion(true);
	}

	private static boolean firstJob(String[] otherArgs) throws IOException,
			InterruptedException, ClassNotFoundException {
		ValueConfigurator valueConfigurator = new ValueConfigurator();
		Job valueJob = valueConfigurator.getJob(conf);

		// CombineInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileInputFormat.addInputPath(valueJob, new Path(otherArgs[1]));
		LazyOutputFormat.setOutputFormatClass(valueJob, TextOutputFormat.class);
		TextOutputFormat.setOutputPath(valueJob, new Path(otherArgs[2]));
		//FileOutputFormat.setOutputPath(valueJob, new Path(otherArgs[3]));
		return valueJob.waitForCompletion(true);
	}

	private static void delete(String arg, Configuration configuration)
			throws IOException {
		final Path path = new Path(arg);
		FileSystem fs = FileSystem.get(configuration);
		if (fs.exists(path)) {

			fs.delete(path, true);
		}
		fs.close();
	}

	private static void copyMerge(String sourceDir, String destFile,
			Configuration conf) throws IOException {
		FileSystem fileSystem = FileSystem.get(conf);
		FileUtil.copyMerge(fileSystem, new Path(sourceDir), fileSystem,
				new Path(destFile), false, conf, null);

	}
}
