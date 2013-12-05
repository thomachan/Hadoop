package mapreduce.hi.api.input;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.google.common.base.Charsets;
import com.radiant.cisms.hdfs.seq.HInfoWritable;

public class CustomInputFormat extends FileInputFormat<LongWritable, HInfoWritable>{

	@Override
	public RecordReader<LongWritable, HInfoWritable> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
		
		 String delimiter = context.getConfiguration().get(
	        "inputformat.record.delimiter");
	    byte[] recordDelimiterBytes = null;
	    if (null != delimiter){
	    	recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
	    }else{
	    	recordDelimiterBytes = new byte[]{'\n'};
	    }
	    return new CustomReordReader(recordDelimiterBytes);
	}
}
