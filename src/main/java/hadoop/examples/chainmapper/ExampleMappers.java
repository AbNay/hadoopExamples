package hadoop.examples.chainmapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ExampleMappers{
	public static class SplitMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	    @Override
	    protected void map(LongWritable key, Text value, Context context)
	    		throws IOException, InterruptedException {
	    	StringTokenizer st = new StringTokenizer(value.toString());
	    	IntWritable one = new IntWritable(1);
	    	while (st.hasMoreTokens()){
	    		context.write(new Text((String) st.nextElement()), one);
	    	}
	    }
	}
	
	public static class LowerCaseMapper extends Mapper<Text, IntWritable, Text, IntWritable>{
	    @Override
	    protected void map(Text key, IntWritable value, Context context)
	    		throws IOException, InterruptedException {
	    	context.write(new Text(key.toString().toLowerCase()), value);
	    }
	}
}
