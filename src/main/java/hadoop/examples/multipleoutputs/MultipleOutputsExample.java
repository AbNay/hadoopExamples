package hadoop.examples.multipleoutputs;
//http://www.lichun.cc/blog/2013/11/how-to-use-hadoop-multipleoutputs/
//http://www.myhadoopexamples.com/2015/07/10/mapreduce-multiple-outputs-use-case/
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MultipleOutputsExample extends Configured implements Tool {
	public static String VEG = "veg";
	public static String FRUIT = "fruit";

	public int run(String[] args) throws Exception {
		// Configuration config = new Configuration();
		// String[] otherArgs = new GenericOptionsParser(config,
		// args).getRemainingArgs();
		// if (otherArgs.length != 2) {
		// System.err.print("Usage: wordcount <in> <out>");
		// System.exit(2);
		// }

		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					MultipleOutputsExample.class.getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		// Create configuration
        Configuration conf = new Configuration();
 
		// create Job
		Job job = Job.getInstance(conf);
		job.setJobName("MultipleOutputsExample");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setJarByClass(MultipleOutputsExample.class);

		job.setMapperClass(MOMapper.class);

		job.setReducerClass(MOReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		MultipleOutputs.addNamedOutput(job, VEG, TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, FRUIT, TextOutputFormat.class, Text.class, Text.class);

		int returnValue = job.waitForCompletion(true) ? 0 : 1;
		System.out.println("job.isSuccessful " + job.isSuccessful());
		return returnValue;		
	}

	public static void main(String args[]) throws Exception {
		int exitCode = ToolRunner.run(new MultipleOutputsExample(), args);
		System.exit(exitCode);
	}

	public static class MOMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			System.out.println("Mapper started");
			StringTokenizer st = new StringTokenizer(value.toString());
			if (st.hasMoreTokens()) {
				context.write(new Text(st.nextToken()), value);
			}
		}
	}

	public static class MOReducer extends Reducer<Text, Text, Text, Writable> {
		MultipleOutputs<Text, Text> moWriters;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// super.setup(context);
			System.out.println("Reducer setup");
			moWriters = new MultipleOutputs(context);
		}

		@Override
		protected void reduce(Text key, Iterable<Text> listValues, Context context)
				throws IOException, InterruptedException {
			System.out.println("Reducer stared. Key - " +  key.toString());
			for (Text t : listValues) {
				if (key.toString().toLowerCase().equals("fruits"))
					moWriters.write(FRUIT, key, t);
				else
					moWriters.write(VEG, key, t);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// super.cleanup(context);
			System.out.println("Reducer cleanup");
			//moWriters = null;
			moWriters.close();
		}
	}

}
