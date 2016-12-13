import com.google.common.collect.MinMaxPriorityQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;

public class Question3_0 {

	final static public int K_DEFAULT = 5;

	// type clé input, type valeur input, type clé output, type valeur output
	public static class MyMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			double latitude = 0, longitude = 0;
			String uTags, mTags;
			String[] fields = value.toString().split("\\t");
			LinkedList<String> tags = new LinkedList<>();

			// Discard invalid inputs
			if (fields.length < 11) {
				return;
			}

			for (int i = 0; i < fields.length; i++) {
				String field = fields[i];

				switch (i) {

					// User tags
					case 8:
						uTags = java.net.URLDecoder.decode(field, "UTF-8");
						tags.addAll(Arrays.asList(uTags.toString().split(",")));
						break;

					// Machine tags
					case 9:
						mTags = java.net.URLDecoder.decode(field, "UTF-8");

						// Machine tags will not be considered here
						//tags.addAll(Arrays.asList(mTags.toString().split(",")));
						break;

					// Longitude
					case 10:
						longitude = Double.parseDouble(field);
						break;

					// Latitude
					case 11:
						latitude = Double.parseDouble(field);
						break;
				}
			}

			Country country = Country.getCountryAt(latitude, longitude);

			if (country != null) {
				for (String tag : tags) {
					if (tag.length() > 0) {
						context.write(
							new Text(country.toString() + tag.trim()),
							new IntWritable(1)
						);
					}
				}
			}
		}
	}

	// type clé input, type valeur input, type clé output, type valeur output
	public static class MyReducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int count = 0;

			for (IntWritable value : values) {
				count += value.get();
			}

			context.write(key, new IntWritable(count));
		}
	}


	// type clé input, type valeur input, type clé output, type valeur output
	public static class MyMapper2 extends Mapper<Text, IntWritable, Text, StringAndInt> {

		@Override
		protected void map(Text key, IntWritable value, Context context)
				throws IOException, InterruptedException {
			String sKey = key.toString();

			String country = sKey.substring(0, 2);
			String tag = sKey.substring(2);

			context.write(new Text(country), new StringAndInt(tag, value.get()));
		}
	}

	// type clé input, type valeur input, type clé output, type valeur output
	public static class MyReducer2 extends Reducer<Text, StringAndInt, Text, MinMaxPriorityQueue> {

		@Override
		protected void reduce(Text key, Iterable<StringAndInt> values, Context context)
				throws IOException, InterruptedException {

			Configuration config = context.getConfiguration();
			final int K = config.getInt("K", K_DEFAULT);

			MinMaxPriorityQueue<StringAndInt> pqueue = MinMaxPriorityQueue.maximumSize(K).create();

			for (StringAndInt si : values) {
				pqueue.add(new StringAndInt(si.getTag(), si.getCount()));
			}

			context.write(key, pqueue);
		}
	}

	// type clé input, type valeur input, type clé output, type valeur output
	public static class MyCombiner2 extends Reducer<Text, StringAndInt, Text, StringAndInt> {

		@Override
		protected void reduce(Text key, Iterable<StringAndInt> values, Context context)
				throws IOException, InterruptedException {

			Configuration config = context.getConfiguration();
			final int K = config.getInt("K", K_DEFAULT);

			MinMaxPriorityQueue<StringAndInt> pqueue = MinMaxPriorityQueue.maximumSize(K).create();

			for (StringAndInt si : values) {
				pqueue.add(new StringAndInt(si.getTag(), si.getCount()));
			}

			for (StringAndInt si : pqueue) {
				context.write(key, si);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		String input;
		String output;

		if (otherArgs.length < 2) {
			output = "output";

			if (otherArgs.length < 1) {
				input = "input";
			}
			else {
				input = otherArgs[0];
			}
		}
		else {
			output = otherArgs[1];
			input = otherArgs[0];

			if (otherArgs.length >= 3) {
				conf.set("K", otherArgs[2]);
			}
		}

		Job job = Job.getInstance(conf, "Question3_0");
		job.setJarByClass(Question3_0.class);

		job.setMapperClass(MyMapper1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(MyReducer1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setCombinerClass(MyReducer1.class);
		job.setNumReduceTasks(3);

		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);

		Path binPath = new Path(output + ".bin");

		FileOutputFormat.setOutputPath(job, binPath);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		if ( !job.waitForCompletion(true) ) {
			System.exit(1);
		}

		Job job2 = Job.getInstance(conf, "Question3_0");
		job2.setJarByClass(Question3_0.class);

		job2.setMapperClass(MyMapper2.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(StringAndInt.class);

		job2.setReducerClass(MyReducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(MinMaxPriorityQueue.class);

		job2.setCombinerClass(MyCombiner2.class);
		job2.setNumReduceTasks(3);

		FileInputFormat.addInputPath(job2, binPath);
		job2.setInputFormatClass(SequenceFileInputFormat.class);

		FileOutputFormat.setOutputPath(job2, new Path(output));
		job2.setOutputFormatClass(TextOutputFormat.class);


		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
