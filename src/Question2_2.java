import com.google.common.collect.MinMaxPriorityQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class Question2_2 {

	final static public int K_DEFAULT = 5;

	// type clé input, type valeur input, type clé output, type valeur output
	public static class MyMapper extends Mapper<LongWritable, Text, Text, StringAndInt> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

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
						//if (uTags.length()>1)
						//    System.out.println("user tags : "+uTags);
						break;

					// Machine tags
					case 9:
						mTags = java.net.URLDecoder.decode(field, "UTF-8");

						// Machine tags will not be considered here
						//tags.addAll(Arrays.asList(mTags.toString().split(",")));
						//if (mTags.length()>1)
						//    System.out.println("machine tags : "+mTags);
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
								new Text(country.toString()),
								new StringAndInt(tag, 1)
						);
					}
				}
			}
		}
	}

	// type clé input, type valeur input, type clé output, type valeur output
	public static class MyReducer extends Reducer<Text, StringAndInt, Text, MinMaxPriorityQueue> {

		@Override
		protected void reduce(Text key, Iterable<StringAndInt> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> tagsMap = new HashMap<>();

			for (StringAndInt value : values) {
				Integer oCount = tagsMap.get(value.getTag());
				int count = value.getCount();

				if (oCount != null) {
					count += oCount.intValue();
				}

				tagsMap.put(value.getTag(), count);
			}

			Configuration config = context.getConfiguration();
			final int K = config.getInt("K", K_DEFAULT);

			MinMaxPriorityQueue<StringAndInt> pqueue = MinMaxPriorityQueue.maximumSize(K).create();

			for (Map.Entry<String, Integer> entry : tagsMap.entrySet()) {
				String tag = entry.getKey();
				int count = entry.getValue();

				pqueue.add(new StringAndInt(tag, count));
			}

			context.write(key, pqueue);
		}
	}

	// type clé input, type valeur input, type clé output, type valeur output
	public static class MyCombiner extends Reducer<Text, StringAndInt, Text, StringAndInt> {

		@Override
		protected void reduce(Text key, Iterable<StringAndInt> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> tagsMap = new HashMap<>();

			for (StringAndInt value : values) {
				Integer oCount = tagsMap.get(value.getTag());
				int count = value.getCount();

				if (oCount != null) {
					count += oCount.intValue();
				}

				tagsMap.put(value.getTag(), count);
			}

			Configuration config = context.getConfiguration();
			final int K = config.getInt("K", K_DEFAULT);

			MinMaxPriorityQueue<StringAndInt> pqueue = MinMaxPriorityQueue.maximumSize(K).create();

			for (Map.Entry<String, Integer> entry : tagsMap.entrySet()) {
				String tag = entry.getKey();
				int count = entry.getValue();

				pqueue.add(new StringAndInt(tag, count));
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

		Job job = Job.getInstance(conf, "Question2_1");
		job.setJarByClass(Question2_2.class);
		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringAndInt.class);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MinMaxPriorityQueue.class);

		job.setCombinerClass(MyCombiner.class);
		job.setNumReduceTasks(3);

		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
