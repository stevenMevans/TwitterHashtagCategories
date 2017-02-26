import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import twitter4j.*;

import java.io.IOException;
import java.lang.reflect.Array;
import java.net.URI;

public class TweetCategorizer
{
    private static final String JOB_NAME = "categorizetweets";
    private static final URI DEFAULT_INPUT = URI.create("src/data/tweets.json");
    private static final URI OUTPUT_PREFIX = URI.create("hdfs://localhost:8020/");
    private static final URI OUTPUT_SUFFIX = URI.create("/tweets");
    private static final URI DEFAULT_OUTPUT = URI.create(OUTPUT_PREFIX + "Failed" + OUTPUT_SUFFIX);

    public static void categorize(java.util.Map<String, IntWritable> topHashtags) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, JOB_NAME);
        job.setJarByClass(TweetCategorizer.class);
        job.setJarByClass(TweetCategorizer.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(TweetCategorizer.Map.class);
        job.setReducerClass(TweetCategorizer.Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(DEFAULT_INPUT));
        FileOutputFormat.setOutputPath(job, new Path(DEFAULT_OUTPUT));
        MultipleOutputs.addNamedOutput(job, "tweets", TextOutputFormat.class, Text.class, NullWritable.class);

        job.waitForCompletion(true);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text>
    {
        private Text word = new Text();
        private Text content = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            word.clear();
            String line = value.toString().trim();

            try
            {
                Status status = TwitterObjectFactory.createStatus(line);
                content.set(line);
                HashtagEntity[] hashtags = status.getHashtagEntities();

                if (hashtags.length == 0)
                    word.set("None");
                else
                {
                    for (HashtagEntity hashtag : hashtags)
                    {
                        if (TopHashTags.isTopHashTag(hashtag.getText()))
                            word.set(hashtag.getText());
                        else
                            word.set("Others");
                    }
                }

                context.write(word, content);
            }
            catch (TwitterException e)
            {
                e.printStackTrace();
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, NullWritable, Text>
    {
        MultipleOutputs<Text, NullWritable> mout;

        @Override
        public void setup(Context context)
        {
            mout = new MultipleOutputs(context);
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            String outputPath = OUTPUT_PREFIX + key.toString() + OUTPUT_SUFFIX;
            for (Text value : values)
                mout.write("tweets", null, value, outputPath);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException
        {
            mout.close();
        }
    }
}
