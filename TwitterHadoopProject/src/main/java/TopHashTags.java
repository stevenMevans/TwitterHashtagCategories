import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.jetbrains.annotations.Nullable;
import twitter4j.*;

public class TopHashTags
{
    private static final int MAX_SIZE = 10;
    private static final String JOB_NAME = "tophashtags";
    private static final String INPUT_FILE_NAME = "src/data/tweets.json";
    private static final String OUTPUT_FILE_NAME = "src/data/output.txt";

    public static HashMap<String, IntWritable> mostUsed = new HashMap<>(MAX_SIZE);

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, JOB_NAME);
        job.setJarByClass(TopHashTags.class);
        job.setJarByClass(TopHashTags.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_FILE_NAME));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_FILE_NAME));

        job.waitForCompletion(true);

        System.out.println(mostUsed.toString());

        DirectoryCreator.create(mostUsed);
        TweetCategorizer.categorize(mostUsed);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            word.clear();
            String line = value.toString();
            line.trim();

            try
            {
                Status status = TwitterObjectFactory.createStatus(line);
                HashtagEntity[] hashtags = status.getHashtagEntities();
                for (HashtagEntity hashtag : hashtags)
                {
                    word.set(hashtag.getText());
                    context.write(word, one);
                }
            } catch (TwitterException tE)
            {
                tE.printStackTrace();
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable value : values)
                sum += value.get();

            addToTopTen(key.toString(), sum);
            context.write(key, new IntWritable(sum));
        }
    }

    public static void addToTopTen(String key, int count)
    {
        IntWritable result = new IntWritable(count);

        if (mostUsed.size() < MAX_SIZE)
            mostUsed.put(key, result);
        else
        {
            java.util.Map.Entry smallestEntry = getSmallestEntry();
            IntWritable smallestValue = (IntWritable) smallestEntry.getValue();
            if (smallestValue.compareTo(result) < 0)
            {
                mostUsed.remove(smallestEntry.getKey());
                mostUsed.put(key, result);
            }
        }
    }

    @Nullable
    public static java.util.Map.Entry getSmallestEntry()
    {
        if (mostUsed.isEmpty())
            return null;

        Iterator iterator = mostUsed.entrySet().iterator();
        if (iterator == null)
            return null;

        java.util.Map.Entry smallestEntry = (java.util.Map.Entry) iterator.next();
        while (iterator.hasNext())
        {
            IntWritable smallestValue = (IntWritable) smallestEntry.getValue();
            java.util.Map.Entry entryToCheck = (java.util.Map.Entry) iterator.next();
            IntWritable valueToCheck = (IntWritable) entryToCheck.getValue();
            if (valueToCheck.compareTo(smallestValue) < 0)
                smallestEntry = entryToCheck;
        }

        return smallestEntry;
    }

    public static boolean isTopHashTag(String hashtag)
    {
        return mostUsed.containsKey(hashtag);
    }
}
