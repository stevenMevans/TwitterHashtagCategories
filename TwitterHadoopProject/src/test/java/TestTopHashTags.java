import org.apache.hadoop.io.IntWritable;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class TestTopHashTags
{
    private final int ARRAY_SIZE = 15;

    private String[] hashtagKeys = {"#love", "#instagood", "#tbt", "#me", "#cute", "#summer", "#wcw", "#nofilter", "#art",
                                   "#follow", "#happy", "#tagforlikes", "#photooftheday", "#instadaily", "#hair"};
    public int[] hashtagValues = {1, 1, 1, 2, 2, 3, 3, 4, 4, 5, 6, 7, 7, 9, 10};

    public HashMap<String, IntWritable> testMap = new HashMap<>(10);

    @Before
    public void initialize()
    {
        testMap.put(hashtagKeys[5], new IntWritable(hashtagValues[5]));
        testMap.put(hashtagKeys[6], new IntWritable(hashtagValues[6]));
        testMap.put(hashtagKeys[7], new IntWritable(hashtagValues[7]));
        testMap.put(hashtagKeys[8], new IntWritable(hashtagValues[8]));
        testMap.put(hashtagKeys[9], new IntWritable(hashtagValues[9]));
        testMap.put(hashtagKeys[10], new IntWritable(hashtagValues[10]));
        testMap.put(hashtagKeys[11], new IntWritable(hashtagValues[11]));
        testMap.put(hashtagKeys[12], new IntWritable(hashtagValues[12]));
        testMap.put(hashtagKeys[13], new IntWritable(hashtagValues[13]));
        testMap.put(hashtagKeys[14], new IntWritable(hashtagValues[14]));
    }

    @Test
    public void testGetSmallestEntry()
    {
        TopHashTags.mostUsed.putAll(testMap);
        java.util.Map.Entry smallestEntry = TopHashTags.getSmallestEntry();
        IntWritable smallestValue = (IntWritable) smallestEntry.getValue();
        assertEquals(smallestValue.get(), 3);
        System.out.println(smallestEntry.getKey());
    }

    @Test
    public void testAddToTopTen()
    {
        for (int i = 0; i < ARRAY_SIZE; i++)
        {
            TopHashTags.addToTopTen(hashtagKeys[i], hashtagValues[i]);
        }
        System.out.println("----- EXPECTED SET ----");
        for (String key : testMap.keySet())
        {
            System.out.println(key);
        }
        System.out.println();
        System.out.println("----- ACTUAL SET ----");
        for (String key : TopHashTags.mostUsed.keySet())
        {
            System.out.println(key);
        }
        assertEquals(testMap.keySet(), TopHashTags.mostUsed.keySet());
    }
}
