package mp2;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PopularityLeague extends Configured implements Tool {

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Integer[] numbers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[numbers.length];
            for (int i = 0; i < numbers.length; i++) {
                ints[i] = new IntWritable(numbers[i]);
            }
            set(ints);
        }
    }	
	
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO
    	
    	Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/mp2/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Link Count");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(LinkCountMap.class);
        jobA.setReducerClass(LinkCountReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(PopularityLeague.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Top Link");
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(IntWritable.class);

        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(IntArrayWritable.class);

        jobB.setMapperClass(TopLinksMap.class);
        jobB.setReducerClass(TopLinksReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(PopularityLeague.class);
        return jobB.waitForCompletion(true) ? 0 : 1;    	
    }
    
    public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }    

    public static class LinkCountMap extends Mapper<Object, Text, Text, IntWritable> {
        // TODO
    	
    	@Override
    	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //TODO
        	
        	String line = value.toString();
        	StringTokenizer tokenizer = new StringTokenizer(line);
        	while (tokenizer.hasMoreTokens()) {
        		String nextToken = tokenizer.nextToken();
        		context.write(new Text(nextToken), new IntWritable(1));
        	}
        	
        }
    	
    }

    public static class LinkCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    	@Override
    	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //TODO
        	
    		int sum = 0;
        	for (IntWritable val : values) {
        		sum += val.get();
        	}
        	context.write(key, new IntWritable(sum));
        }
    }

    public static class TopLinksMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
        Integer N;
        
        List<String> leagueList;
        
        public static final Log log = LogFactory.getLog(TopLinksMap.class);

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
            
            log.info("Inizio lettura configurazione");
            String league = conf.get("league");
            log.info(league);
            this.leagueList = Arrays.asList(readHDFSFile(league, conf).split("\n"));
            for (String entry : this.leagueList) {
				log.info(entry);
			}
        }
        
        private TreeSet<Pair<Integer, String>> countToWordMap = new TreeSet<Pair<Integer, String>>();
        
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            // TODO
        	Integer count = Integer.parseInt(value.toString());
        	if (this.leagueList.contains(key.toString().trim())) {
        		System.out.println("Trovato id");
        		countToWordMap.add(new Pair<Integer, String>(count, key.toString()));	
        	}
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // TODO
        	
        	for (Pair<Integer, String> item : countToWordMap) {
        		String[] strings = {item.second, item.first.toString()};
        		Integer[] ar = {Integer.parseInt(strings[0]), Integer.parseInt(strings[1])};  
        		IntArrayWritable val = new IntArrayWritable(ar);
        		context.write(NullWritable.get(), val);
        	}
        }        
        
    }

    public static class TopLinksReduce extends Reducer<NullWritable, IntArrayWritable, Text, IntWritable> {
        Integer N;

        List<String> leagueList;
        
        public static final Log log = LogFactory.getLog(TopLinksReduce.class);
        
        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
            
            log.info("Inizio lettura configurazione");
            String league = conf.get("league");
            log.info(league);
            this.leagueList = Arrays.asList(readHDFSFile(league, conf).split("\n"));
            for (String entry : this.leagueList) {
				log.info(entry);
			}
        }
        // TODO
        
        private TreeSet<Pair<Integer, Integer>> countToWordMap = new TreeSet<Pair<Integer, Integer>>();
        
        @Override
        public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
            // TODO
        	
        	for (IntArrayWritable val: values) {
        		IntWritable[] pair= (IntWritable[]) (val.toArray());
        		Integer count = pair[0].get();
        		Integer word = pair[1].get();
        	System.out.println(count.toString() + this.leagueList.contains(count.toString().trim()));	
        	if (this.leagueList.contains(count.toString().trim())) {
        		System.out.println("Trovato id");
        		countToWordMap.add(new Pair<Integer, Integer>(word, count));	
        	}
        	}
        	
        	List<Pair<Integer, Integer>> listaResult = new ArrayList<Pair<Integer,Integer>>();
        	for (Pair<Integer, Integer> item: countToWordMap) {
        		listaResult.add(item);
        	}
        	
        	for (int i = 0;i < listaResult.size(); i++) {
        		int sum = 0;
        		for (int j = 0; j< listaResult.size(); j ++) {
        			if (i != j) {
        				System.out.println("sopra " + listaResult.get(i).first + " sotto " + listaResult.get(j).first);
        				if (listaResult.get(j).first < listaResult.get(i).first) sum++;
        			}	
        		}
        		context.write(new Text("" + listaResult.get(i).second), new IntWritable(sum));
        	}

        }
    }
}

// >>> Don't Change
class Pair<A extends Comparable<? super A>, B extends Comparable<? super B>>
		implements Comparable<Pair<A, B>> {

	public final A first;
	public final B second;

	public Pair(A first, B second) {
		this.first = first;
		this.second = second;
	}

	public static <A extends Comparable<? super A>, B extends Comparable<? super B>> Pair<A, B> of(
			A first, B second) {
		return new Pair<A, B>(first, second);
	}

	@Override
	public int compareTo(Pair<A, B> o) {
		int cmp = o == null ? 1 : (this.first).compareTo(o.first);
		return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
	}

	@Override
	public int hashCode() {
		return 31 * hashcode(first) + hashcode(second);
	}

	private static int hashcode(Object o) {
		return o == null ? 0 : o.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Pair))
			return false;
		if (this == obj)
			return true;
		return equal(first, ((Pair<?, ?>) obj).first)
				&& equal(second, ((Pair<?, ?>) obj).second);
	}

	private boolean equal(Object o1, Object o2) {
		return o1 == o2 || (o1 != null && o1.equals(o2));
	}

	@Override
	public String toString() {
		return "(" + first + ", " + second + ')';
	}
    
    // TODO
}