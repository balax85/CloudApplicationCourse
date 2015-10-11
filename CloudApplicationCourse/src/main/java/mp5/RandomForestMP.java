package mp5;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.mllib.tree.RandomForest;

import scala.Tuple2;

import java.util.HashMap;
import java.util.regex.Pattern;

public final class RandomForestMP {
	
	private static class ParsePoint implements Function<String, Vector> {
		
		private static final Pattern SPACE = Pattern.compile(",");
		
		public Vector call(String line) {
			String[] tok = SPACE.split(line);
			double[] point = new double[tok.length-1];
			for (int i = 1; i < tok.length; ++i) {
				point[i-1] = Double.parseDouble(tok[i]);
			}
			return Vectors.dense(point);
		}
	}
	
	private static class ParseTitle implements Function<String, String> {
	
		private static final Pattern SPACE = Pattern.compile(",");
		
		public String call(String line) {
			String[] tok = SPACE.split(line);
			return tok[0];
		}
	}
	
	private static class PrintCluster implements VoidFunction<Tuple2<Integer, Iterable<String>>> {
		
		private KMeansModel model;
		
		public PrintCluster(KMeansModel model) { 
			this.model = model;
		}
		
		public void call(Tuple2<Integer, Iterable<String>> Cars) throws Exception {
			String ret = "[";
			for(String car: Cars._2()){
				ret += car + ", ";
			}
			System.out.println(ret + "]");
		}
	}
	
	private static class ClusterCars implements PairFunction<Tuple2<String, Vector>, Integer, String> {
	
		private KMeansModel model;
		
		public ClusterCars(KMeansModel model) {
			this.model = model;
		}
		
		public Tuple2<Integer, String> call(Tuple2<String, Vector> args) {
			String title = args._1();
			Vector point = args._2();
			int cluster = model.predict(point);
			return new Tuple2<Integer, String>(cluster, title);
		}
	}

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println(
                    "Usage: RandomForestMP <training_data> <test_data> <results>");
            System.exit(1);
        }
        String training_data_path = args[0];
        String test_data_path = args[1];
        String results_path = args[2];

        SparkConf sparkConf = new SparkConf().setAppName("RandomForestMP");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        final RandomForestModel model;

        Integer numClasses = 2;
        HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
        Integer numTrees = 3;
        String featureSubsetStrategy = "auto";
        String impurity = "gini";
        Integer maxDepth = 5;
        Integer maxBins = 32;
        Integer seed = 12345;

		// TODO
        
        JavaRDD<LabeledPoint> train = sc.textFile(training_data_path).map(new DataToPoint());
        JavaRDD<LabeledPoint> test = sc.textFile(training_data_path).map(new DataToPoint()); 
        
        
        
        JavaRDD<LabeledPoint> results = test.map(new Function<Vector, LabeledPoint>() {
            public LabeledPoint call(Vector points) {
                return new LabeledPoint(model.predict(points), points);
            }
        });

        results.saveAsTextFile(results_path);

        sc.stop();
    }

}