package com.viettel.http.period;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.commons.math3.stat.descriptive.moment.Kurtosis;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.Skewness;
import org.apache.commons.math3.stat.descriptive.moment.Variance;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import com.viettel.hostfilter.FilterHost;

import scala.Tuple2;
import scala.Tuple7;
/**
 * The {@link PeriodicallyRequestDetection} detect request periodically. 
 * @author THANHNT78
 * 
 * */
public class PeriodicallyRequestDetection implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	//final static Logger logger = Logger.getLogger(PeriodicallyRequestDetection.class);
	
	private String number_cluster = null;
	
	private String white_list_path =null;
	private String white_list_filename= null;
	private String data_path = null;
	private String data_filename = null; 
	private String output_path= null;
	private String output_filename = null;
	
	private String sql_column = null;
	private String sql_value = null;
	
	public static void main(String[] args) throws IOException{
		PeriodicallyRequestDetection starter =null;

			//starter = new PeriodicallyRequestDetection(args[0]);
		starter = new PeriodicallyRequestDetection("config.txt");
			starter.detect();

	}
	
	/**
	 * Create a PeriodicallyRequestDetection by load configure file.
	 * @param  config_path the system-dependent file name.
	 * @throws IOException if the file does not exist, is a directory rather than a regular file, or for some other reason cannot be opened for reading.
	 * 
	 * */
	public PeriodicallyRequestDetection(String config_path) throws IOException{
		
		Properties config = new Properties();
		config.load(new FileInputStream(config_path));
		// Thiet lap 
		loadConfig(config);
	}
	/**
	 * Create a {@link PeriodicallyRequestDetection} by Properties config
	 * @param config properties of {@link PeriodicallyRequestDetection}
	 * 
	 * */
	public PeriodicallyRequestDetection(Properties config){
		loadConfig(config);
	}
	/**
	 * Load config to avirable
	 * @param config properties of {@link PeriodicallyRequestDetection}
	 * @return void 
	 * */
	private void loadConfig(Properties config){
		number_cluster = config.getProperty("system.numbercluster");
		white_list_path = config.getProperty("whitelist.path");
		white_list_filename = config.getProperty("whitelist.filename");
		data_path = config.getProperty("data.path");
		data_filename = config.getProperty("data.filename");
		output_path = config.getProperty("output.path");
		output_filename = config.getProperty("output.filename");
		sql_column = config.getProperty("sql.column");
		sql_value = config.getProperty("sql.value");
		
		//logger.setLevel(Level.INFO);
	}
	/**
	 * Evaluate mean, skewness, variance, kurtosis of each traffic
	 * @throws IOException if file input, whitelist do not exist
	 * @return void 
	 * */
	public void detect() throws IOException {
		System.out.println("======================================================================");
		System.out.println(String.format("Number of cluster = %s", number_cluster));
		System.out.println();
		System.out.println(String.format("White List: %s",white_list_path+white_list_filename));
		System.out.println();
		System.out.println(String.format("Data: %s",data_path+data_filename));
		System.out.println();
		System.out.println(String.format("Output: %s",output_path+output_filename));
		System.out.println();
		System.out.println("======================================================================");
		final FilterHost filter = new FilterHost(white_list_path + white_list_filename);
		SparkConf conf = new SparkConf().setAppName("Periodically Request Detection").setMaster("local["+number_cluster+"]");
		SparkContext sc = new SparkContext(conf);
		JavaSparkContext jsc = new JavaSparkContext(sc);
		SQLContext sqlContext = new SQLContext(sc);

		DataFrame df = sqlContext.read().json(data_path + data_filename).cache(); 
		df.printSchema();
		df.show(30);

		DataFrame analysis = df.filter(/*"category='http'"*/sql_column+"='"+sql_value+"'").select("src", "hostname", "url", "timestamp")
				.orderBy("timestamp").cache();
		analysis.show(30);

		final long first_timestamp = analysis.select("timestamp").take(1)[0].getLong(0);
		System.out.println(first_timestamp);

		analysis.registerTempTable("dataset");
		sqlContext.udf().register("maptime", new UDF1<Long, Double>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Double call(Long t1) {
				if (t1 <= 0)
					System.out.println("NaN roi :(");
				return ((double) t1 - first_timestamp) / 1000;
			}
		}, DataTypes.DoubleType);

		String KEY_MAP_TIME = "maptime";
		String KEY_ONEHOST_TIME = "onehottime";
		DataFrame analysis_maptime = sqlContext
				.sql("SELECT src, hostname, maptime(timestamp) as maptime, url from dataset ").cache();
		analysis_maptime.printSchema();
		analysis_maptime.show(30);

		OneHotEncoder encoder = new OneHotEncoder();
		encoder.setInputCol(KEY_MAP_TIME); // timestamp
		encoder.setOutputCol(KEY_ONEHOST_TIME); // output column

		DataFrame a_sparse = encoder.transform(analysis_maptime); // dataframe
																	// sau khi
																	// danh
																	// index
		a_sparse.show(20);

		// SparseVector tmp = (SparseVector) a_sparse.take(10)[8].get(3);

		a_sparse.registerTempTable("spareset");
		JavaRDD<Tuple7<String, HashMap<Integer, Integer>, int[], Double, Double, Double, Double>> rdd = a_sparse
				.javaRDD().filter(new Function<Row, Boolean>() {
					private static final long serialVersionUID = 1L;

					/**
					 * loc bo whitelist
					 */
					public Boolean call(Row arg0) throws Exception {
						if (filter.check(arg0.getString(1)))
							return false;
						return true;
					}
				}).map(new Function<Row, Row>() {
					private static final long serialVersionUID = 1L;
					/**
					 * giam tu 3 col ->2 col
					 */
					public Row call(Row v1) throws Exception {
						// TODO Auto-generated method stub
						String result = v1.getString(3).replaceAll("=([^&]+)&", "=&");
						result = result.substring(0, result.lastIndexOf('=') + 1);
						return RowFactory.create(v1.getString(0) + " " + v1.getString(1) + " " + result, v1.get(4));
						// return null;
					}
				}).mapToPair(new PairFunction<Row, String, SparseVector>() {
					private static final long serialVersionUID = 1L;
					/**
					 * 2 col - > key,value
					 */
					public Tuple2<String, SparseVector> call(Row t) throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<String, SparseVector>(t.getString(0), (SparseVector) t.get(1));
					}

				}).reduceByKey(new Function2<SparseVector, SparseVector, SparseVector>() {
					private static final long serialVersionUID = 1L;
					/**
					 * n key,value -> key,(value+.... +value)
					 */
					public SparseVector call(SparseVector v1, SparseVector v2) throws Exception {
						// TODO Auto-generated method stub
						return add(v1, v2);
					}
				}).filter(new Function<Tuple2<String, SparseVector>, Boolean>() {
					private static final long serialVersionUID = 1L;
					/**
					 * loc bo cac truy van chi xuat hien 1 lan
					 */
					public Boolean call(Tuple2<String, SparseVector> arg0) throws Exception {
						if (arg0._2().indices().length > 1)
							return true;
						else
							return false;
					}
				})
				.map(new Function<Tuple2<String, SparseVector>, Tuple7<String, HashMap<Integer, Integer>, int[], Double, Double, Double, Double>>() {
					private static final long serialVersionUID = 1L;
					/**
					 * key, value -> key, histogram, mean, skewness, varian,
					 * kurtosis
					 */
					public Tuple7<String, HashMap<Integer, Integer>, int[], Double, Double, Double, Double> call(
							Tuple2<String, SparseVector> v1) throws Exception {
						// TODO Auto-generated method stub
						int[] index = v1._2().indices();
						int[] times = new int[index.length - 1];
						for (int i = 0; i < index.length - 1; i++) {
							times[i] = index[i + 1] - index[i];
						}

						Arrays.sort(times);
						HashMap<Integer, Integer> histogram = new HashMap<Integer, Integer>();
						for (int i : times) {
							if (histogram.containsKey(i))
								histogram.put(i, histogram.get(i) + 1);
							else
								histogram.put(i, 1);
						}

						double[] dtimes = new double[times.length];
						for (int i = 0; i < times.length; i++) {
							dtimes[i] = (double) times[i];
						}

						Double mean = new Mean().evaluate(dtimes);
						Double skew = new Skewness().evaluate(dtimes);
						Double vari = new Variance().evaluate(dtimes);
						Double kurtosis = new Kurtosis().evaluate(dtimes);
						return new Tuple7<String, HashMap<Integer, Integer>, int[], Double, Double, Double, Double>(
								v1._1(), histogram, times, mean, skew, vari, kurtosis);
					}
				})
				.sortBy(new Function<Tuple7<String, HashMap<Integer, Integer>, int[], Double, Double, Double, Double>, Double>() {
					private static final long serialVersionUID = 1L;
					/**
					 * sort by frequent
					 */
					public Double call(
							Tuple7<String, HashMap<Integer, Integer>, int[], Double, Double, Double, Double> v1)
									throws Exception {
						// TODO Auto-generated method stub
						Collection<Integer> x = v1._2().values();
						if (!x.isEmpty())
							return (double) Collections.max(x);
						else
							return 0.0;
					}

				}, true, 4);
		BufferedWriter write = new BufferedWriter(new FileWriter(output_path + output_filename));
		for (Tuple7<String, HashMap<Integer, Integer>, int[], Double, Double, Double, Double> row : rdd.collect()) {
			write.write(row._1() + "\t\t\t");
			HashMap<Integer, Integer> histo = row._2();
			TreeMap<Integer, Integer> sorted = SortByValue(histo);
			for (Map.Entry<Integer, Integer> run : sorted.entrySet()) {
				write.write(run.getKey() + ":" + run.getValue() + " ");
			}
			write.write("\t" + row._4().toString());
			write.write("\t" + row._5().toString());
			write.write("\t" + row._6().toString());
			write.write("\t" + row._7().toString());

			write.write("\r\n");
		}
		write.close();

		/*
		 * List<Tuple2<String, HashMap<Integer, Integer>>> tmp2 = rdd.take(200);
		 * for (Tuple2<String, HashMap<Integer, Integer>> row : tmp2) {
		 * System.out.print(row._1()+"\t\t\t"); HashMap<Integer, Integer> histo
		 * = row._2(); for (Map.Entry<Integer, Integer> run : histo.entrySet() )
		 * { System.out.print(run.getKey()+":"+run.getValue()+" "); }
		 * System.out.println(); }
		 */
		jsc.close();
		sc.stop();
	}
	/**
	 * Add 2 SparseVector
	 * @param v1 vector1
	 * @param v2 vector2
	 * @return vector1 +vector2  
	 * */
	public static SparseVector add(SparseVector v1, SparseVector v2) {
		HashMap<Integer, Double> sum = new HashMap<Integer, Double>();
		int[] index = v1.indices();
		double[] value = v1.values();
		for (int i = 0; i < value.length; i++) {
			sum.put(index[i], value[i]);
		}

		index = v2.indices();
		value = v2.values();
		for (int i = 0; i < value.length; i++) {
			sum.put(index[i], value[i]);
		}
		int[] out_i = new int[sum.size()];
		double[] out_v = new double[sum.size()];
		int i = 0;
		for (Map.Entry<Integer, Double> a : sum.entrySet()) {
			out_i[i] = a.getKey();
			out_v[i] = a.getValue();
			i++;
		}
		Arrays.sort(out_i);
		return new SparseVector(v1.size(), out_i, out_v);
	}
	/**
	 * Sort by value
	 * @param map contain Map<key, value>
	 * @return a treemap was sorted.
	 * */
	public static TreeMap<Integer, Integer> SortByValue(HashMap<Integer, Integer> map) {
		ValueComparator vc = new ValueComparator(map);
		TreeMap<Integer, Integer> sorted = new TreeMap<Integer, Integer>(vc);
		sorted.putAll(map);
		return sorted;
	}
}

class ValueComparator implements Comparator<Integer> {
	Map<Integer, Integer> map;

	public ValueComparator(Map<Integer, Integer> base) {
		this.map = base;
	}

	public int compare(Integer a, Integer b) {
		if (map.get(a) >= map.get(b))
			return -1;
		else
			return 1;
	}
}
