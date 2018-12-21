import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Map;

public class JavaALSExample {

    /** Rating 类
     * userId:用户ID
     * movieId：电影ID
     * rating：用户对电影的评分
     * timestamp：评分的时间戳
     */
    public  static String dataDir = "data/mllib/als/";
    public static class Rating implements Serializable {
        private int userId;
        private int movieId;
        private float rating;
        private long timestamp;

        public Rating() {}

        public Rating(int userId, int movieId, float rating, long timestamp) {
            this.userId = userId;
            this.movieId = movieId;
            this.rating = rating;
            this.timestamp = timestamp;
        }

        public int getUserId() {
            return userId;
        }

        public int getMovieId() {
            return movieId;
        }

        public float getRating() {
            return rating;
        }

        public long getTimestamp() {
            return timestamp;
        }

        /** 将一条评分记录解析得到userId、movieId、rating、timestamp
         * @param str 一条评分的数据
         * @return  Rating 对象
         */
        public static Rating parseRating(String str) {
            String[] fields = str.split("::");
            if (fields.length != 4) {
                throw new IllegalArgumentException("Each line must contain 4 fields");
            }
            int userId = Integer.parseInt(fields[0]);
            int movieId = Integer.parseInt(fields[1]);
            float rating = Float.parseFloat(fields[2]);
            long timestamp = Long.parseLong(fields[3]);
            return new Rating(userId, movieId, rating, timestamp);
        }
    }


    public  static  class  User implements Serializable{
        private  int userId;
        private int userAge;
        private char userSex;
        private int userOccupation;
        private String zipcode;
        public User() {}
        public User(int userId, int userAge, char userSex, int userOccupation,String zipcode) {
            this.userId = userId;
            this.userAge = userAge;
            this.userSex = userSex;
            this.userOccupation = userOccupation;
            this.zipcode = zipcode;
        }

        public int getUserId() {
            return userId;
        }

        public int getUserAge() {
            return userAge;
        }

        public float getUserSex() {
            return userSex;
        }
        public int getOccupation() {
            return userOccupation;
        }
        public String getZipcode() {
            return zipcode;
        }

        /** 从字符串中解析每条用户数据，转化为 User 对象
         * @param str 用户数据
         * @return User 对象
         */
        public static User parseUser(String str) {
            String[] fields = str.split("::");
            if (fields.length != 5) {
                throw new IllegalArgumentException("Each line must contain 5 fields");
            }
            int userId = Integer.parseInt(fields[0]);
            char userSex = fields[1].charAt(0);
            int userAge = Integer.parseInt(fields[2]);
            int userOccupation = Integer.parseInt(fields[3]);
            String zipcode = fields[4];
            return new User(userId, userAge, userSex, userOccupation,zipcode);
        }
    }

    /** Movie 类
     * movieID 电影ID
     * movieTitle 电影名称
     * moiveGenres 电影类型
     */
    public static class Movie implements Serializable {
        private int movieId;
        private String movieTitle;
        private String[] moiveGenres;

        public Movie() {}

        public Movie(int movieId, String movieTitle, String[] moiveGenres) {
            this.movieId = movieId;
            this.movieTitle = movieTitle;
            this.moiveGenres = moiveGenres;
        }

        public int getmovieId() {
            return movieId;
        }

        public String getMovieTitle() {
            return movieTitle;
        }

        public String[]  getMoiveGenres() {
            return moiveGenres;
        }
        /** 将一条评分记录解析得到userId、movieId、rating、timestamp
         * @param str 一条评分的数据
         * @return  Rating 对象
         */
        public static Movie parseMovie(String str) {
            String[] fields = str.split("::");
            if (fields.length != 3) {
                throw new IllegalArgumentException("Each line must contain 3 fields");
            }
            int movieId = Integer.parseInt(fields[0]);
            String movieTitle = fields[1];
            String[] moiveGenres = fields[2].split("\\|");
            return new Movie(movieId, movieTitle,moiveGenres);
        }
    }


    // $example off$
//统计分析数据
    private static void analyzeData(SparkSession spark){
        //分析用户数据
        JavaRDD<User> user_data = spark.read().textFile(dataDir+"users.dat").javaRDD().map(User::parseUser);
        Dataset<Row> users = spark.createDataFrame(user_data, User.class);
        //统计年龄分布
        JavaRDD<Integer> ageRDD = user_data.map(User::getUserAge);
        Map<Integer,Long> ageMap =  ageRDD.countByValue();
        System.out.println("每个元素出现的次数:" + ageRDD.countByValue());
        //统计职业分布
        JavaRDD<Integer> occupationRDD = user_data.map(User::getOccupation);
        //将每个职业转为key-value的RDD，并给每个职业计数为1
        JavaPairRDD<Integer,Integer>  occupationPairRDD = occupationRDD.mapToPair(s -> new Tuple2<>(s, 1));
        //计算每个职业出现的个数
        JavaPairRDD<Integer,Integer>  occupationCountRDD = occupationPairRDD.reduceByKey((a,b) -> a+b);
        Dataset<Row> occupationDF = spark.createDataset(occupationCountRDD.collect(),Encoders.tuple(Encoders.INT(),Encoders.INT())).toDF("key","value");

        //分析评分数据 获得评分的最大值 最小值 和 平均值
        JavaRDD<String> ratings_data = spark.read().textFile(dataDir+"ratings.dat").javaRDD();
        JavaRDD<Rating> ratingJavaRDD =  ratings_data.map(Rating::parseRating);
        JavaRDD<Float> rating = ratingJavaRDD.map(Rating::getRating);
        Float maxRating  = rating.reduce((Float x,Float y)-> x>y?x:y);
        Float meanRating = rating.reduce((Float x,Float y)-> x+y)/ratings_data.count();
    }


    public static void main(String[] args) {
        //设置 log 等级，不输出INFO
        Logger.getLogger("org").setLevel(Level.ERROR);
//       System.setProperty("hadoop.home.dir", "F:/Program Files (x86)");
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaALSExample")
                .getOrCreate();

//        analyzeData(spark);
        JavaRDD<Rating> ratingsRDD = spark
                .read().textFile(dataDir+"ratings.dat").javaRDD()
                .map(Rating::parseRating);
        Dataset<Row> ratings = spark.createDataFrame(ratingsRDD, Rating.class);

        // 将数据分为训练集和测试集两部分
        Dataset<Row>[] splits = ratings.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> training = splits[0];
        Dataset<Row> test = splits[1];
        //加载用户数据
        JavaRDD<User> user_data = spark.read().textFile(dataDir+"users.dat").javaRDD().map(User::parseUser);

        //加载电影元数据
        JavaRDD<Movie> movieRDD = spark
                .read().textFile(dataDir+"movies.dat").javaRDD()
                .map(Movie::parseMovie);

        // 建立模型
        ALS als = new ALS()
                .setMaxIter(5)
                .setRegParam(0.01)
                .setUserCol("userId")
                .setItemCol("movieId")
                .setRatingCol("rating");
        ALSModel model = als.fit(training);

        //设置冷启动
        model.setColdStartStrategy("drop");
        Dataset<Row> predictions = model.transform(test);
        predictions.toJavaRDD().repartition(1).saveAsTextFile(dataDir+"predictions");

        //推荐模型效果评估
        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setMetricName("rmse")
                .setLabelCol("rating")
                .setPredictionCol("prediction");
        // 得到根方误差
        Double rmse = evaluator.evaluate(predictions);
        System.out.println("Root-mean-square error = " + rmse);

        // 为每名用户推荐10部电影
        Dataset<Row> userRecs = model.recommendForAllUsers(10);
        // 为每部电影推荐10名用户
        Dataset<Row> movieRecs = model.recommendForAllItems(10);

        // 显示推荐结果
        userRecs.show();
        movieRecs.show();

        //保存结果到文件中
        movieRecs.toJavaRDD().repartition(1).saveAsTextFile(dataDir+"movies");
        userRecs.toJavaRDD().repartition(1).saveAsTextFile(dataDir+"user");
        spark.stop();
    }
}

