 // create RDD for Movie statistics
JavaPairRDD<Integer, Integer> columnsMoviesA_= columnsTrainingFile.
mapToPair((String[] row) -> new Tuple2(Integer.parseInt(row[1]), Integer
.parseInt(row[2])));
JavaPairRDD<Integer, Integer> columnsMoviesA = columnsMoviesA_.
reduceByKey((a, b) -> a + b);
JavaPairRDD<Integer, Integer> columnsMoviesB_ = columnsTrainingFile.
mapToPair((String[] row) -> new Tuple2(Integer.parseInt(row[1]), 1));
JavaPairRDD<Integer, Integer> columnsMoviesB = columnsMoviesB_.reduceByKey((a, b) -> a + b);
JavaPairRDD<Integer, Tuple2<Integer, Integer>> columnsMovies = columnsMoviesB.
join(columnsMoviesA);
this.m_aMovies_PairRDD = columnsMovies.mapToPair((Tuple2<Integer, Tuple2<Integer, Integer>> tuple) -> 
new Tuple2<Integer, Movie>(tuple._1, new Movie(tuple._2._1, tuple._2._2)));
// create Ratings array with Data objects
JavaRDD<TrainingData> rddData = columnsTrainingFile.map((String[] row)
 -> new TrainingData(Integer.parseInt(row[0]), Integer.parseInt(row[1]), Integer.parseInt(row[2])));
JavaPairRDD<TrainingData, Long> rddIndexedData = rddData.zipWithIndex();
this.m_aTrainingRatings_PairRDD = rddIndexedData.
mapToPair((Tuple2<TrainingData, Long> tuple) -> new Tuple2(tuple._2.intValue(), tuple._1));
