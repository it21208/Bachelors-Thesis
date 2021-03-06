for (f = 0; f < MAX_FEATURES; f++) {
// create a vector of MAX_CUSTOMERS with INIT_BRDCST value
matrixRowList.add(new MatrixRow(Collections.nCopies(MAX_CUSTOMERS, INIT)));
}
JavaRDD<MatrixRow> rdd1 = sc.parallelize(matrixRowList);
this.m_aCustFeatures_PairRDD = rdd1.zipWithIndex().
mapToPair((Tuple2<MatrixRow, Long> tuple) -> new Tuple2<Integer, MatrixRow>(Integer.valueOf(tuple.
_2.intValue()), tuple._1));
// Create RDDs for Movie Feature matrix, using autogenerated indices
matrixRowList = new ArrayList<>();
for (f = 0; f < MAX_FEATURES; f++) {
  // create a vector of MAX_MOVIES with INIT_BRDCST value
matrixRowList.add(new MatrixRow(Collections.nCopies(MAX_MOVIES, INIT)));
}
this.m_aMovieFeatures_PairRDD = sc.parallelize(matrixRowList).zipWithIndex().
mapToPair((Tuple2<MatrixRow, Long> tuple) -> new Tuple2<Integer, MatrixRow>(Integer.
valueOf(tuple._2.intValue()), tuple._1));
