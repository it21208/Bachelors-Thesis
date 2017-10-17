// create RDD for Customer statistics
JavaRDD<String[]> columnsTrainingFile = trainingFile.map(line -> line.split("\t"));
JavaPairRDD<Integer, Integer> columnsCustomersA_
= columnsTrainingFile.mapToPair((String[] row) -> new Tuple2(Integer.
parseInt(row0]), Integer.parseInt(row[2])));
JavaPairRDD<Integer, Integer> columnsCustomersA = columnsCustomersA_.reduceByKey((a, b) -> a + b);
JavaPairRDD<Integer,Integer> columnsCustomersB_ = columnsTrainingFile.
mapToPair((String[] row) -> new Tuple2(Integer.parseInt(row[0]), 1));
JavaPairRDD<Integer,Integer> columnsCustomersB = columnsCustomersB_.
reduceByKey((a, b) -> a + b);
JavaPairRDD<Integer, Tuple2<Integer, Integer>> columnsCustomers = 0columnsCustomersB.
join(columnsCustomersA);
this.m_aCustomers_PairRDD = columnsCustomers.mapToPair((Tuple2<Integer, 
Tuple2<Integer, Integer>> tuple) -> new Tuple2<Integer, Customer>(tuple.
_1, new Customer(tuple._2._1, tuple._2._2)));
