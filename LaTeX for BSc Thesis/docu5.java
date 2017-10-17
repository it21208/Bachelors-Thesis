    // Reads in parallel the test file and produces the predictions file in parallel
    void ProcessTest() throws IOException {
        long cnt; // number of test ratings
        double sum; // sum of Abs difference between Rating and PredictionRating
        // read test file and create RDD of TrainingData items
        JavaRDD<TrainingData> c_testingData_RDD
                = this.sc.textFile(this.TESTING_FILE_BRDCST.getValue()).
        map(line -> line.split("\t")).
        map((String[] row) -> new TrainingData(Integer.
        parseInt(row[0]), Integer.parseInt(row[1]), Integer.parseInt(row[2])));
        // transform RDD of TrainingData to RDD of TestingData items with our predictions
        JavaRDD<TestingData> testingDataRDD
                = c_testingData_RDD.
                map((TrainingData t) -> new TestingData(t, PredictRating(t.CustId, t.MovieId)));
        // count test ratings
        cnt = testingDataRDD.count();
        // sum the abs differences between ratings and predictionratings
        sum = testingDataRDD.map(t -> t.diff()).reduce((a, b) -> a + b);
        // save RDD of TestingData to prediction file
        testingDataRDD.saveAsTextFile(this.PREDICTIONS_FILE_BRDCST.getValue());
        System.out.printf("\n--\nNumber of predictions: %d\nAvg Abs(diff): %f\n", cnt,sum/cnt);
    }
