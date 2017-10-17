JavaRDD<String> trainingFile = this.sc.textFile(filename.value());
        // calculate the number of ratings
        this.m_nRatingCount_BRDCST = this.sc.broadcast(trainingFile.count());
        // create RDD for Customer statistics
        JavaRDD<String[]> columnsTrainingFile = trainingFile.map(line -> line.split("\t"));
