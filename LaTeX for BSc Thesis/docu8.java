 // create maps from the Customer and Movie Feature RDDs
        mapCustomerFeatures = new HashMap<>(m_aCustFeatures_PairRDD.collectAsMap());
        mapMovieFeatures = new HashMap<>(m_aMovieFeatures_PairRDD.collectAsMap());
