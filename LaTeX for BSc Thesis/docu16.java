this.m_nRatingCount = this.m_nRatingCount_BRDCST.getValue().intValue();
this.m_aCustomers_PairRDD.collectAsMap().forEach((k,v)->this.m_aCustomers[k]=v);
this.m_aMovies_PairRDD.collectAsMap().forEach((k, v) -> this.m_aMovies[k] = v);
this.m_aTrainingRatings_PairRDD.collectAsMap().forEach((k,v)->this.m_aRatings[k]=v);
