 // initialize broadcast matrices to make them available in executors
        m_aCustFeatures_BRDCST = sc.broadcast(m_aCustFeatures);
        m_aMovieFeatures_BRDCST = sc.broadcast(m_aMovieFeatures);
