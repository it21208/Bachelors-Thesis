    // Used by RDD transformation
double PredictRating(int custId, int movieId) {
double sum = 1;
int f = 0;

for (f = 0; f < this.MAX_FEATURES_BRDCST.getValue(); f++) {
 sum += this.m_aMovieFeatures_BRDCST.getValue()[f][movieId] * this.
 m_aCustFeatures_BRDCST.getValue()[f][custId];
  if (sum > 5) {
      sum = 5;
  }
  if (sum < 1) {
      sum = 1;
  }
}
  return sum;
}
}
