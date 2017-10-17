public static void main(String[] args) throws IOException {
LocalTime t1, t2, t3, t4, t5;

t1 = LocalTime.now();
SVDMovieLensSparkJava engine = new SVDMovieLensSparkJava();
t2 = LocalTime.now();
System.out.printf("engine construction duration equals %d s\n",Duration.between(t1,t2).getSeconds());
engine.LoadHistory();
t3 = LocalTime.now();
System.out.printf("load history duration equals %d s\n",Duration.between(t2,t3).getSeconds());
engine.CalcFeatures();
t4 = LocalTime.now();
System.out.printf("calculation feature duration equals %d s\n",Duration.between(t3,t4).getSeconds());
engine.ProcessTest();
t5 = LocalTime.now();
System.out.printf("processing test duration equals %d s\n",Duration.between(t4,t5).getSeconds());
System.out.println("\nDone\n");
}