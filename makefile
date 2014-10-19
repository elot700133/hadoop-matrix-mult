HADOOP_CLASSPATH=$(hadoop classpath)

all:
	mkdir -p class-folder
	javac -cp `hadoop classpath` -d class-folder/ MatrixMult.java
	jar -cvf ./matrixmult.jar -C class-folder/ .
clean:
	rm -rf class-folder/*
	rm matrixmult.jar
run:
	hadoop jar /home/to778287/hadoop-matrix-mult/matrixmult.jar org.myorg.MatrixMult /usr/to778287/matrix-mult/input /usr/to778287/matrix-mult/output
