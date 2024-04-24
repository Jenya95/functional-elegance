# Functional Elegance: Making Spark Applications Cleaner with the Cats Library
This repository contains the source code of the blog post.

## Requirements
* Scala 2.13
* Java 8 or 11 (for spark, maybe newer also work)
* sbt in terminal

## How to run
You can import the project into your favorite IDE as sbt project.

Also, you can try to run all 3 approaches from the post as follows:

```shell
sbt "runMain com.sanevich.example.NaiveApproach"
```
```shell
sbt "runMain com.sanevich.example.TupledApproach"
```
```shell
sbt "runMain com.sanevich.example.WriterApproach"
```