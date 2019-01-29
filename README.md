# Big Data Engineering test

The goal of this test is to impress LINKIT, asses yourself and open a path to a clear and concise technical interview.

Remember, you already went through our screening and first interview; this is the beginning and preparation for a mutual technical conversation.

## Hortonworks Data Platform Sandbox

Download the latest HDP sandbox and run locally following the instructions [here](https://hortonworks.com/tutorial/learning-the-ropes-of-the-hortonworks-sandbox/) to learn how to setup and everything.
Do the first tutorial [Learning the Ropes of the HDP Sandbox](https://hortonworks.com/tutorial/learning-the-ropes-of-the-hortonworks-sandbox)

> This was tested running on a macbook with 16GB of RAM. Check your memory usage or deploy in the cloud if you need more. I recommend allocating at least 8GB for this `vbox`.

Explain you steps and impression in `MyExperience.md`.

## Basic HDFS & Hive on Spark

Build a Scala application using Spark and execute against the Sandbox Hive & SPark to do the following:
- upload the `.csv` files on <a href="data-spark/">`data-spark`</a> to HDFS
- create tables on Hive for each `.csv` file
- output a dataframe on Spark that contains `DRIVERID, NAME, HOURS_LOGGED, MILES_LOGGED` so you can have aggregated information about the driver.

Besides the code on a repo, explain you steps and impression in <a href="`MyExperience.md">`MyExperience.md`</a>.

## HBase

Extend the Scala application above so that it can: 
- create a table `dangerous_driving` on HBase
- load <a href="data-hbase/dangerous-driver.csv">`dangerous-driver.csv`</a>
- add a 4th element to the table from `extra-driver.csv`
- Update `id = 4` to display `routeName` as `Los Angeles to Santa Clara` instead of `Santa Clara to San Diego`
- Outputs to console the Name of the driver, the type of event and the event Time if the origin or destination is `Los Angeles`.

Same thing here, besides the code on a repo, explain you steps and impression in <a href="`MyExperience.md">`MyExperience.md`</a>.

## Kafka Ingestion

Setup a single Kafka cluster on Docker and create a simple stream with any information you would like to.
<br> Ingest this `raw` stream into `HDFS` of the Sandbox. How? Choose your preferred tool - Kafka (Streaming or Connect), Spark (Regular or Streaming), Flink, Storm, Flume, NiFi... up to you. Choose if you want to do in batches or real-streaming.
<br> I expect you to have issues on the connectivity and make it work here, so do not worry... put your learnings and explain your steps in <a href="`MyExperience.md">`MyExperience.md`</a>.

## Extra Points
Ingest the Kafka stream as a real stream into HBase.

## Doubts &/Or Submission

Clone this repository to start working on your own prefered git tool. In the end, commit and push your solution and send us the link.
<br> Feel free to reach out to [Thiago de Faria](mailto:thiago.de.faria@linkit.nl).
