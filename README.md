# Lab 2 Report

## Usage

The application is executed via `Planet.jar` , a fat JAR file packaged by the [sbt-assembly](https://github.com/sbt/sbt-assembly) plugin. After downloading the `Planet.jar` file, log in to your AWS account. 

Then you will come to the AWS management console, from which you can get access to a variety of services that AWS provides. The one we use to run our Spark application is Elastic MapReduce(EMR). Type `EMR` in the search bar and click the link to the first result. Here, we can run our application in Spark cluster mode. 

<p align="center">
<img width="700" src="lab2_images/EMRclusters.png" alt="EMRclusters" title="EMRclusters" >
</p> 
<h6 align="center">EMR clusters interface</h6>


To create a cluster, the hardware configuration we suggest using to run our application on the Planet data set is `master-node-type` for the master node, `core-node-type` for core nodes. 

<p align="center">
<img width="700" src="lab2_images/hardware_conf.png" alt="Hardware configuration" title="Hardware configuration" >
</p> 
<h6 align="center">Hardware configuration </h6>


The final step is to add a step to the cluster. Choose Spark application as "Step type". Copy and paste the following configures into the "Spark-submit options" part. Select "Application location" from the S3 bucket and add an integer in "Arguments" to represent the rising sea level. Click "Add" and it is all set! 

```
--conf "spark.sql.autoBroadcastJoinThreshold=-1"  
--conf "spark.sql.broadcastTimeout=36000" 
--conf "spark.yarn.am.waitTime=36000"
--conf "spark.yarn.maxAppAttempts=1" 
```
<p align="center">
<img width="700" src="lab2_images/add_step.png" alt="Add a step to cluster" title="Add a step to cluster" >
</p> 
<h6 align="center"> Add a step to cluster </h6>




## Approach

> Describe significant iterations (see Rubric) of your implementation.
>
> Note down relevant metrics such as: which dataset did you run on what system,
> how much memory did this use, what was the run-time, cost, throughput, etc
> or whatever is applicable in your story towards your final implementation.
>
> You can show both application-level improvements and cluster-level improvements here.

### Iteration 0: Baseline
The metrics of Lab 1 are listed as follows:
|Metrics| Value |
|---|---|
| System                  | Laptop  |
| Workers                 | 1 | 
| Dataset                 | Netherlands | 
| Run time  | 20-22 minutes | 
| (more relevant metrics) | (more relevant values) |
  
### Iteration 1: Improving performance on reading data
By inspecting the spark history server, we found that the performance of our application is not optimal due to several reasons:
1. The process of reading data takes a very long period of time
2. Duplicated process of reading data
3. Lack of proper configuration setting
<p align="center">
<img width="700" src="lab2_images/spark_stages.png" alt="Stages in Spark history server" title="Stages in Spark history server" >
</p> 
<h6 align="center"> Stage 1, 2 , 5 took up 99% of time consumption </h6>



Accordingly, we implemented the following optimization:
1. Filter out the unnecessary data immediately after reading the data. 
    <p align="center">
    <img width="500" src="lab2_images/wayrelation.png" alt="extra data" title="extra data" >
    </p> 
    <h6 align="center">   The type "relation" and "way" are useless! </h6>
2. Cache intermediate data into memory
3. Add some configures through a trial-and-error process and others' experience. For example, set the partition number to 5 instead of the default 200.
 ```
val spark = SparkSession
      .builder()
      .appName("Lab 2")
      .config("spark.master", "local[*]")
      .config("spark.sql.broadcastTimeout", "600") // avoid time-out error
      .config("spark.io.compression.lz4.blockSize", "512kb")
      .config("spark.shuffle.unsafe.file.output.buffer", "1mb")
      .config("spark.shuffle.file.buffer", "1mb")
      .config("spark.executor.memory", "2g")
      .config("spark.sql.shuffle.partitions", 5)
 ```
 <h6 align="center">  Configures </h6>

 After implementing this alternative, we re-ran the experiment, resulting in:
|Metrics| Value |
|---|---|
| System                  | Laptop  |
| Workers                 | 1 | 
| Dataset                 | Netherlands | 
| Run time  | 7-8 minutes | 
| (more relevant metrics) | (more relevant values) |

### Iteration 2: Improving Y
Since the previous iteration didn't bring us to the target run-time of ...

## Summary of application-level improvements

> A point-wise summary of all application-level improvements (very short, about
> one or two 80 character lines per improvement, just to provide an overview).
 
## Cluster-level improvements

> A point-wise summary of all cluster-level improvements (very short, about
> one or two 80 character lines per improvement, just to provide an overview).

## Conclusion

> Briefly conclude what you have learned.
<p align="center">
<img width="700" src="lab2_images/cluster_overview.png" alt="Spark cluster mode" title="Spark cluster mode" >
</p>

Amazon EMR uses YARN as cluster manager to schedule the jobs for processing data and managing cluster resources. The program we developed with the `main` function is the driver program, it  



 We came up with three alternatives:
> * Alternative A: to replace the step with (explanation of some approach)
> * Alternative B: to replace the step with (explanation of another approach)

> 
