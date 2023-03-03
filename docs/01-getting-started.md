Getting started
===============

The easiest way to deploy an instance of Sleeper and interact with it is to use the "system test" functionality. This
deploys a Sleeper instance with a simple schema, and writes some random data into a table in the instance. You can then
use the status scripts to see how much data is in the system, run some example queries, and view logs to help understand
what the system is doing. It is best to do this from an EC2 instance as a significant amount of code needs to be
uploaded to AWS.

### Dependencies

Before running this demo functionality, you will need the following installed:

* [Bash](https://www.gnu.org/software/bash/): Tested with v3.2. Use `bash --version`.
* [Docker](https://docs.docker.com/get-docker/): Tested with v20.10.17
* Sleeper CLI

#### Sleeper CLI installation

The Sleeper CLI contains Docker images with the necessary dependencies and scripts to work with Sleeper. Run the
following commands to install the CLI. The version can be `main` or a release in the format `v0.14.0`.

```bash
curl "https://raw.githubusercontent.com/gchq/sleeper/[version]/scripts/cli/install.sh" -o ./sleeper-install.sh
chmod +x ./sleeper-install.sh
./sleeper-install.sh [version]
```

This installs a `sleeper` command to run other commands inside a Docker container. You can use `sleeper aws` or
`sleeper cdk` to run `aws` or `cdk` commands without needing to install the AWS or CDK CLI on your machine. If you set
AWS environment variables or configuration on the host machine, that will be propagated to the Docker container when
you use `sleeper`.

You can also upgrade the CLI to a different version with `sleeper cli upgrade`.

### Deployment environment

You can use the AWS CDK to create an EC2 instance in a VPC that is suitable for deploying Sleeper. Run these commands to
do this with the Sleeper CLI (note that cdk bootstrap only needs to be done once in a given AWS account):

```bash
sleeper aws configure
sleeper cdk bootstrap
sleeper environment deploy TestEnvironment
```

The `sleeper environment deploy` command will create an SSH key locally, and wait for the EC2 instance to be deployed.
You can then SSH to it with this command:

```bash
sleeper environment connect
```

Immediately after it's deployed, commands will run on this instance to install the Sleeper CLI. Once you're connected,
you can check the progress of those commands like this:

```bash
cloud-init status
```

You can check the output like this (add `-f` if you'd like to follow the progress):

```bash
tail /var/log/cloud-init-output.log
```

Once it has finished the instance might restart.

You can access a built copy of the Sleeper scripts by running `sleeper deployment` in the EC2. That will get you a shell
inside a Docker container inside the EC2. You can run all the deployment scripts there as explained below. If you run it
outside of the EC2, you'll get the same thing but in your local Docker host. Use the one in the EC2 to avoid the
deployment being slow uploading jars and Docker images.

The Sleeper Git repository will also be cloned, and you can access it by running `sleeper builder` in the EC2.
That will get you a shell inside a Docker container similar to the `sleeper deployment` one, but with the dependencies
for building Sleeper. The whole working directory will be persisted between executions of `sleeper builder`.

To deploy Sleeper or run the system tests from this instance, you'll need to add your own credentials for the AWS CLI.
See
the [AWS IAM guide for CLI access](https://docs.aws.amazon.com/singlesignon/latest/userguide/howtogetcredentials.html).

### System test

To run the system test, set the environment variable `ID` to be a globally unique string. This is the instance id. It
will be used as part of the name of various AWS resources, such as an S3 bucket, lambdas, etc., and therefore should
conform to the naming requirements of those services. In general stick to lowercase letters, numbers, and hyphens. We
use the instance id as part of the name of all the resources that are deployed. This makes it easy to find the resources
that Sleeper has deployed within each service (go to the service in the AWS console and type the instance id into the
search box).

Create an environment variable called `VPC` which is the id of the VPC you want to deploy Sleeper to, and create an
environment variable called `SUBNET` with the id of the subnet you wish to deploy Sleeper to (note that this is only
relevant to the ephemeral parts of Sleeper - all of the main components use services which naturally span availability
zones).

The VPC _must_ have an S3 Gateway endpoint associated with it otherwise the `cdk deploy` step will fail.

While connected to your EC2 instance run:

```bash
sleeper deployment test/deployAll/buildDeployTest.sh ${ID} ${VPC} ${SUBNET}
```

This will use Maven to build Sleeper (this will take around 3 minutes, and the script will be silent during this time).
After that, an S3 bucket will be created for the jars, and ECR repos will be created and Docker images pushed to them.
Note that this script currently needs to be run from an x86 machine as we do not yet have cross-architecture Docker
builds. Then CDK will be used to deploy a Sleeper instance. This will take around 10 minutes. Once that is complete,
some code is run to start some tasks running on an ECS cluster. These tasks generate some random data and write it to
Sleeper. 11 ECS tasks will be created. Each of these will write 40 million records. As all writes to Sleeper are
asynchronous it will take a while before the data appears (around 8 minutes).

You can watch what the ECS tasks that are writing data are doing by going to the ECS cluster named
sleeper-${ID}-system-test-cluster, finding a task and viewing the logs.

Run the following command to see how many records are currently in the system:

```bash
sleeper deployment utility/filesStatusReport.sh ${ID} system-test
```

The randomly generated data in the table conforms to the schema given in the file `scripts/templates/schema.template`.
This has a key field called `key` which is of type string. The code that randomly generates the data will generate keys
which are random strings of length 10. To run a query, use:

```bash
sleeper deployment utility/query.sh ${ID}
```

As the data that went into the table is randomly generated, you will need to query for a range of keys, rather than a
specific key. The above script can be used to run a range query (i.e. a query for all records where the key is in a
certain range) - press 'r' and then enter a minimum and a maximum value for the query. Don't choose too large a range or
you'll end up with a very large amount of data sent to the console (e.g a min of 'aaaaaaaaaa' and a max of
'aaaaazzzzz'). Note that the first query is slower than the others due to the overhead of initialising some libraries.
Also note that this query is executed directly from a Java class. Data is read directly from S3 to wherever the script
is run. It is also possible to execute queries using lambda and have the results written to either S3 or to SQS. The
lambda-based approach allows for a much greater degree of parallelism in the queries. Use `lambdaQuery.sh` instead of
`query.sh` to experiment with this.

Be careful that if you specify SQS as the output, and query for a range containing a large number of records, then a
large number of results could be posted to SQS, and this could result in significant charges.

Over time you will see the number of active files (as reported by the `filesStatusReport.sh` script) decrease. This is
due to compaction tasks merging files together. These are executed in ECS clusters (named
`sleeper-${ID}-merge-compaction-cluster` and `sleeper-${ID}-splitting-merge-compaction-cluster`).

You will also see the number of leaf partitions increase. This functionality is performed using lambdas called
`sleeper-${ID}-find-partitions-to-split` and `sleeper-${ID}-split-partition`.

To ingest more random data, run:

```bash
sleeper deployment java -cp jars/system-test-*-utility.jar sleeper.systemtest.ingest.RunWriteRandomDataTaskOnECS ${ID} system-test
```

To tear all the infrastructure down, run

```bash
sleeper deployment test/tearDown.sh
```

Note that this will sometimes fail if there are ECS tasks running. Ensure that there are no compaction tasks running
before doing this.

It is possible to run variations on this system-test by editing the system test properties, like this:

```bash
sleeper deployment
cd test/deployAll
editor system-test-instance.properties
./buildDeployTest.sh  ${ID} ${VPC} ${SUBNET}
```

To deploy your own instance of Sleeper with a particular schema, go to the [deployment guide](02-deployment-guide.md).
