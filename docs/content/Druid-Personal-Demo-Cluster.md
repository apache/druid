---
layout: doc_page
---
# Druid Personal Demo Cluster (DPDC)

Note, there are currently some issues with the CloudFormation.  We are working through them and will update the documentation here when things work properly.  In the meantime, the simplest way to get your feet wet with a cluster setup is to run through the instructions at [housejester/druid-test-harness](https://github.com/housejester/druid-test-harness), though it is based on an older version.  If you just want to get a feel for the types of data and queries that you can issue, check out [Realtime Examples](Realtime-Examples.html)

## Introduction
To make it easy for you to get started with Druid, we created an AWS (Amazon Web Services) [CloudFormation](http://aws.amazon.com/cloudformation/) Template that allows you to create a small pre-configured Druid cluster using your own AWS account. The cluster contains a pre-loaded sample workload, the Wikipedia edit stream, and a basic query interface that gets you familiar with Druid capabilities like drill-downs and filters. 


This guide walks you through the steps to create the cluster and then how to create basic queries. (The cluster setup should take you about 15-20 minutes depending on AWS response times).


## What’s in this Druid Demo Cluster?

1. A single "Coordinator" node.  This node co-locates the [Coordinator](Coordinator.html) process, the [Broker](Broker.html) process, Zookeeper, and the MySQL instance. You can read more about Druid architecture [Design](Design.html).

1. Three historical nodes; these historical nodes, have been pre-configured to work with the Coordinator node and should automatically load up the Wikipedia edit stream data (no specific setup is required).

## Setup Instructions
1. Log in to your AWS account: Start by logging into the [Console page](https://console.aws.amazon.com) of your AWS account; if you don’t have one, follow this link to sign up for one [http://aws.amazon.com/](http://aws.amazon.com/).

![AWS Console Page](images/demo/setup-01-console.png)

1. If you have a [Key Pair](http://docs.aws.amazon.com/gettingstarted/latest/wah/getting-started-create-key-pair.html) already created you may skip this step. Note: this is required to create the demo cluster and is generally not used unless instances need to be accessed directly (e.g. via SSH). 

    1. Click **EC2** to go to the EC2 Dashboard. From there, click **Key Pairs** under Network & Security. 
    ![EC2 Dashboard](images/demo/setup-02a-keypair.png)

    1. Click on the button **Create Key Pair**. A dialog box will appear prompting you to enter a Key Pair name (as long as you remember it, the name is arbitrary, for this example we entered `Druid`). Click **Create**. You will be prompted to download a .pam; store this file in a safe place.
    ![Create Key Pair](images/demo/setup-02b-keypair.png)

1. Unless you’re there already, go back to the Console page, or follow this link: https://console.aws.amazon.com. Click **CloudFormation** under Deployment & Management.
![CloudFormation](images/demo/setup-03-ec2.png)

1. Click **Create New Stack**, which will bring up the **Create Stack** dialog.
![Create New Stack](images/demo/setup-04-newstack.png)

1. Enter a **Stack Name** (it’s arbitrary, we chose, `DruidStack`). Click **Provide a Template URL** type in the following template URL: _**https://s3.amazonaws.com/cf-templates-jm2ikmzj3y6x-us-east-1/2013081cA9-Druid04012013.template**_. Press **Continue**, this will take you to the Create Stack dialog.
![Stack Name & URL](images/demo/setup-05-createstack.png)

1. Enter `Druid` (or the Key Pair name you created in Step 2) in the **KeyPairName** field; click **Continue**. This should bring up another dialog prompting you to enter a **Key** and **Value**. 
![Stack Parameters](images/demo/setup-06-parameters.png)

1. While the inputs are arbitrary, it’s important to remember this information; we chose to enter `version` for **Key** and `1` for **Value**. Press **Continue** to bring up a confirmation dialog.
![Add Tags](images/demo/setup-07a-tags.png)

1. Click **Continue** to start creating your Druid Demo environment (this will bring up another dialog box indicating your environment is being created; click **Close** to take you to a more detailed view of the Stack creation process). Note: depending on AWS, this step could take over 15 minutes – initialization continues even after the instances are created. (So yes, now would be a good time to grab that cup of coffee). 
![Review](images/demo/setup-07b-review.png)
![Create Stack Complete](images/demo/setup-07c-complete.png)

1. Click and expand the **Events** tab in the CloudFormation Stacks window to get a more detailed view of the Druid Demo Cluster setup.      

![CloudFormations](images/demo/setup-09-events.png)

1. Get the IP address of your Druid Coordinator Node:
   1. Go to the following URL: [https://console.aws.amazon.com/ec2](https://console.aws.amazon.com/ec2)
   1. Click **Instances** in the left pane – you should see something similar to the following figure. 
   1. Select the **DruidCoordinator** instance
   1. Your IP address is right under the heading: **EC2 Instance: DruidCoordinator**. Select and copy that entire line, which ends with `amazonaws.com`.

![EC2 Instances](images/demo/setup-10-ip.png)

## Querying Data

1. Use the following URL to bring up the Druid Demo Cluster query interface (replace **IPAddressDruidCoordinator** with the actual druid coordinator IP Address):

**`http://IPAddressDruidCoordinator:8082/druid/v3/demoServlet`**

As you can see from the image below, there are default values in the Dimensions and Granularity fields. Clicking **Execute** will produce a basic query result. 
![Demo Query Interface](images/demo/query-1.png)

1. Note: when the Query is in running the **Execute** button will be disabled and read: **Fetching…**
![Demo Query](images/demo/query-2.png)

1. You can add multiple Aggregation values, adjust Granularity, and Dimensions; query results will appear at the bottom of the window. 


Enjoy! And for sure, please send along your comments and feedback or, aspirations on expanding and developing this demo. https://groups.google.com/d/forum/druid-development. Attention R users: we just open-sourced our R Druid connector: https://github.com/metamx/RDruid.
