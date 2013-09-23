---
layout: default
---
Greetings! This tutorial will help clarify some core Druid concepts. We will use a realtime dataset and issue some basic Druid queries. If you are ready to explore Druid, and learn a thing or two, read on!

About the data
--------------

The data source we’ll be working with is the Bit.ly USA Government website statistics stream. You can see the stream [here](http://developer.usa.gov/1usagov), and read about the stream [here](http://www.usa.gov/About/developer-resources/1usagov.shtml) . This is a feed of json data that gets updated whenever anyone clicks a bit.ly shortened USA.gov website. A typical event might look something like this:
\`\`\`json
{
 [user\_agent]() “Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)”,
 [country]() “US”,
 [known\_user]() 1,
 [timezone]() “America/New\_York”,
 [geo\_region]() “DC”,
 [global\_bitly\_hash]() “17ctAFs”,
 [encoding\_user\_bitly\_hash]() “17ctAFr”,
 [encoding\_user\_login]() “senrubiopress”,
 [aaccept\_language]() “en-US”,
 [short\_url\_cname]() “1.usa.gov”,
 [referring\_url]() “http://t.co/4Av4NUFAYq”,
 [long\_url]() “http://www.rubio.senate.gov/public/index.cfm/fighting-for-florida?ID=c8357d12-9da8-4e9d-b00d-7168e1bf3599”,
 [timestamp]() 1372190407,
 [timestamp of time hash was created]() 1372190097,
 [city]() “Washington”,
 [latitude\_longitude]() [
 38.893299,
 ~~77.014603
 ]
}
\`\`\`
The “known\_user” field is always 1 or 0. It is 1 if the user is known to the server, and 0 otherwise. We will use this field extensively in this demo.
h2. Setting Up
There are two ways to setup Druid: download a tarball, or ]. You only need to do one of these.
h3. Download a Tarball
We’ve built a tarball that contains everything you’ll need. You’ll find it [here](http://static.druid.io/artifacts/releases/druid-services-0.5.50-bin.tar.gz)
Download this file to a directory of your choosing.
You can extract the awesomeness within by issuing:
\<pre\>tar~~zxvf druid-services~~**~~bin.tar.gz\</pre\>
Not too lost so far right? That’s great! If you cd into the directory:
\<pre\>cd druid-services-0.5.50\</pre\>
You should see a bunch of files:
\* run\_example\_server.sh
\* run\_example\_client.sh
\* LICENSE, config, examples, lib directories

h2. Running Example Scripts
Let’s start doing stuff. You can start a Druid ] node by issuing:
\<pre\>./run\_example\_server.sh\</pre\>
Select “webstream”.
Once the node starts up you will see a bunch of logs about setting up properties and connecting to the data source. If everything was successful, you should see messages of the form shown below.
\<pre\><code>
2013-07-19 21:54:05,154 INFO com.metamx.druid.realtime.RealtimeNode~~ Starting Jetty
2013-07-19 21:54:05,154 INFO org.mortbay.log - jetty-6.1.x
2013-07-19 21:54:05,171 INFO com.metamx.druid.realtime.plumber.RealtimePlumberSchool - Expect to run at
2013-07-19 21:54:05,246 INFO org.mortbay.log - Started SelectChannelConnector@0.0.0.0:8083
</code>\</pre\>
The Druid real time-node ingests events in an in-memory buffer. Periodically, these events will be persisted to disk. If you are interested in the details of our real-time architecture and why we persist indexes to disk, I suggest you read our [White Paper](http://static.druid.io/docs/druid.pdf).
Okay, things are about to get real. To query the real-time node you’ve spun up, you can issue:
\<pre\>./run\_example\_client.sh\</pre\>
Select “webstream” once again. This script issues ]s to the data we’ve been ingesting. The query looks like this:
\`\`\`json
{
 [queryType]() “groupBy”,
 [dataSource]() “webstream”,
 [granularity]() “minute”,
 [dimensions]() [
 “timezone”
 ],
 [aggregations]() [
 {
 [type]() “count”,
 [name]() “rows”
 },
 {
 [type]() “doubleSum”,
 [fieldName]() “known\_users”,
 [name]() “known\_users”
 }
 ],
 [filter]() {
 [type]() “selector”,
 [dimension]() “country”,
 [value]() “US”
 },
 [intervals]() [
 “2013-06-01T00:00/2020-01-01T00”
 ]
}
\`\`\`
This is a****groupBy**\* query, which you may be familiar with from SQL. We are grouping, or aggregating, via the **dimensions** field: . We are **filtering** via the **“country”** dimension, to only look at website hits in the US. Our **aggregations** are what we are calculating: a row count, and the sum of the number of known users in our data.
The result looks something like this:
\`\`\`json
[
 {
 [version]() “v1”,
 [timestamp]() “2013-07-18T19:39:00.000Z”,
 [event]() {
 [timezone]() “America/Chicago”,
 [known\_users]() 10,
 [rows]() 15
 }
 },
 {
 [version]() “v1”,
 [timestamp]() “2013-07-18T19:39:00.000Z”,
 [event]() {
 [timezone]() “America/Los\_Angeles”,
 [known\_users]() 0,
 [rows]() 3
 }
 },
…
\`\`\`
This groupBy query is a bit complicated and we’ll return to it later. For the time being, just make sure you are getting some blocks of data back. If you are having problems, make sure you have [curl](http://curl.haxx.se/) installed. Control+C to break out of the client script.
h2. Querying Druid
In your favorite editor, create the file:
\<pre\>time\_boundary\_query.body\</pre\>
Druid queries are JSON blobs which are relatively painless to create programmatically, but an absolute pain to write by hand. So anyway, we are going to create a Druid query by hand. Add the following to the file you just created:
\<pre\><code>
{
 [queryType]() “timeBoundary”,
 [dataSource]() “webstream”
}
</code>\</pre\>
The ] is one of the simplest Druid queries. To run the query, you can issue:
\<pre\><code> curl~~X POST ‘http://localhost:8083/druid/v2/?pretty’ ~~H ‘content-type: application/json’~~d ```` time_boundary_query.body</code></pre>

We get something like this JSON back:

```json
[
    {
        "timestamp": "2013-07-18T19:39:00.000Z", 
        "result": {
            "minTime": "2013-07-18T19:39:00.000Z", 
            "maxTime": "2013-07-18T19:46:00.000Z"
        }
    }
]
```
As you can probably tell, the result is indicating the maximum and minimum timestamps we've seen thus far (summarized to a minutely granularity). Let's explore a bit further.

Return to your favorite editor and create the file:
<pre>timeseries_query.body</pre>

We are going to make a slightly more complicated query, the [TimeseriesQuery](TimeseriesQuery.html). Copy and paste the following into the file:
<pre><code>
{
    "queryType": "timeseries", 
    "dataSource": "webstream", 
    "intervals": [
        "2010-01-01/2020-01-01"
    ], 
    "granularity": "all", 
    "aggregations": [
        {
            "type": "count", 
            "name": "rows"
        }, 
        {
            "type": "doubleSum", 
            "fieldName": "known_users", 
            "name": "known_users"
        }
    ]
}
</code></pre>

You are probably wondering, what are these [Granularities](Granularities.html) and [Aggregations](Aggregations.html) things? What the query is doing is aggregating some metrics over some span of time. 
To issue the query and get some results, run the following in your command line:
<pre><code>curl -X POST 'http://localhost:8083/druid/v2/?pretty' -H 'content-type: application/json'  -d  ````timeseries\_query.body</code>

</pre>
Once again, you should get a JSON blob of text back with your results, that looks something like this:

\`\`\`json
[
{
 “timestamp” : “2013-07-18T19:39:00.000Z”,
 “result” : {
 “known\_users” : 787.0,
 “rows” : 2004
 }
}
]
\`\`\`

If you issue the query again, you should notice your results updating.

Right now all the results you are getting back are being aggregated into a single timestamp bucket. What if we wanted to see our aggregations on a per minute basis? What field can we change in the query to accomplish this?

If you loudly exclaimed “we can change granularity to minute”, you are absolutely correct! We can specify different granularities to bucket our results, like so:

    <code>
    {
        "queryType": "timeseries", 
        "dataSource": "webstream", 
        "intervals": [
            "2010-01-01/2020-01-01"
        ], 
        "granularity": "minute", 
        "aggregations": [
            {
                "type": "count", 
                "name": "rows"
            }, 
            {
                "type": "doubleSum", 
                "fieldName": "known_users", 
                "name": "known_users"
            }
        ]
    }
    </code>

This gives us something like the following:

\`\`\`json
[
 {
 [timestamp]() “2013-07-18T19:39:00.000Z”,
 [result]() {
 [known\_users]() 33,
 [rows]() 76
 }
 },
 {
 [timestamp]() “2013-07-18T19:40:00.000Z”,
 [result]() {
 [known\_users]() 105,
 [rows]() 221
 }
 },
 {
 [timestamp]() “2013-07-18T19:41:00.000Z”,
 [result]() {
 [known\_users]() 53,
 [rows]() 167
 }
 },
…
\`\`\`

Solving a Problem
-----------------

One of Druid’s main powers is to provide answers to problems, so let’s pose a problem. What if we wanted to know what the top states in the US are, ordered by the number of visits by known users over the last few minutes? To solve this problem, we have to return to the query we introduced at the very beginning of this tutorial, the [GroupByQuery](GroupByQuery.html). It would be nice if we could group by results by dimension value and somehow sort those results… and it turns out we can!

Let’s create the file:

    group_by_query.body</pre>
    and put the following in there:
    <pre><code>
    {
        "queryType": "groupBy", 
        "dataSource": "webstream", 
        "granularity": "all", 
        "dimensions": [
            "geo_region"
        ], 
        "orderBy": {
            "type": "default", 
            "columns": [
                {
                    "dimension": "known_users", 
                    "direction": "DESCENDING"
                }
            ], 
            "limit": 10
        }, 
        "aggregations": [
            {
                "type": "count", 
                "name": "rows"
            }, 
            {
                "type": "doubleSum", 
                "fieldName": "known_users", 
                "name": "known_users"
            }
        ], 
        "filter": {
            "type": "selector", 
            "dimension": "country", 
            "value": "US"
        }, 
        "intervals": [
            "2012-10-01T00:00/2020-01-01T00"
        ]
    }
    </code>

Woah! Our query just got a way more complicated. Now we have these [Filters](Filters.html) things and this [OrderBy](OrderBy.html) thing. Fear not, it turns out the new objects we’ve introduced to our query can help define the format of our results and provide an answer to our question.

If you issue the query:

    <code>curl -X POST 'http://localhost:8083/druid/v2/?pretty' -H 'content-type: application/json'  -d @group_by_query.body</code>

You should see an answer to our question. For my stream, it looks like this:

\`\`\`json
[
 {
 [version]() “v1”,
 [timestamp]() “2012-10-01T00:00:00.000Z”,
 [event]() {
 [geo\_region]() “RI”,
 [known\_users]() 359,
 [rows]() 143
 }
 },
 {
 [version]() “v1”,
 [timestamp]() “2012-10-01T00:00:00.000Z”,
 [event]() {
 [geo\_region]() “NY”,
 [known\_users]() 187,
 [rows]() 322
 }
 },
 {
 [version]() “v1”,
 [timestamp]() “2012-10-01T00:00:00.000Z”,
 [event]() {
 [geo\_region]() “CA”,
 [known\_users]() 145,
 [rows]() 466
 }
 },
 {
 [version]() “v1”,
 [timestamp]() “2012-10-01T00:00:00.000Z”,
 [event]() {
 [geo\_region]() “IL”,
 [known\_users]() 121,
 [rows]() 185
 }
 },
…
\`\`\`

Feel free to tweak other query parameters to answer other questions you may have about the data.

Next Steps
----------

What to know even more information about the Druid Cluster? Check out [Tutorial: The Druid Cluster](Tutorial:-The-Druid-Cluster.html)
Druid is even more fun if you load your own data into it! To learn how to load your data, see [Loading Your Data](Loading-Your-Data.html).

Additional Information
----------------------

This tutorial is merely showcasing a small fraction of what Druid can do. If you are interested in more information about Druid, including setting up a more sophisticated Druid cluster, please read the other links in our wiki.

And thus concludes our journey! Hopefully you learned a thing or two about Druid real-time ingestion, querying Druid, and how Druid can be used to solve problems. If you have additional questions, feel free to post in our [google groups page](http://www.groups.google.com/forum/#!forum/druid-development).
