# Learnings

## Kafka offset initialiser

I initially had the kafka offset initialiser set to "earliest" as I think this was just the first thing I read in the docs. However, down the line this caused me to think that I had my window assigner wrong. Why?

The earliest offset for this stream was set to `03-05`. I was testing this days later. So when I assigned my window to be a tumbling window of duration ten seconds, I was expecting results to appear every... ten seconds. They did not. I have not looked into the details but it seems as though Flink just did all the older timestamps in parallel. Once I left the stream to run for a few minutes, it "caught up" and indeed I saw results every 10 seconds.

From here on out, for testing, I have set the offset initialiser to "latest" and the results print out as I initially expected they would!
