#### Analyze your MongoDB traffic

This script analyzes the output of a [mongosniff](https://docs.mongodb.org/manual/reference/program/mongosniff/)
command, giving you an insight into your MongoDB traffic. You can see which queries retrieve the most data
from your database or look at aggregate per-collection statistics. Here's what you should do to analyze
your production traffic:

1. On the MongoDB server: `sudo tcpdump -i eth0 -w dump.pcap 'port 27017'`
   (caution: this will cause a CPU/disk IO spike, so make sure your machine
   can handle it).
2. Download the pcap file to your local machine (e.g. via scp).
3. `mongosniff --source FILE dump.pcap > sniffed.log`
4. Run this script, e.g.:
    * `python sniff_into sniffed.log sort`
    * `python sniff_into sniffed.log aggregate`


#### When is this script useful?

This script is useful when your monitoring tools indicate that there's a lot
of data sent/received by your MongoDB server, but you're not certain which
queries are responsible for it.
