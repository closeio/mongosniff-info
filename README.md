This script analyzes the output of a mongosniff command. If you want to
gain some insight into your production MongoDB traffic, follow these steps:

1. On the MongoDB server: `sudo tcpdump -i eth0 -w dump.pcap 'port 27017'`
   (caution: this will cause a CPU/disk IO spike, so make sure your machine
   can handle it).
2. Download the pcap file to your local machine (e.g. via scp).
3. `mongosniff --source FILE dump.pcap > sniffed.log`.
4. run this script, e.g.:
    python sniff_into sniffed.log sort
    python sniff_into sniffed.log aggregate


