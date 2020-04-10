package com.example.loganalytics.pipeline.generators;

public class SquidGenerator extends LogGenerator {

    private static final String[] squidSamples = new String[]{"%d.000   5265 75.133.181.135 TCP_TUNNEL/200 4532 CONNECT odr.mookie1.com:443 - HIER_DIRECT/35.190.90.30 -",
            "%d.000   5308 75.133.181.135 TCP_TUNNEL/200 9892 CONNECT pagead2.googlesyndication.com:443 - HIER_DIRECT/216.58.195.66 -",
            "%d.0000   4163 75.133.181.135 TCP_TUNNEL/200 1573 CONNECT pagead2.googlesyndication.com:443 - HIER_DIRECT/216.58.195.66 -",
            "%d.0000   1419 75.133.181.135 TCP_TUNNEL/200 4524 CONNECT mb.moatads.com:443 - HIER_DIRECT/54.219.137.51 -",
            "%d.0000    5230 75.133.181.135 TCP_TUNNEL/200 1157 CONNECT ping.chartbeat.net:443 - HIER_DIRECT/18.210.38.187 -",
            "%d.0000    7864 75.133.181.135 TCP_TUNNEL/200 1120 CONNECT www.facebook.com:443 - HIER_DIRECT/157.240.3.35 -",
            "%d.0000    4281 75.133.181.135 TCP_TUNNEL/200 2163 CONNECT clients4.google.com:443 - HIER_DIRECT/172.217.0.46 -",
            "%d.0000    4890 75.133.181.135 TCP_TUNNEL/200 4037 CONNECT tpc.googlesyndication.com:443 - HIER_DIRECT/172.217.6.33 -",
            "%d.0000    5142 75.133.181.135 TCP_TUNNEL/200 463 CONNECT www.summerhamster.com:443 - HIER_DIRECT/3.225.201.66 -",
            "%d.0000    2404 75.133.181.135 TCP_TUNNEL/200 19304 CONNECT cdn.cnn.com:443 - HIER_DIRECT/184.28.183.201 -",
            "%d.0000   5262 75.133.181.135 TCP_TUNNEL/200 4546 CONNECT p.adsymptotic.com:443 - HIER_DIRECT/104.18.102.194 -",
            "%d.0000    7306 75.133.181.135 TCP_TUNNEL/200 3905 CONNECT log.outbrainimg.com:443 - HIER_DIRECT/66.225.223.127 -",
            "%d.0000   12542 75.133.181.135 TCP_TUNNEL/200 6648 CONNECT ads.pubmatic.com:443 - HIER_DIRECT/23.1.245.5 -"};

    public SquidGenerator() {
        super(squidSamples);
    }
}
