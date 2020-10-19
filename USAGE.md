Using Kitchen Sync
==================

Getting started
---------------

If you haven't already installed Kitchen Sync, please see [Installing Kitchen Sync](INSTALL.md).

If you'd like to see the wide range of exciting and confusing command-line options Kitchen Sync has to offer, simply run `ks`.  Otherwise, read on.

To synchronize between two databases on the local machine, you might start with:

```
ks --from postgresql://someuser:mypassword@localhost/sourcedb \
   --to postgresql://someuser:mypassword@localhost/targetdb
```

The `--from` and `--to` arguments are both "dburls" (database URLs).  This version supports protocols "postgresql" and "mysql" (which works equally well for mariadb).

To synchronize between machines on the same network, simply use the hostnames in the URL.  For example, to copy from a local database server to your own machine:

```
ks --from postgresql://myuser:secretpassword@ourserver.ourofficelan/sourcedb \
   --to postgresql://someuser:mypassword@localhost/targetdb
```

Or to synchronize between two directly-accessible database servers, but from a third machine which has Kitchen Sync installed on it:

```
ks --from postgresql://myuser:secretpassword@server1.cluster1/sourcedb \
   --to postgresql://anotheruser:greatpassword@server2.cluster2/targetdb
```

Please see "Transporting Kitchen Sync over SSH" below for more options, especially if you want to synchronize over the Internet or a WAN.

Sychronization between different database servers is directly supported:

```
ks --from mysql://someuser:mypassword@localhost/sourcedb \
   --to postgresql://someuser:mypassword@localhost/targetdb
```

Parallelizing
-------------

By default Kitchen Sync will start only one worker for each end, with a single database connection each.  To parallelize further, use the `--workers` option:

```
ks --from postgresql://someuser:mypassword@localhost/sourcedb \
   --to postgresql://someuser:mypassword@localhost/targetdb \
   --workers 4
```

In this case there would be 4 workers for each end.  In practice the appropriate number of workers depends mainly on your hardware.  Typically laptops are best with 2-4 workers and workstations with 4-8, but production-scale servers can easily scale up to 16 or more workers if there are an appropriate number of CPUs available, the disks are fast SSDs, etc.

What is it doing?
-----------------

Kitchen Sync is not very chatty by default.  Add the `--verbose` argument if you would like to see what it is working on.

Or, take to the next level and use `--debug` instead, if you would like to see how well/badly its synchronization protocol is working.

Transporting Kitchen Sync over SSH
----------------------------------

The above examples all access the database servers directly (over their native database protocol) from the machine that Kitchen Sync is started on.

For some use cases, direct access to the database server will not be allowed by the network security policies.

And for many other use cases, even though this may be possible, it's highly inefficient because it means that all the data is transferred over the network, even the data that already matches.

But Kitchen Sync does not have to be run on a single node.  When you run Kitchen Sync it actually splits itself into two halves - one for the 'from' end (i.e. source) and one for the 'to' end (i.e. target) - and these two halves can be separated by an SSH connection using the built-in transport option.

The Kitchen Sync protocol that runs between them mostly transfers only hashes of the content, so if large sections of the data already matches, the traffic actually sent over the network should be much smaller than the actual data compared.  (If the network is a bottleneck, this means the SSH transport option can even be beneficial over local networks, but it particularly helps over Internet-scale distances.)

To use the SSH transport you need to install Kitchen Sync on a machine that you can SSH to at the source datacentre.  Ideally you should install the same version as you will have at the other end, but we do attempt to maintain forward compatibility in case your server has an older version.

For example, if you want to copy from a database server called `server1.remotesite` and have Kitchen Sync installed on another machine `console1.remotesite`, and you want to copy to a `server2.localsite` server (which is on the same network you're running Kitchen Sync from):

```
ks --via console1.remotesite
   --from postgresql://myuser:secretpassword@server1.remotesite/sourcedb \
   --to postgresql://anotheruser:greatpassword@server2.localsite/targetdb
```

Of course, it is always more efficient to install Kitchen Sync directly on the source and target database servers itself, and avoid even the LANs at each end becoming a bottleneck:

```
ks --via server1.remotesite \
   --from postgresql://myuser:secretpassword@localhost/sourcedb \
   --to postgresql://anotheruser:greatpassword@localhost/targetdb
```

Note that in this case the `localhost` specified in the two connection strings is different - at the 'from' end it will be the `--via` server, but at the 'to' end it will be the server you are starting Kitchen Sync on.

(The `--via` option always controls what machine Kitchen Sync runs on for the 'from' end; there is no option to run Kitchen Sync's 'to' end on a different machine.)

Filtering data
--------------

Kitchen Sync allows you to filter the data as it is read from the source database.  You can filter out rows, replace the values in columns, or filter out entire tables.  See [Filtering data](FILTERING.md).
