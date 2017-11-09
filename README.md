# LiteDFS: Lightweight Distributed File System

## Overview

`LiteDFS` is a simple distributed file system with the following characteristics:
*    Oriented on manipulation with large bulks of data (chunks): default chunk size is 4Mb
*    Provides abstraction of a flat file
*    Works with raw partitions
*    Allocation model: cyclic buffer. Data is never deallocated, when end of partition is reached, current position is moved to the begging of partition. So it is assumed that size of the storage is large enough to fit all data, either data is iteratively written in FIFO order (new iteration can overwrite data written by previous iteration)
*    No centralized metadata server: location of chunks is maintained by clients themselves
*    Fault tolerance achieved by redundant storage of chunks
*    DFS can be splitted in cliques to optimize network communications and provide uniform filling of the newly added nodes
*    DFS nodes can be added on the fly
*    System can survive failure of one or more DFS nodes (including corruption of disk)
*    Recovery of crashed nodes is supported


## DFS client

DFS client (process needed to work with large volumes of data) should used DFSFile class. It provides standard set of operations with restriction that unit of write is always chunk (you can use BufferedOutput class to group data in chunks). It is possible to use virtual file handle interface (VFileHandle) which allows to work in the same way with local Unix files and DFS files.

Client knows nothing about location of DFS nodes. Instead of it DFS nodes are connected to the clients. So client works with online subset of DFS nodes which are connected to it. When new chunk has to be written, a client randomly choose one (or more - depending of redundancy) of the nodes within clique and sends chunk to it. DFS node replies with the offset of the chunk within node's storage. And finally client saves position of the chunk (node identifier + offset within node) in its INODE table.

Example of DFS usage:

```c++
    DFS::instance.listen("localhost:5101"); // spawn thread accepting connections of DFS nodes

    DFSFile file;
    VFileHandle* vfh;
    if (useDfs) {         
        file.open("inodetab.dfs", 0, DFS_INODE_TABLE_SIZE, DFS_CACHE_SIZE, 0);
        vfh = new DFSFileHandle();
        vfh->assign(file);
    } else {
        int fd = open("locafile.img", O_RDWR, 0);
        vfh = new UnixFileHandle();
        vfh->assign(fd);        
    }
    // Write some data
    BufferedOutputStream buf(BLOCK_SIZE);
    buf.open(vfh);
    buf.write(someData, sizeof(someData);
    buf.flush(true);

    // Read data from the file
    if (vph->pread(buf, size, pos, V_NOWAIT)) { 
        ...
    }
```

DFS file is identified by its INODE table: information about location of DFS chunks. There are can be arbitrary number of files in DFS. But file can be written only by one process. Two or more processes can read the same file if them share INODE table or send part of them to each other needed to map used region of the file.

DFS client should accept connections from DFS nodes. It can be either done by client application itself: it should just listen port, accept connections, read DFS node id (4 bytes) and invoke `DFS::instance.addChannel` method. Or alternatively client can use DFS::instance.listen method which will spawn separate thread for accepting DFS node connections.

## DFS server

DFS data is managed by DFS servers. DFS server is a process representing DFS node. It works with some physical storage device (OS file or raw partition). It is possible to run several DFS nodes at a single host. The desired configuration of DFS storage is a large number of inexpensive servers with minimal memory (2Gb is more than enough) and slow CPU. But it should contains as much HDDs as possible (usually 4 for standard 1U rack server). So at each such host 4 DFS servers are launched. With 100 such servers equipped with four 1Tb disks, total number of DFS nodes will be 400 and total capacity - 400Tb. Taking in account redundancy (2 or 3) and reservation of space needed to avoid overflow, actual storage size will be about 100Tb.

Below is an example of starting DFS servers:

```c++
i=1
j=0
while [ $i -le $N_DFS_HOSTS ] 
do 
    for d in a b c d  
    do 
        ssh dfs$i "cd ./dfs_server $j /dev/sd$d -clients dfs_clients.lst" &
	j=`expr $j + 1`
    done
    i=`expr $i + 1`
done
```

The file dfs_clients.lst contains addresses of DFS clients. Address is specified in the following format: `NAME:PORT`. Below is an example of dfs_clients.lst file:

```
mediaserver1:5001
mediaserver2:5002
mediaserver3:5003
mediaserver4:5004
```

You can certainly use IP address instead of name of the server.

In case of some host failure, all DFS servers running at it should be restart. In case of disk crash/corruption of data on the disk node has to be recovered. It can be done by launching dfs_server with -recovery flag.

DFS nodes can be organized in cliques. There are two purposes for it. First of all modern network hardware is not able to provide equal throughput between all nodes of the cluster. Nodes are connected to the network through switches and typical number of nodes connected to one switch is about 30-50 (switches supported more connections are significantly more expensive). Most switches provide 100Mb throughput between any pair of nodes connected to the switch. But as total number of nodes in large cluster is usually more than 100 (sometimes even more than thousand), it is not possible to connect all nodes to one switch. And if nodes connected to different switches need to exchange data, than them have to share interswitch link which is used to be cluster's bottleneck. If data stored in DFS can be split in two subsets: smaller but more frequently used and larger but rarely used, it is possible to place more frequently used data at DFS nodes connected to the same switch as consumers of this data. For example in search engine an example of frequently accessed data is inverse index and speed of extraction of content of documents and images is less important. In LiteDFS it can be achieved be grouping these DFS nodes in separate clique. The client should specify this clique identifier when writing this data to DFS.

Another role of clique is uniform consumption of disk space. `LiteDFS` randomly distribute chunks between all available nodes. So used disk space is almost the same at all nodes. But it we are close to exhaustion of cluster space and so add new DFS nodes, then new nodes will be filled at the same speed as old nodes, And it certainly leads to overflow of old nodes. To prevent this scenario it is expected that when old nodes are mostly filled we add new bulk of nodes and assign them new clique number. Then clients start to write data to the new clique, using old nodes only for retrieving data.


## Use cases
This DFS was used in WebAlta search engine for storing inverse index, content of documents and images. Also it was used in content oriented social network Fishker to store media data. The provided output bandwise with 20 cheap 1U rack servers (Celeron, 2Gb RAM, 4 SATA disks) is about 150Mb/sec.

## License
This software is distributed under MIT license. 