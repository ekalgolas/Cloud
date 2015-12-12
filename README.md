# Cloud
Cloud group C - Directory structure maintainence


README.txt
----------

Steps to be taken before running the CEPH environment:
-----------------------------------------------------

1. Check all the mds.server.id are set correctly for both MDS and replica nodes.
2. Check all the mds.inode.start, mds.inode.end and mds.current.inode are set correctly.
	Note: All the MDS should have unique start and end inode numbers. 
		  Also use the same set of inode valued for Replicas of a particular MDS node(optional).
3. Check all the mds.replica.info values are set correctly as per the required format in all the machines/nodes.
4. Check all the mds.partition.config values are set correctly as per the required format in all the machines/nodes.
5. Check all the ips of various nodes are set correct.
	Note: In the mds.<node id>.ip key name, the "node id" should concinde with all the mds.server.id provided for all the nodes.
6. Check whether all the clients "client.id" are given a unique id.
7. Check whether "server.type" in masternode.conf is set to "MDS".
8. Check whether "client.ceph.root" is set to the ip of the MDS containing root directory.

Steps to be taken before running the All Client environments:
------------------------------------------------------------

1. Make sure client.commandsFile file exists in the folder whereever it point to.
2. Make sure client.masterIp and client.masterPort are set correct and pointing to the master containing the root folder.

Steps to be taken before running the All NFS environments:
----------------------------------------------------------
1. Make sure the path exists for the server.nfs.folder exist on all the NFS nodes.