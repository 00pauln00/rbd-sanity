# rbd-sanity
Write and optionally verify block data to a Ceph RBD volume.
The I/O engine specifically tests for write order correctness by overwriting
the same block, asynchronously, many times with verifiable contents.  This
test mode is useful for detecting OSD write operations which are executed in
an order which differs from the application's write order.  More specifically,
this program is developed for testing the correctness of RBD volume contents
when Ceph RPC priorities are dynamically adjusted.

rbd-sanity writes are issued by a single thread which adjusts its asynchronous
I/O depth randomly throughout the test.  The purpose of random I/O depths is to
create varying sized I/O pipelines which will cause RPC priorities to be
frequently adjusted.

rbd-sanity uses mmap() to store its output.  The output file contains a single
value per block which holds the version number of the last write.  In verify
mode, this file is used to test the correctness of the RBD volume's block data.
