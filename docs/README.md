How it works
============

![Architecture](https://github.com/legionus/kavka/blob/master/docs/kavka-intro.dot.png)

Receiving a message
===================

![Message processing](https://github.com/legionus/kavka/blob/master/docs/kavka-message-processing.png)

1. Client sends a message to server.
2. Server read it.
3. Incoming message splited info chunks on fly.
4. Every chunk stored on local storage separately. The key is the digest of the
   chunk.
5. Since the chunk is stored on local storage when we make a record in etcd about
   this chunk with information about server which keep it.
6. Another kavka node looks at the etcd collection and see the emergence of the new
   record.
7. Node asks for chunk node that has it.
8. The obtained chunk is written to local storage and write in etcd record about
   yourself.
9. After all the chunks received and replicated to other nodes we make recored
   about message which conatins list of chunks.

