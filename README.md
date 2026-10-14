# Distributed ZooKeeper-like Coordination System

This project implements a simplified ZooKeeper-like coordination layer for a
distributed number game. It uses XML-RPC for communication between five local
node processes.

The system supports:

- Automatic leader election
- Heartbeat-based failure detection
- Persistent replicated logs
- Write replication with quorum-based commits
- Recovery after leader failure
- Automatic log synchronization for nodes that rejoin later

## Project structure

```text
app.py                     Starts one node process
config.py                  Node configuration
node.py                    Node server and RPC endpoint registration
state.py                   Volatile node state
rpc.py                     XML-RPC transport layer
log_store.py               Persistent transaction log
election.py                Leader election
heartbeat.py               Leader heartbeat monitoring
replication.py             Proposal, ACK, and commit handling
recovery.py                Log synchronization after election
sync.py                    Catch-up synchronization during normal operation
cluster_smoke_test.py      Command-line write/read smoke test
data/                      Persistent log files
```

## Requirements

- Python 3.10 or newer
- No external Python packages are required

The implementation uses Python's standard `xmlrpc` modules.

## Start the cluster

Open one terminal per node and run:

```powershell
python app.py --node 1
python app.py --node 2
python app.py --node 3
python app.py --node 4
python app.py --node 5
```

Each node listens on port `8000 + node_id`:

| Node | Address |
|---|---|
| Node 1 | `127.0.0.1:8001` |
| Node 2 | `127.0.0.1:8002` |
| Node 3 | `127.0.0.1:8003` |
| Node 4 | `127.0.0.1:8004` |
| Node 5 | `127.0.0.1:8005` |

Wait until every node prints:

```text
RECOVERY COMPLETE
```

## Run a write test

Use `cluster_smoke_test.py` to send a write to any node:

```powershell
python cluster_smoke_test.py --node 1 --value 42
```

To send the write through a follower:

```powershell
python cluster_smoke_test.py --node 3 --value 90
```

The script prints:

- The contacted node and its current role
- The result of the write request
- The current target value after the write

## Read the current target

To inspect the current target through a node:

```powershell
python cluster_smoke_test.py --node 5 --value 42
```

The final `Read result` line shows the value currently visible on that node.

For a proper consistency check, run the same command against several nodes and
compare the reported target values.

## Test the leader-crash flow

1. Start all five nodes.
2. Perform a successful write:

   ```powershell
   python cluster_smoke_test.py --node 1 --value 10
   ```

3. Identify the current leader from the node output.
4. Stop the leader process with `Ctrl+C`.
5. Wait for the remaining nodes to print election and recovery messages:

   ```text
   ELECTION START
   Node X elected in term ...
   RECOVERY START
   RECOVERY COMPLETE
   ```

6. Send another write through a follower:

   ```powershell
   python cluster_smoke_test.py --node 3 --value 30
   ```

7. Verify that the new value is visible on the remaining nodes:

   ```powershell
   python cluster_smoke_test.py --node 2 --value 30
   python cluster_smoke_test.py --node 4 --value 30
   python cluster_smoke_test.py --node 5 --value 30
   ```

8. Restart the old leader:

   ```powershell
   python app.py --node 1
   ```

9. Wait for the old leader to synchronize its log. It should become a follower
   and contain the same committed entries as the other nodes.

## Logs

Each node stores its transaction log in:

```text
data/node_<node_id>.log
```

Each log entry contains:

```json
{
  "epoch": 1,
  "seq": 8,
  "op": "WRITE",
  "value": 100,
  "status": "COMMITTED"
}
```

- `epoch` identifies the leader generation.
- `seq` is the proposal sequence number within that epoch.
- `op` is either `WRITE` or `GUESS`.
- `status` is either `PROPOSED` or `COMMITTED`.


## Current status

Implemented and manually tested:

- Five-node local cluster
- Leader election
- Heartbeat monitoring
- Persistent logs
- Quorum-based write commits
- Recovery after leader failure
- Automatic catch-up synchronization for late-joining followers

Still to implement:

- Client guess flow
- Leader-side collection and selection of the closest guess
- Client application for interactive `Write` and `Guess` commands