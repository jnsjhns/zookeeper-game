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
game_client.py             Interactive Write/Guess/Read client
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

By default, a round completes after two committed guesses. The minimum is two;
there is no fixed upper limit. To change the number required, pass the same
`--guesses-per-round` value (2 or greater) to every node.

Each node listens on port `8000 + node_id`:

| Node | Address |
|---|---|
| Node 1 | `127.0.0.1:8001` |
| Node 2 | `127.0.0.1:8002` |
| Node 3 | `127.0.0.1:8003` |
| Node 4 | `127.0.0.1:8004` |
| Node 5 | `127.0.0.1:8005` |

## Play the number game

Start an interactive client connected to any node:

```powershell
python game_client.py --node 1
```

Run a separate client process in each player's terminal. At the prompt:

- Enter `w` to set or replace the target value (0-100).
- Enter `g` to submit a guess (0-100).
- Enter `r` to read the latest committed target.
- Enter `q` to exit.

The first player should use `w` to set the initial target. Each `g` submission
is replicated and committed as a `GUESS`. Once the configured number of guesses
has committed, the leader selects the guess closest to the target and commits
that value as the next target. If guesses are equally close, the earliest
committed guess wins. The target remains unchanged while guesses are being
collected. When a round completes, the client displays both the old and new
target values.

For example, with the default threshold of two guesses, set the target to 50,
then submit guesses of 30 and 62. The leader selects 62 and the next target
becomes 62. Players can run separate client processes and connect to different
nodes; follower nodes forward game requests to the leader.

All nodes in the cluster should use the same guess threshold. For example, to
require five guesses per round:

```powershell
python app.py --node 1 --guesses-per-round 5
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
  "status": "COMMITTED",
  "timestamp": "2026-10-17T09:30:00.123Z"
}
```

- `epoch` identifies the leader generation.
- `seq` is the proposal sequence number within that epoch.
- `op` is either `WRITE` or `GUESS`.
- `status` is either `PROPOSED` or `COMMITTED`.
- `timestamp` is the UTC time when the leader created the proposal. The same
  timestamp is replicated to every node. Older entries without a timestamp
  remain readable.


## Defence preparation

Implemented:

- Five-node local cluster
- Leader election
- Heartbeat monitoring
- Persistent logs
- Quorum-based write commits
- Recovery after leader failure
- Automatic catch-up synchronization for late-joining followers
- Interactive Write/Guess/Read client
- Closest-guess selection and next-round target commit

Still to prepare and verify:

- Run and capture evidence for all three defence scenarios: normal write,
  guessing, and leader crash/recovery
- Report with screenshots/recordings and a short analysis of design choices
- Source-code archive for submission before the defence