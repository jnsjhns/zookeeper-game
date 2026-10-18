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


## Defence scenarios using the interactive client

The defence should be demonstrated through the interactive game client.

### Scenario 1: Normal write and read flow

1. Start all five nodes:

   ```powershell
   python app.py --node 1
   python app.py --node 2
   python app.py --node 3
   python app.py --node 4
   python app.py --node 5
   ```

2. Connect to any node with the interactive client:

   ```powershell
   python game_client.py --node 1
   ```

3. Write a value such as `10`:

   ```text
   > w
   New target (0-100): 10
   ```

4. Read the value back from the same or another node:

   ```text
   > r
   ```

5. Confirm that the value is visible across the cluster and that the leader has
   processed the write successfully.

This demonstrates that writes are accepted by the leader and that the target is
replicated consistently across followers.

### Scenario 2: Guessing flow with multiple clients

This is the main round-based case for the defence.

1. Start the full five-node cluster.
2. Connect a client to any node and set the target to `50`:

   ```text
   > w
   New target (0-100): 50
   ```

3. Open a second client and submit a guess from a different player:

   ```text
   > g
   Your guess (0-100): 30
   ```

4. Open a third client and submit another guess:

   ```text
   > g
   Your guess (0-100): 62
   ```

5. The leader collects both guesses and, once the configured threshold is hit,
   selects the closest value to the target. With the default threshold of two,
   the second guess completes the round.

6. The client should show a round result such as:

   ```text
   Round complete: old target = 50, new target = 62
   ```

7. Read the value again to verify that the new target is visible on the cluster.

This demonstrates the intended flow: multiple clients submit guesses, the leader
selects the closest value, and the next target is committed before the next
round starts.

### Scenario 3: Leader crash and recovery

1. Start all five nodes and confirm the leader is active.
2. Connect a client to any node and write a value such as `10`:

   ```text
   > w
   New target (0-100): 10
   ```

3. Stop the current leader process with `Ctrl+C`.
4. Wait until the remaining nodes print election and recovery messages, such as:

   ```text
   ELECTION START
   Node X elected in term ...
   RECOVERY START
   RECOVERY COMPLETE
   ```

5. Connect to a follower node and submit another write, for example `30`:

   ```text
   > w
   New target (0-100): 30
   ```

6. Read from multiple follower nodes and confirm that they all see the same
   updated target value.

7. Restart the old leader process:

   ```powershell
   python app.py --node 1
   ```

8. Wait for the restarted node to resynchronize. It should become a follower and
   contain the same committed history as the cluster.

This confirms that the cluster can recover from a leader failure, elect a new
leader, and continue to serve consistent writes and reads.

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


