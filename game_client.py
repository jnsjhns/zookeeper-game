import argparse
import json
import xmlrpc.client

from config import NODES

def parse_args():
    parser = argparse.ArgumentParser(
        description="Interactively play the distributed number game"
    )
    parser.add_argument(
        "--node",
        type=int,
        required=True,
        choices=sorted(NODES),
        help="ID of the node to contact (1-5)",
    )
    return parser.parse_args()


def read_value(prompt):
    try:
        value = int(input(prompt))
    except ValueError:
        print("Enter a whole number between 0 and 100.")
        return None

    if not 0 <= value <= 100:
        print("The number must be between 0 and 100.")
        return None

    return value


def main():
    # Parse command-line arguments to determine which node to connect to
    args = parse_args()
    address = NODES[args.node]
    url = f"http://{address}"

    # Connect to the specified node and interactively play the game
    try:
        with xmlrpc.client.ServerProxy(url, allow_none=True) as client:
            status = client.hello(args.node)
            print(
                f"Connected to Node {status['node_id']} "
                f"(role={status['role']}, leader={status['leader_id']})"
            )

            while True:
                print("\n[w] Write target  [g] Guess  [r] Read target  [q] Quit")
                command = input("> ").strip().lower()

                if command in {"q", "quit"}:
                    return

                # Handle write, guess, and read commands
                if command in {"w", "write"}:
                    value = read_value("New target (0-100): ")
                    if value is not None:
                        # Send the write request to the node and print the result
                        result = client.write(value)
                        print(json.dumps(result, indent=2))
                    continue

                if command in {"g", "guess"}:
                    value = read_value("Your guess (0-100): ")
                    if value is not None:
                        # Send the guess request to the node and print the result, including round status
                        result = client.guess(value)
                        round_result = result.get("round", {})
                        if (
                            round_result.get("status") == "committed"
                            and round_result.get("new_target") is not None
                        ):
                            # Print the round result if the round has been committed and a new target has been selected
                            print(
                                "Round complete: "
                                f"old target = {round_result['old_target']}, "
                                f"new target = {round_result['new_target']} "
                                f"(selected guess = "
                                f"{round_result['selected_guess']})"
                            )
                        elif round_result.get("status") == "collecting":
                            # Print the round status if the round is still collecting guesses
                            print(
                                "Guess recorded: "
                                f"{round_result['guesses_received']}/"
                                f"{round_result['guesses_required']} guesses; "
                                f"target remains "
                                f"{round_result['target']}"
                            )
                        print(json.dumps(result, indent=2))
                    continue

                if command in {"r", "read"}:
                    print(json.dumps(client.read(), indent=2))
                    continue

                print("Choose w, g, r, or q.")

    except (OSError, xmlrpc.client.Error) as error:
        raise SystemExit(
            f"Could not complete the client request to Node "
            f"{args.node} at {url}: {error}"
        ) from error


if __name__ == "__main__":
    main()
