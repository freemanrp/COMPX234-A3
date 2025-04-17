# server.py
import socket
import threading
import time
# Shared tuple space for storing key-value pairs
tuple_space = {}
# Lock to ensure thread-safe access to shared resources
lock = threading.Lock()
clients_connected = 0
# Dictionary to track operations statistics
total_ops = {'PUT': 0, 'GET': 0, 'READ': 0, 'ERR': 0}

def log_stats():
    """
    Logs server statistics periodically every 10 seconds.
    This includes the number of tuples, average key/value lengths,
    number of connected clients, and operation counts.
    """
    while True:
        time.sleep(10)
        with lock:
        # Number of tuples in the shared space
        n = len(tuple_space)
            avg_key = sum(len(k) for k in tuple_space) / n if n else 0
            avg_val = sum(len(v) for v in tuple_space.values()) / n if n else 0
            print(f"--- Server Stats ---")
            print(f"Tuples: {n}, Avg Key: {avg_key:.2f}, Avg Val: {avg_val:.2f}")
            print(f"Clients: {clients_connected}, Ops: {total_ops}")

def handle_client(conn):
    """
    Handles communication with a single client.
    Processes messages sent by the client and performs
    the requested operations (PUT, GET, READ).
    """
    global clients_connected
    with lock:
        clients_connected += 1
    try:
        while data := conn.recv(1024):
            msg = data.decode().strip()
            cmd = msg[4]  # R, G, P
            rest = msg[6:] # Extract the rest of the message (key or key-value pair)
            response = ""

            with lock:
                if cmd == 'R':
                    key = rest
                    if key in tuple_space:
                        val = tuple_space[key]
                        response = f"OK ({key}, {val}) read"
                        total_ops['READ'] += 1
                        # Increment READ count
                    else:
                        response = f"ERR {key} does not exist"
                        total_ops['ERR'] += 1
                        # Increment error count
                elif cmd == 'G':
                    key = rest
                    if key in tuple_space:
                        val = tuple_space.pop(key)
                        # Remove the key-value pair
                        response = f"OK ({key}, {val}) removed"
                        total_ops['GET'] += 1
                    else:
                        response = f"ERR {key} does not exist"
                        total_ops['ERR'] += 1
                elif cmd == 'P':
                    key, val = rest.split(" ", 1)
                    # Split the message into key and value
                    if key in tuple_space:
                        response = f"ERR {key} already exists"
                        total_ops['ERR'] += 1
                    else:
                        tuple_space[key] = val
                        # Add the key-value pair to the shared space
                        response = f"OK ({key}, {val}) added"
                        total_ops['PUT'] += 1

            length = str(len(response) + 4).zfill(3)
            conn.sendall((length + " " + response).encode())
    finally:
        conn.close()

def main():
    """
    Main function to start the server.
    Listens for incoming client connections and spawns
    a new thread to handle each client.
    """
    port = int(input("Enter port (e.g. 51234): "))
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('localhost', port))
    server.listen()
    print(f" Server is running on port {port}...")

    threading.Thread(target=log_stats, daemon=True).start()

    while True:
        conn, _ = server.accept()
        threading.Thread(target=handle_client, args=(conn,), daemon=True).start()

if __name__ == "__main__":
    main()
