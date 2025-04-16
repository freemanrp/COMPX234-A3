# server.py
import socket
import threading
import time

tuple_space = {}
lock = threading.Lock()
clients_connected = 0
total_ops = {'PUT': 0, 'GET': 0, 'READ': 0, 'ERR': 0}

def log_stats():
    while True:
        time.sleep(10)
        with lock:
            n = len(tuple_space)
            avg_key = sum(len(k) for k in tuple_space) / n if n else 0
            avg_val = sum(len(v) for v in tuple_space.values()) / n if n else 0
            print(f"--- Server Stats ---")
            print(f"Tuples: {n}, Avg Key: {avg_key:.2f}, Avg Val: {avg_val:.2f}")
            print(f"Clients: {clients_connected}, Ops: {total_ops}")

def handle_client(conn):
    global clients_connected
    with lock:
        clients_connected += 1
    try:
        while data := conn.recv(1024):
            msg = data.decode().strip()
            cmd = msg[4]  # R, G, P
            rest = msg[6:]
            response = ""

            with lock:
                if cmd == 'R':
                    key = rest
                    if key in tuple_space:
                        val = tuple_space[key]
                        response = f"OK ({key}, {val}) read"
                        total_ops['READ'] += 1
                    else:
                        response = f"ERR {key} does not exist"
                        total_ops['ERR'] += 1
                elif cmd == 'G':
                    key = rest
                    if key in tuple_space:
                        val = tuple_space.pop(key)
                        response = f"OK ({key}, {val}) removed"
                        total_ops['GET'] += 1
                    else:
                        response = f"ERR {key} does not exist"
                        total_ops['ERR'] += 1
                elif cmd == 'P':
                    key, val = rest.split(" ", 1)
                    if key in tuple_space:
                        response = f"ERR {key} already exists"
                        total_ops['ERR'] += 1
                    else:
                        tuple_space[key] = val
                        response = f"OK ({key}, {val}) added"
                        total_ops['PUT'] += 1

            length = str(len(response) + 4).zfill(3)
            conn.sendall((length + " " + response).encode())
    finally:
        conn.close()

def main():
    port = int(input("Enter port (e.g. 51234): "))
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('localhost', port))
    server.listen()
    print(f"🌟 Server is running on port {port}...")

    threading.Thread(target=log_stats, daemon=True).start()

    while True:
        conn, _ = server.accept()
        threading.Thread(target=handle_client, args=(conn,), daemon=True).start()

if __name__ == "__main__":
    main()