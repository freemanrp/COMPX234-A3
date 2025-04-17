# client.py
import socket
import sys
#function to send a request to the server
def send_request(sock, line):
    parts = line.strip().split(" ", 2)
    if not parts:
        return
    #construct the command based on the input type
    if parts[0] == "PUT" and len(parts) >= 3:
        cmd = f"P {parts[1]} {parts[2]}"
    elif parts[0] == "GET" and len(parts) >= 2:
        #GET command expects a key
        cmd = f"G {parts[1]}"
    elif parts[0] == "READ" and len(parts) >= 2:
        #READ command expects a key 
        cmd = f"R {parts[1]}"
    else:
        print(f"Invalid line: {line.strip()}")
        return

    full_msg = f"{len(cmd)+4:03d} {cmd}"
    sock.sendall(full_msg.encode())
    response = sock.recv(1024).decode().strip()
    print(f"{line.strip()}: {response[4:]}")

def main():
    # Ensure the correct number of command-line arguments
    if len(sys.argv) != 4:
        print("Usage: python client.py <host> <port> <request_file>")
        return

    host = sys.argv[1]
    port = int(sys.argv[2])
    file = sys.argv[3]

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        with open(file, 'r') as f:
            for line in f:
                if line.strip():
                    send_request(s, line)

if __name__ == "__main__":
    main()
