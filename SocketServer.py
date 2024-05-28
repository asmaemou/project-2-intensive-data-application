import socket
import threading
from loadBalancer import main  # Import the main function from loadBalancer

def handle_client(client_socket):
    request = client_socket.recv(1024).decode('utf-8')
    print(f"Received: {request}")

    # Process the request (insert data into the least loaded partition)
    main(request)  # Call main with the data received from the client

    client_socket.send("Data inserted successfully".encode('utf-8'))
    client_socket.close()

def main_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('0.0.0.0', 9999))
    server.listen(5)
    print("Listening on port 9999")

    while True:
        client_socket, addr = server.accept()
        print(f"Accepted connection from {addr}")

        client_handler = threading.Thread(target=handle_client, args=(client_socket,))
        client_handler.start()

if __name__ == "__main__":
    main_server()
