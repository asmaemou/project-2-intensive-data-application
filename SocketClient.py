import socket

def send_data(data):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(('0.0.0.0', 9999))  # Replace 'LoadBalancer_IP' with the actual IP
    client.send(data.encode('utf-8'))
    response = client.recv(4096).decode('utf-8')
    print(response)
    client.close()

if __name__ == "__main__":
    data = "example data"
    send_data(data)
