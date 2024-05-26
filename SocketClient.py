import socket
#Here is a simple client script that sends data to the load balancer server.


def send_data(data):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(('LoadBalancer_IP', 9999))
    client.send(data.encode('utf-8'))
    response = client.recv(4096).decode('utf-8')
    print(response)
    client.close()

if __name__ == "__main__":
    data = "example data"
    send_data(data)
