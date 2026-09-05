import net from 'net';

const server = net.createServer((socket) =>{
    console.log('Client connected');

    // Every time the client sends data, this callback runs
    // The data parameter contains the incoming bytes
    socket.on('data', (data) =>{   // data = A sealed envelope with binary contents
        console.log('Received:', data.toString());
        socket.write(`Echo: ${data.toString()}`);
    });

    // Runs when the client closes the connection
    socket.on('end', () => {
        console.log('Client disconnected');
    });
});

/*
Creates a TCP server object
The arrow function is a callback that runs whenever a client connects
The socket parameter represents the connection to that specific client

Socket = The Connection:
Each client gets their OWN socket
It's like a dedicated phone line to that client
You can read from it (receive data) and write to it (send data)

*/ 

server.listen(4000, () =>{
    console.log('TCP server listening on port 4000');
});
