import net from 'net';

const client = net.createConnection({port: 4000}, () =>{
    console.log('Connected to server');
    client.write('Hello from client');
});
client.on('data', (data) =>{
    console.log('Server replied:', data.toString());
    client.end();
});
