import dgram from 'dgram';

const client = dgram.createSocket('udp4');

const message = Buffer.from('Hello via UDP');

client.send(message, 5000, 'localhost', () => {
    console.log('Sent, closing client');
    client.close();
});