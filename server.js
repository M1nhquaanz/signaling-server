const WebSocket = require('ws');

const PORT = process.env.PORT || 8765;
const wss = new WebSocket.Server({ port: PORT });

const rooms = new Map();
const clients = new Map();

function generateUniqueRoomId() {
    let roomId;
    do {
        roomId = 'NET_' + Math.random().toString(36).substr(2, 5).toUpperCase();
    } while (rooms.has(roomId));
    return roomId;
}

wss.on('connection', (ws) => {
    let clientPeerId = null;
    ws.isAlive = true;

    ws.on('pong', () => {
        ws.isAlive = true;
    });

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);

            switch (data.type) {
                case 'create-room': {
                    clientPeerId = data.peerId;
                    const newRoomId = generateUniqueRoomId();
                    
                    rooms.set(newRoomId, new Set([data.peerId]));
                    clients.set(data.peerId, ws);

                    ws.send(JSON.stringify({
                        type: 'room-created',
                        roomId: newRoomId
                    }));
                    break;
                }

                case 'join-room': {
                    clientPeerId = data.peerId;
                    const { roomId, peerId } = data;

                    if (!rooms.has(roomId)) {
                        ws.send(JSON.stringify({
                            type: 'room-not-found',
                            roomId: roomId
                        }));
                        return;
                    }

                    const roomPeers = rooms.get(roomId);
                    const existingPeers = Array.from(roomPeers);

                    roomPeers.add(peerId);
                    clients.set(peerId, ws);

                    ws.send(JSON.stringify({
                        type: 'room-joined',
                        roomId: roomId,
                        existingPeers: existingPeers
                    }));

                    existingPeers.forEach(existingPeerId => {
                        const targetWs = clients.get(existingPeerId);
                        if (targetWs && targetWs.readyState === WebSocket.OPEN) {
                            targetWs.send(JSON.stringify({
                                type: 'user-joined',
                                peerId: peerId
                            }));
                        }
                    });
                    break;
                }

                case 'offer':
                case 'answer':
                case 'candidate': {
                    const targetWs = clients.get(data.targetId);
                    if (targetWs && targetWs.readyState === WebSocket.OPEN) {
                        targetWs.send(JSON.stringify(data));
                    }
                    break;
                }

                case 'chat-message': {
                    const roomPeers = rooms.get(data.roomId);
                    if (roomPeers) {
                        roomPeers.forEach(peerId => {
                            if (peerId !== data.senderId) {
                                const targetWs = clients.get(peerId);
                                if (targetWs && targetWs.readyState === WebSocket.OPEN) {
                                    targetWs.send(JSON.stringify(data));
                                }
                            }
                        });
                    }
                    break;
                }

                case 'leave-room': {
                    handleUserLeave(data.roomId, data.peerId);
                    break;
                }
            }
        } catch (e) {}
    });

    ws.on('close', () => {
        if (clientPeerId) {
            clients.delete(clientPeerId);
            rooms.forEach((peers, roomId) => {
                if (peers.has(clientPeerId)) {
                    handleUserLeave(roomId, clientPeerId);
                }
            });
        }
    });
});


const interval = setInterval(() => {
    wss.clients.forEach((ws) => {
        if (ws.isAlive === false) return ws.terminate();
        ws.isAlive = false;
        ws.ping();
    });
}, 30000);

wss.on('close', () => {
    clearInterval(interval);
});

function handleUserLeave(roomId, peerId) {
    if (rooms.has(roomId)) {
        const roomPeers = rooms.get(roomId);
        roomPeers.delete(peerId);

        roomPeers.forEach(remainingPeerId => {
            const targetWs = clients.get(remainingPeerId);
            if (targetWs && targetWs.readyState === WebSocket.OPEN) {
                targetWs.send(JSON.stringify({
                    type: 'user-left',
                    peerId: peerId
                }));
            }
        });

        if (roomPeers.size === 0) {
            rooms.delete(roomId);
        }
    }
}

console.log(`Signaling Server running on port ${PORT}`);