const express = require('express');
const WebSocket = require('ws');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 3000;

const server = app.listen(port, () => {
    console.log(`✅ Server running at http://localhost:${port}`);
});

app.get('/', (req, res) => {
    res.send('✅ WebSocket Presence Server is running!');
});

const wss = new WebSocket.Server({ server });

/**
 * userSubscriptions: Map where key = userId, value = Set of WebSocket clients
 */
const userSubscriptions = new Map();

/**
 * Simulated in-memory presence data (mock)
 * You can replace this with real-time presence from Discord or your own backend.
 */
const presenceData = {
    '123456789': {
        status: 'online',
        platform: 'desktop',
        activities: ['Playing a game']
    },
    '987654321': {
        status: 'offline',
        platform: null,
        activities: []
    }
};

// === Handle Incoming Connections ===
wss.on('connection', (ws) => {
    console.log('🔗 New WebSocket connection.');

    ws.on('message', (message) => {
        let data;
        try {
            data = JSON.parse(message);
        } catch (err) {
            ws.send(JSON.stringify({ type: 'error', message: '❌ Invalid JSON' }));
            return;
        }

        if (data.type === 'subscribe') {
            const userId = String(data.userId);

            if (!userId || isNaN(Number(userId))) {
                ws.send(JSON.stringify({ type: 'error', message: '❌ Invalid User ID' }));
                ws.close(4001, 'Invalid User ID');
                return;
            }

            if (!userSubscriptions.has(userId)) {
                userSubscriptions.set(userId, new Set());
            }

            userSubscriptions.get(userId).add(ws);
            console.log(`✅ Subscribed to user ${userId}`);

            // Send current presence immediately
            sendPresenceToClient(ws, userId);
        }
    });

    ws.on('close', () => {
        // Remove ws from all user subscriptions
        for (const [userId, sockets] of userSubscriptions.entries()) {
            if (sockets.has(ws)) {
                sockets.delete(ws);
                if (sockets.size === 0) {
                    userSubscriptions.delete(userId);
                }
            }
        }
        console.log('❌ WebSocket disconnected.');
    });
});

// === Helper: Send data to all subscribers of a user ===
function broadcastPresence(userId) {
    const sockets = userSubscriptions.get(userId);
    const presence = presenceData[userId];

    if (sockets && presence) {
        const payload = JSON.stringify({
            type: 'presenceUpdate',
            userId,
            presence
        });

        for (const ws of sockets) {
            if (ws.readyState === WebSocket.OPEN) {
                ws.send(payload);
            }
        }
    }
}

// === Helper: Send current presence to one client ===
function sendPresenceToClient(ws, userId) {
    const presence = presenceData[userId] || {
        status: 'offline',
        platform: null,
        activities: []
    };

    if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({
            type: 'presenceInit',
            userId,
            presence
        }));
    }
}

// === Example: Simulate presence update every 10s ===
setInterval(() => {
    // Toggle user presence for demo
    const userId = '123456789';
    const current = presenceData[userId];

    presenceData[userId] = {
        status: current.status === 'online' ? 'offline' : 'online',
        platform: current.status === 'online' ? null : 'desktop',
        activities: current.status === 'online' ? [] : ['Watching a movie']
    };

    console.log(`🔄 Presence updated for user ${userId}: ${presenceData[userId].status}`);
    broadcastPresence(userId);
}, 10000);
