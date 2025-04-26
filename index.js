const express = require('express');
const { requestUserPresence, presence, connectWebSocket, isUserInGuild } = require('./ws');
const WebSocket = require('ws');
const userCache = new Map();
const fetch = require('node-fetch');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 10000;

app.get('/', (req, res) => {
    res.send('Server is running!');
});

const server = app.listen(port, () => {
    console.log(`Server running on http://localhost:${port}`);
});

const wss = new WebSocket.Server({ server });
const lastOnlineData = {};
const userSubscriptions = {};
const offline = {};

connectWebSocket();

presence.on('update', async (data) => {
    lastOnlinePlatform(data);
    if (userSubscriptions[data.user.id]) {
        broadcastUpdate(await fullData(data));
    }
});

presence.on('get', async ({ data, userId }) => {
    if (data) {
        sendPresenceData(await fullData(data));
        console.log(data);
        lastOnlinePlatform(data);
    } else {
        sendPresenceData(await fullData(offline[userId] || { user: { id: userId }, status: 'offline', client_status: { desktop: 'offline' }, activities: [] }));
    }
});

wss.on('connection', (ws) => {
    ws.on('message', async (message) => {
        const data = JSON.parse(message);

        if (data.type === 'subscribe') {
            const userId = data.userId;
            const numericUserId = Number(userId);

            if (isNaN(numericUserId)) {
                return ws.send(JSON.stringify({ type: 'error', code: 404, message: 'Invalid User ID' }), () => ws.close(4000, 'Invalid User ID'));
            }

            if (await isUserInGuild(userId) === 404) {
                return ws.send(JSON.stringify({
                    type: 'error',
                    code: 404,
                    message: `User Not In Our Server: ${process.env.INVITE}. Disconnecting...`
                }), () => ws.close(4001, `User Not In Our Server: ${process.env.INVITE}`));
            }

            requestUserPresence(userId);

            if (!userSubscriptions[userId]) {
                userSubscriptions[userId] = [];
            }

            userSubscriptions[userId].push(ws);
            console.log(`Subscribed to user ${userId}`);
        }
    });

    ws.on('close', () => {
        for (const userId in userSubscriptions) {
            const userClients = userSubscriptions[userId];
            if (userClients.includes(ws)) {
                userSubscriptions[userId] = userClients.filter(client => client !== ws);
                userCache.delete(userId);
            }
        }
        console.log(`Connection Closed`);
    });
});

function broadcastUpdate(data) {
    const userId = data.user.id;
    if (userSubscriptions[userId]) {
        userSubscriptions[userId].forEach((ws) => {
            if (ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({ type: 'update', data }));
            }
        });
    }
}

function sendPresenceData(data) {
    const userId = data.user.id;
    if (userSubscriptions[userId]) {
        userSubscriptions[userId].forEach((ws) => {
            if (ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({ type: 'get', data }));
            }
        });
    }
}

function capitalizeFirstChar(str) {
    return str.charAt(0).toUpperCase() + str.slice(1);
}

async function fullData(data) {
    const userId = data.user.id;
    let user;

    // Check if the user data is cached and is still valid (less than 5 minutes old)
    if (userCache.has(userId)) {
        const cachedData = userCache.get(userId);
        const currentTime = Date.now();
        if (currentTime - cachedData.timestamp < 5 * 60 * 1000) { // 5 minutes in milliseconds
            user = cachedData.data;
        }
    } else {
        try {
            const res = await fetch(`https://discord.com/api/v9/users/${userId}/profile`, {
                headers: { authorization: `Bot ${process.env.ACCTOKEN}` }
            });

            if (!res.ok) {
                throw new Error(`Failed to fetch profile: ${res.statusText}`);
            }

            const data = await res.json();
            const currentTime = Date.now();

            delete data.mutual_guilds;
            delete data.guild_badges;

            userCache.set(userId, { data, timestamp: currentTime });
            user = data;
        } catch (error) {
            console.error("🔴 Error fetching user profile:", error);
            user = { user: { id: userId }, status: 'offline', client_status: { desktop: 'offline' }, activities: [] };
        }
    }

    return user;
}

function lastOnlinePlatform(data) {
    if (data.status !== "offline") {
        const updatedStatus = {};
        for (let platform in data.client_status) {
            updatedStatus[platform] = 'offline';
        }
        lastOnlineData[data.user.id] = updatedStatus;
    } else {
        offline[data.user.id] = {
            user: { id: data.user.id },
            status: 'offline',
            client_status: lastOnlineData[data.user.id],
            activities: []
        };
    }
}
