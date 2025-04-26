const express = require('express');
const { requestUserPresence, presence, connectWebSocket, isUserInGuild } = require('./ws');
const WebSocket = require('ws');
const userCache = new Map();
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
    if (!data?.user?.id) return console.error('Presence update event missing user ID:', data);
    lastOnlinePlatform(data);
    if (userSubscriptions[data.user.id]) {
        broadcastUpdate(await fullData(data));
    }
});

presence.on('get', async ({ data, userId }) => {
    if (data?.user?.id) {
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
    if (!data?.user?.id) return console.error("broadcastUpdate: data.user.id is undefined", data);
    const userId = data.user.id;
    userSubscriptions[userId].forEach((ws) => {
        if (ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ type: 'update', data }));
        }
    });
}

function sendPresenceData(data) {
    if (!data?.user?.id) return console.error("sendPresenceData: data.user.id is undefined", data);
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
    if (!data?.user?.id) throw new Error("fullData: data.user.id is undefined");
    const userId = data.user.id;
    let user;

    if (userCache.has(userId)) {
        const cachedData = userCache.get(userId);
        const currentTime = Date.now();
        if (currentTime - cachedData.timestamp < 5 * 60 * 1000) {
            user = cachedData.data;
        }
    }

    if (!user) {
        const res = await fetch(`https://discord.com/api/v9/users/${userId}/profile`, {
            headers: { authorization: process.env.ACCTOKEN }
        });
        const profileData = await res.json();
        const currentTime = Date.now();

        delete profileData.mutual_guilds;
        delete profileData.guild_badges;

        userCache.set(userId, { data: profileData, timestamp: currentTime });
        user = profileData;
    }

    try {
        const clientStatus = Object.keys(data.client_status || {}).length === 0 ? lastOnlineData[userId] : data.client_status;

        Object.keys(clientStatus || {}).forEach(platform => {
            let status = clientStatus[platform];
            const statusColors = {
                idle: "#f0b232",
                dnd: "#f23f43",
                online: "#23a55a",
                offline: "#80848e",
                streaming: "#593695"
            };

            user.badges = user.badges || [];
            user.badges.push({
                id: platform,
                description: (!status || status === "offline") ? `Last Online From ${capitalizeFirstChar(platform)}` : `Online From ${capitalizeFirstChar(platform)}`,
                status: status || "offline",
                color: statusColors[status] || "#80848e",
            });
        });
        user.status = data.status || "offline";
        user.activities = data.activities || [];
    } catch (e) {
        if (!process.env.WEBHOOK) return;
        const embed = {
            title: "Error Fetching User Profile",
            color: 16711680,
            description: `Hey <@&${process.env.SUPPORTROLE}>, Please Check Why <@${userId}> is getting the below error...\n\u0060\u0060\u0060json\n${e}\n\u0060\u0060\u0060`
        };

        await fetch(process.env.WEBHOOK, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ embeds: [embed] })
        });
    }
    return user;
}

function lastOnlinePlatform(data) {
    if (!data?.user?.id) return console.error("lastOnlinePlatform: data.user.id is undefined", data);
    const userId = data.user.id;

    if (data.status !== "offline") {
        const updatedStatus = {};
        for (let platform in data.client_status || {}) {
            updatedStatus[platform] = 'offline';
        }
        lastOnlineData[userId] = updatedStatus;
    } else {
        offline[userId] = {
            user: { id: userId },
            status: 'offline',
            client_status: lastOnlineData[userId],
            activities: []
        };
    }
}
