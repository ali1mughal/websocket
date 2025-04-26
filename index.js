const express = require('express');
const { requestUserPresence, presence, connectWebSocket, isUserInGuild } = require('./ws');
const WebSocket = require('ws');
const userCache = new Map();
require('dotenv').config();
const fetch = require('node-fetch'); // Required for fullData()

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

// Listen for presence updates from Discord
presence.on('update', async (data) => {
    if (!data?.user?.id) return;
    lastOnlinePlatform(data);

    if (userSubscriptions[data.user.id]) {
        broadcastUpdate(await fullData(data));
    }
});

// Listen when presence data is retrieved
presence.on('get', async ({ data, userId }) => {
    console.log(`📥 presence:get triggered for userId: ${userId}`);
    if (data?.user?.id) {
        console.log('✅ Got live presence data');
        sendPresenceData(await fullData(data));
        lastOnlinePlatform(data);
    } else {
        console.log('⚠️ No live presence, using offline fallback');
        const fallback = offline[userId] || {
            user: { id: userId },
            status: 'offline',
            client_status: { desktop: 'offline' },
            activities: []
        };
        sendPresenceData(await fullData(fallback));
    }
});

// Handle new WebSocket connections
wss.on('connection', (ws) => {
    if (wss.clients.size > 10) {
        ws.send(JSON.stringify({ type: 'error', message: 'Server busy. Max 10 users allowed at once.' }));
        return ws.close(4002, 'Max users limit reached');
    }

    ws.on('message', async (message) => {
        let data;
        try {
            data = JSON.parse(message);
        } catch (err) {
            return ws.send(JSON.stringify({ type: 'error', message: 'Invalid JSON' }));
        }

        if (data.type === 'subscribe') {
            const userId = data.userId;
            const numericUserId = Number(userId);

            if (isNaN(numericUserId)) {
                return ws.send(JSON.stringify({ type: 'error', code: 404, message: 'Invalid User ID' }), () =>
                    ws.close(4000, 'Invalid User ID'));
            }

            if (await isUserInGuild(userId) === 404) {
                return ws.send(JSON.stringify({
                    type: 'error',
                    code: 404,
                    message: `User Not In Our Server: ${process.env.INVITE}. Disconnecting...`
                }), () =>
                    ws.close(4001, `User Not In Our Server: ${process.env.INVITE}`));
            }

            // Subscribe and send data immediately
            requestUserPresence(userId);

            if (!userSubscriptions[userId]) {
                userSubscriptions[userId] = [];
            }

            userSubscriptions[userId].push(ws);
            console.log(`✅ Subscribed to user ${userId}`);

            // Send current cached or offline data immediately
            const fallback = offline[userId] || {
                user: { id: userId },
                status: 'offline',
                client_status: { desktop: 'offline' },
                activities: []
            };
            const enriched = await fullData(fallback);
            ws.send(JSON.stringify({ type: 'get', data: enriched }));
        }
    });

    ws.on('close', () => {
        for (const userId in userSubscriptions) {
            const clients = userSubscriptions[userId];
            if (clients.includes(ws)) {
                userSubscriptions[userId] = clients.filter(client => client !== ws);
                if (userSubscriptions[userId].length === 0) {
                    userCache.delete(userId);
                }
            }
        }
        console.log(`Connection Closed`);
    });
});

// Broadcast update to all subscribed sockets
function broadcastUpdate(data) {
    if (!data?.user?.id) return;
    const userId = data.user.id;

    if (userSubscriptions[userId]) {
        userSubscriptions[userId].forEach((ws) => {
            if (ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({ type: 'update', data }));
            }
        });
    }
}

// Send presence data to all sockets watching this user
function sendPresenceData(data) {
    if (!data?.user?.id) {
        console.log('❌ Cannot send presence: missing user ID');
        return;
    }

    const userId = data.user.id;
    console.log(`📤 Sending presence to subscribers of ${userId}`);

    if (userSubscriptions[userId]) {
        userSubscriptions[userId].forEach((ws) => {
            if (ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({ type: 'get', data }));
            }
        });
    }
}

// Fetch full user profile and enrich it with status badges
async function fullData(data) {
    if (!data?.user?.id) {
        throw new Error('Invalid data received in fullData');
    }

    const userId = data.user.id;
    let user;

    if (userCache.has(userId)) {
        const cached = userCache.get(userId);
        const now = Date.now();
        if (now - cached.timestamp < 5 * 60 * 1000) {
            user = cached.data;
        }
    }

    if (!user) {
        const res = await fetch(`https://discord.com/api/v9/users/${userId}/profile`, {
            headers: { authorization: process.env.ACCTOKEN }
        });
        const profileData = await res.json();

        delete profileData.mutual_guilds;
        delete profileData.guild_badges;

        const timestamp = Date.now();
        userCache.set(userId, { data: profileData, timestamp });
        user = profileData;
    }

    try {
        const clientStatus = Object.keys(data.client_status || {}).length === 0 ? lastOnlineData[userId] : data.client_status;
        user.badges = [];

        Object.keys(clientStatus || {}).forEach(platform => {
            const status = data.client_status?.[platform] || "offline";
            const statusColors = {
                idle: "#f0b232",
                dnd: "#f23f43",
                online: "#23a55a",
                offline: "#80848e",
                streaming: "#593695"
            };

            user.badges.push({
                id: platform,
                description: (status === "offline" ? `Last Online From ` : `Online From `) + capitalizeFirstChar(platform),
                status,
                color: statusColors[status] || "#80848e"
            });
        });

        user.status = data.status || "offline";
        user.activities = data.activities || [];
    } catch (err) {
        console.error("🔴 Error enriching user:", err);
        if (!process.env.WEBHOOK) return;

        const embed = {
            title: "Error Fetching User Profile",
            color: 16711680,
            description: `Hey <@&${process.env.SUPPORTROLE}>, please check why <@${userId}> is causing this:\n\`\`\`json\n${err}\`\`\``
        };

        await fetch(process.env.WEBHOOK, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ embeds: [embed] })
        });
    }

    return user;
}

// Track user's last online platform
function lastOnlinePlatform(data) {
    if (!data?.user?.id) return;

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

function capitalizeFirstChar(str) {
    return str.charAt(0).toUpperCase() + str.slice(1);
}
