const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { WebcastPushConnection } = require('tiktok-live-connector');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static(path.join(__dirname, 'public')));

// Objek untuk menyimpan semua akun beserta memori log-nya
const monitoredAccounts = {};
let importQueue = [];
let isProcessingQueue = false;

io.on('connection', (socket) => {
    console.log('Client Dashboard terhubung');

    // Kirim seluruh data (termasuk memori log chat/gift) saat web pertama kali dibuka
    const initData = Object.keys(monitoredAccounts).map(user => ({
        username: user,
        status: monitoredAccounts[user].status,
        info: monitoredAccounts[user].info,
        logs: monitoredAccounts[user].logs,
        chatCount: monitoredAccounts[user].chatCount,
        giftCount: monitoredAccounts[user].giftCount
    }));
    socket.emit('initStreams', initData);

    // TAMBAH AKUN SATUAN
    socket.on('addStream', (data) => {
        let rawUsername = typeof data === 'string' ? data : data.username;
        const username = rawUsername.replace(/[@\s]/g, '').toLowerCase();
        const label1 = data.label1 || 'Tanpa Label 1';
        const label2 = data.label2 || 'Tanpa Label 2';

        if (monitoredAccounts[username] || importQueue.some(acc => acc.username === username)) {
            socket.emit('systemMsg', { type: 'error', msg: `Akun @${username} sudah ada di daftar atau antrean!` });
            return;
        }

        monitoredAccounts[username] = {
            status: 'checking',
            info: { startTime: null, viewers: 0, likes: 0, label1: label1, label2: label2 },
            logs: [], // Server-side Memory
            chatCount: 0,
            giftCount: 0,
            connection: null,
            lastPing: Date.now()
        };

        io.emit('streamAdded', { 
            username, status: 'checking', info: monitoredAccounts[username].info, 
            logs: [], chatCount: 0, giftCount: 0 
        });
        connectStream(username);
    });

    // TAMBAH AKUN BANYAK (BULK)
    socket.on('addBulkStreams', (accounts) => {
        if (!Array.isArray(accounts)) return;
        let validAccounts = [];
        for (const acc of accounts) {
            let rawUsername = typeof acc === 'string' ? acc : acc.username;
            const cleanUsername = rawUsername.replace(/[@\s]/g, '').toLowerCase();
            if (cleanUsername && !monitoredAccounts[cleanUsername] && !importQueue.some(q => q.username === cleanUsername)) {
                validAccounts.push({ username: cleanUsername, label1: acc.label1 || 'Tanpa Label 1', label2: acc.label2 || 'Tanpa Label 2' });
            }
        }
        if (validAccounts.length > 0) {
            importQueue.push(...validAccounts);
            socket.emit('systemMsg', { type: 'success', msg: `${validAccounts.length} akun dimasukkan ke antrean.` });
            if (!isProcessingQueue) processImportQueue();
        }
    });

    // HAPUS AKUN DARI PANTAUAN
    socket.on('removeStream', (username) => {
        if (monitoredAccounts[username]) {
            if (monitoredAccounts[username].connection) {
                try { monitoredAccounts[username].connection.disconnect(); } catch (e) {}
            }
            delete monitoredAccounts[username];
            io.emit('streamRemoved', { username });
        }
    });

    // FITUR BARU: BERSIHKAN SEMUA AKTIVITAS GLOBAL
    socket.on('clearAllActivities', () => {
        Object.keys(monitoredAccounts).forEach(user => {
            monitoredAccounts[user].logs = [];
            monitoredAccounts[user].chatCount = 0;
            monitoredAccounts[user].giftCount = 0;
        });
        // Beritahu semua client untuk reset UI
        io.emit('allActivitiesCleared');
    });

    // FITUR BARU: BERSIHKAN AKTIVITAS (Kembali ke 0)
    socket.on('clearActivity', (username) => {
        if (monitoredAccounts[username]) {
            monitoredAccounts[username].logs = [];
            monitoredAccounts[username].chatCount = 0;
            monitoredAccounts[username].giftCount = 0;
            
            // Beritahu semua client agar menghapus data di layarnya
            io.emit('activityCleared', username);
        }
    });
});

async function processImportQueue() {
    isProcessingQueue = true;
    let processedCount = 0;
    const totalBatch = importQueue.length;

    while (importQueue.length > 0) {
        const acc = importQueue.shift();
        monitoredAccounts[acc.username] = {
            status: 'checking',
            info: { startTime: null, viewers: 0, likes: 0, label1: acc.label1, label2: acc.label2 },
            logs: [], chatCount: 0, giftCount: 0,
            connection: null, lastPing: Date.now()
        };
        
        io.emit('streamAdded', { 
            username: acc.username, status: 'checking', info: monitoredAccounts[acc.username].info,
            logs: [], chatCount: 0, giftCount: 0 
        });
        connectStream(acc.username);
        processedCount++;
        io.emit('importProgress', { current: processedCount, total: totalBatch, username: acc.username });

        const randomDelay = Math.floor(Math.random() * 2000) + 2000;
        await new Promise(resolve => setTimeout(resolve, randomDelay));
        if (processedCount % 10 === 0 && importQueue.length > 0) {
            await new Promise(resolve => setTimeout(resolve, 5000));
        }
    }
    isProcessingQueue = false;
    io.emit('importComplete');
}

function handleOffline(username) {
    if (monitoredAccounts[username]) {
        monitoredAccounts[username].status = 'offline';
        monitoredAccounts[username].info.viewers = 0;
        io.emit('streamStatusChanged', { username, status: 'offline', info: monitoredAccounts[username].info });
        
        if (monitoredAccounts[username].connection) {
            try { 
                monitoredAccounts[username].connection.removeAllListeners();
                monitoredAccounts[username].connection.disconnect(); 
            } catch (e) {}
            monitoredAccounts[username].connection = null;
        }
    }
}

function connectStream(username) {
    if (!monitoredAccounts[username]) return;

    if (monitoredAccounts[username].connection) {
        try { 
            monitoredAccounts[username].connection.removeAllListeners();
            monitoredAccounts[username].connection.disconnect(); 
        } catch (e) {}
        monitoredAccounts[username].connection = null;
    }

    const conn = new WebcastPushConnection(username, {
        processInitialData: false, 
        enableExtendedGiftInfo: true,
        enableWebsocketUpgrade: true,
        requestPollingIntervalMs: 2000
    });
    monitoredAccounts[username].connection = conn;

    // SIMPAN & KIRIM DATA CHAT
    conn.on('chat', data => {
        if (monitoredAccounts[username]) {
            monitoredAccounts[username].lastPing = Date.now();
            if (monitoredAccounts[username].status === 'live') {
                monitoredAccounts[username].chatCount++;
                const logEntry = { time: new Date().toLocaleTimeString('id-ID'), type: 'Chat', user: data.uniqueId, detail: data.comment };
                monitoredAccounts[username].logs.push(logEntry);
                if (monitoredAccounts[username].logs.length > 200) monitoredAccounts[username].logs.shift(); // Limit memori 200 log per akun
                
                io.emit('streamChat', { username, log: logEntry, chatCount: monitoredAccounts[username].chatCount });
            }
        }
    });

    // SIMPAN & KIRIM DATA GIFT
    conn.on('gift', data => {
        if (monitoredAccounts[username]) {
            monitoredAccounts[username].lastPing = Date.now();
            if (data.giftType === 1 && !data.repeatEnd) return; 
            if (monitoredAccounts[username].status === 'live') {
                monitoredAccounts[username].giftCount++;
                const count = data.repeatCount || 1;
                const diamonds = data.diamondCount * count;
                const detailMsg = `${data.giftName} x${count} (${diamonds} D)`;
                
                const logEntry = { time: new Date().toLocaleTimeString('id-ID'), type: 'Gift', user: data.uniqueId, giftName: data.giftName, count: count, diamonds: diamonds, detail: detailMsg };
                monitoredAccounts[username].logs.push(logEntry);
                if (monitoredAccounts[username].logs.length > 200) monitoredAccounts[username].logs.shift(); // Limit memori

                io.emit('streamGift', { username, log: logEntry, giftCount: monitoredAccounts[username].giftCount });
            }
        }
    });

    conn.on('roomUser', data => {
        if (monitoredAccounts[username]) {
            monitoredAccounts[username].lastPing = Date.now();
            if (monitoredAccounts[username].status === 'live') {
                monitoredAccounts[username].info.viewers = data.viewerCount;
                io.emit('streamUpdate', { username, viewers: data.viewerCount });
            }
        }
    });

    conn.on('like', data => {
        if (monitoredAccounts[username]) {
            monitoredAccounts[username].lastPing = Date.now();
            if (monitoredAccounts[username].status === 'live') {
                monitoredAccounts[username].info.likes += data.likeCount;
                io.emit('streamUpdate', { username, likes: monitoredAccounts[username].info.likes });
            }
        }
    });

    conn.on('streamEnd', () => handleOffline(username));
    conn.on('disconnected', () => handleOffline(username));
    conn.on('error', err => {});

    conn.connect().then(state => {
        if (!monitoredAccounts[username]) return; 
        monitoredAccounts[username].lastPing = Date.now();

        let isLive = true;
        if (state && state.roomInfo && state.roomInfo.status === 4) isLive = false;

        if (!isLive) { handleOffline(username); return; }
        
        monitoredAccounts[username].status = 'live';
        monitoredAccounts[username].info = {
            startTime: Date.now(),
            viewers: state.viewerCount || 0,
            likes: monitoredAccounts[username].info.likes || 0,
            label1: monitoredAccounts[username].info.label1,
            label2: monitoredAccounts[username].info.label2
        };
        io.emit('streamStatusChanged', { username, status: 'live', info: monitoredAccounts[username].info });
        if (state.viewerCount) io.emit('streamUpdate', { username, viewers: state.viewerCount });
    }).catch(err => handleOffline(username));
}

async function checkOfflineAccounts() {
    if (isProcessingQueue) return; 
    const offlineUsers = Object.keys(monitoredAccounts).filter(u => monitoredAccounts[u].status === 'offline');
    for (const user of offlineUsers) {
        if (monitoredAccounts[user]) {
            monitoredAccounts[user].status = 'checking';
            io.emit('streamStatusChanged', { username: user, status: 'checking' });
            connectStream(user);
            await new Promise(resolve => setTimeout(resolve, 2000));
        }
    }
}
setInterval(checkOfflineAccounts, 10000); 

setInterval(() => {
    if (isProcessingQueue) return;
    const now = Date.now();
    const onlineUsers = Object.keys(monitoredAccounts).filter(u => monitoredAccounts[u].status === 'live');
    for (const user of onlineUsers) {
        if (monitoredAccounts[user] && monitoredAccounts[user].lastPing) {
            if (now - monitoredAccounts[user].lastPing > 60000) {
                handleOffline(user);
            }
        }
    }
}, 20000); 

setInterval(() => {
    const memoryUsage = process.memoryUsage().rss / 1024 / 1024;
    io.emit('serverStats', { memory: memoryUsage.toFixed(2) });
}, 5000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server Dashboard Berjalan di http://localhost:${PORT}`);
});