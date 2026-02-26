const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { WebcastPushConnection } = require('tiktok-live-connector');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static(path.join(__dirname, 'public')));

const monitoredAccounts = {};

// Sistem Antrean Import Anti-Blokir
let importQueue = [];
let isProcessingQueue = false;

io.on('connection', (socket) => {
    console.log('Client Dashboard terhubung');

    const initData = Object.keys(monitoredAccounts).map(user => ({
        username: user,
        status: monitoredAccounts[user].status,
        info: monitoredAccounts[user].info
    }));
    socket.emit('initStreams', initData);

    // 1. TAMBAH MONITORING SATUAN
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
            connection: null,
            lastPing: Date.now() // Set detak jantung awal
        };

        io.emit('streamAdded', { username, status: 'checking', info: monitoredAccounts[username].info });
        connectStream(username);
    });

    // 2. TAMBAH MONITORING BULK (QUEUE SYSTEM)
    socket.on('addBulkStreams', (accounts) => {
        if (!Array.isArray(accounts)) return;
        
        let validAccounts = [];
        for (const acc of accounts) {
            let rawUsername = typeof acc === 'string' ? acc : acc.username;
            const cleanUsername = rawUsername.replace(/[@\s]/g, '').toLowerCase();
            const label1 = acc.label1 || 'Tanpa Label 1';
            const label2 = acc.label2 || 'Tanpa Label 2';

            if (cleanUsername && !monitoredAccounts[cleanUsername] && !importQueue.some(q => q.username === cleanUsername)) {
                validAccounts.push({ username: cleanUsername, label1, label2 });
            }
        }

        if (validAccounts.length > 0) {
            importQueue.push(...validAccounts);
            socket.emit('systemMsg', { type: 'success', msg: `${validAccounts.length} akun dimasukkan ke antrean import aman.` });
            
            if (!isProcessingQueue) {
                processImportQueue();
            }
        } else {
            socket.emit('systemMsg', { type: 'error', msg: 'Semua akun dalam file sudah ada di dashboard.' });
        }
    });

    socket.on('removeStream', (username) => {
        if (monitoredAccounts[username]) {
            if (monitoredAccounts[username].connection) {
                try { monitoredAccounts[username].connection.disconnect(); } catch (e) {}
            }
            delete monitoredAccounts[username];
            io.emit('streamRemoved', { username });
            console.log(`Monitoring dihapus: @${username}`);
        }
    });
});

// Proses Antrean (Algoritma Keamanan IP)
async function processImportQueue() {
    isProcessingQueue = true;
    let processedCount = 0;
    const totalBatch = importQueue.length;

    while (importQueue.length > 0) {
        const acc = importQueue.shift();
        
        monitoredAccounts[acc.username] = {
            status: 'checking',
            info: { startTime: null, viewers: 0, likes: 0, label1: acc.label1, label2: acc.label2 },
            connection: null,
            lastPing: Date.now() // Detak jantung
        };
        
        io.emit('streamAdded', { username: acc.username, status: 'checking', info: monitoredAccounts[acc.username].info });
        connectStream(acc.username);

        processedCount++;
        
        io.emit('importProgress', { 
            current: processedCount, 
            total: totalBatch, 
            username: acc.username,
            remaining: importQueue.length
        });

        const randomDelay = Math.floor(Math.random() * 2000) + 2000;
        await new Promise(resolve => setTimeout(resolve, randomDelay));

        if (processedCount % 10 === 0 && importQueue.length > 0) {
            await new Promise(resolve => setTimeout(resolve, 5000));
        }
    }

    isProcessingQueue = false;
    io.emit('systemMsg', { type: 'success', msg: 'Seluruh antrean import telah selesai diproses dengan aman!' });
    io.emit('importComplete');
}

function handleOffline(username) {
    if (monitoredAccounts[username]) {
        monitoredAccounts[username].status = 'offline';
        monitoredAccounts[username].info.viewers = 0;
        io.emit('streamStatusChanged', { username, status: 'offline', info: monitoredAccounts[username].info });
        
        // Pastikan memori koneksi lama dibersihkan secara total
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

    // Bersihkan sisa koneksi lama sebelum membuat yang baru agar bisa mendapat Room ID fresh
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

    // UPDATE PING SETIAP ADA AKTIVITAS APAPUN (Heartbeat System)
    conn.on('chat', data => {
        if (monitoredAccounts[username]) monitoredAccounts[username].lastPing = Date.now();
        if (monitoredAccounts[username] && monitoredAccounts[username].status === 'live') {
            io.emit('streamChat', { username, user: data.uniqueId, msg: data.comment });
        }
    });

    conn.on('gift', data => {
        if (monitoredAccounts[username]) monitoredAccounts[username].lastPing = Date.now();
        if (data.giftType === 1 && !data.repeatEnd) return; 
        if (monitoredAccounts[username] && monitoredAccounts[username].status === 'live') {
            io.emit('streamGift', { username, user: data.uniqueId, giftName: data.giftName, count: data.repeatCount || 1, diamonds: data.diamondCount * (data.repeatCount || 1) });
        }
    });

    conn.on('roomUser', data => {
        if (monitoredAccounts[username]) monitoredAccounts[username].lastPing = Date.now();
        if (monitoredAccounts[username] && monitoredAccounts[username].status === 'live') {
            monitoredAccounts[username].info.viewers = data.viewerCount;
            io.emit('streamUpdate', { username, viewers: data.viewerCount });
        }
    });

    conn.on('like', data => {
        if (monitoredAccounts[username]) monitoredAccounts[username].lastPing = Date.now();
        if (monitoredAccounts[username] && monitoredAccounts[username].status === 'live') {
            monitoredAccounts[username].info.likes += data.likeCount;
            io.emit('streamUpdate', { username, likes: monitoredAccounts[username].info.likes });
        }
    });

    conn.on('streamEnd', () => {
        console.log(`[STREAM END] @${username} mengakhiri Live.`);
        handleOffline(username);
        try { conn.disconnect(); } catch(e){} 
    });
    
    conn.on('disconnected', () => {
        handleOffline(username);
    });
    
    conn.on('error', err => {
        // Silent catch error minor
    });

    conn.connect().then(state => {
        if (!monitoredAccounts[username]) return; 
        
        monitoredAccounts[username].lastPing = Date.now(); // Koneksi sukses = detak jantung aktif

        let isLive = true;
        if (state && state.roomInfo && state.roomInfo.status === 4) {
            isLive = false;
        }

        if (!isLive) {
            handleOffline(username);
            return;
        }
        
        console.log(`[ONLINE] @${username} Valid dan sedang Live!`);
        monitoredAccounts[username].status = 'live';
        monitoredAccounts[username].info = {
            startTime: Date.now(),
            viewers: state.viewerCount || 0,
            likes: monitoredAccounts[username].info.likes || 0,
            label1: monitoredAccounts[username].info.label1,
            label2: monitoredAccounts[username].info.label2
        };
        
        io.emit('streamStatusChanged', { username, status: 'live', info: monitoredAccounts[username].info });
        
        if (state.viewerCount) {
            io.emit('streamUpdate', { username, viewers: state.viewerCount });
        }

    }).catch(err => {
        handleOffline(username);
    });
}

// ------------------------------------------------------------------
// 1. RECONNECT AKUN OFFLINE (Berjalan setiap 10 Detik)
// ------------------------------------------------------------------
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

// ------------------------------------------------------------------
// 2. CROSSCHECK AKUN ONLINE NYANGKUT (Berjalan Setiap 20 Detik)
// ------------------------------------------------------------------
setInterval(() => {
    if (isProcessingQueue) return;

    const now = Date.now();
    const onlineUsers = Object.keys(monitoredAccounts).filter(u => monitoredAccounts[u].status === 'live');
    
    for (const user of onlineUsers) {
        // Jika akun "Live" tapi sudah TIDAK mengirim detak data selama lebih dari 60 detik (Dinaikkan agar Live sepi tidak terputus)
        if (monitoredAccounts[user] && monitoredAccounts[user].lastPing) {
            if (now - monitoredAccounts[user].lastPing > 60000) {
                console.log(`[CROSSCHECK DETECTED] @${user} nyangkut/mati tanpa kabar. Force Offline!`);
                handleOffline(user);
            }
        }
    }
}, 20000); // Mengevaluasi setiap 20 Detik

// PENGIRIMAN DATA MEMORY RAM KE DASHBOARD (Jalan tiap 5 detik)
setInterval(() => {
    const memoryUsage = process.memoryUsage().rss / 1024 / 1024;
    io.emit('serverStats', { memory: memoryUsage.toFixed(2) });
}, 5000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`===========================================`);
    console.log(`Server Dashboard Berjalan di http://localhost:${PORT}`);
    console.log(`===========================================`);
});