import net from 'net';
import cbor from 'cbor';
import { Buffer } from 'buffer';
import { Readable } from 'stream';
class StreamItemClass {
    constructor(part, proto, id, version, dev, stream) {
        this.part = part;
        this.proto = proto;
        this.id = id;
        this.version = version;
        this.dev = dev;
        this.stream = stream;
    }
}
async function handleSession(item, socket) {
    console.info(`Session started for device ${item.dev}`);
    const response = new StreamItemClass("session", "raw", null, null, null, null);
    const encoded = cbor.encode(response);
    await socket.write(encoded);
}
async function processLogMessage(msg) {
    const timestamp = new Date(msg.ts * 1000).toISOString();
    console.info(`[${timestamp}][${msg.level}] ${msg.msg}`);
}
async function processTemperatureMessage(msg) {
    const timestamp = new Date(msg.ts * 1000).toISOString();
    console.info(`[${timestamp}][Temperature] ` +
        `Left: ${msg.left / 100}°C, ` +
        `Right: ${msg.right / 100}°C, ` +
        `Ambient: ${msg.amb / 100}°C, ` +
        `Heat Sink: ${msg.hs / 100}°C`);
}
async function processCapSenseMessage(msg) {
    const timestamp = new Date(msg.ts * 1000).toISOString();
    console.info(`[${timestamp}][CapSense] ` +
        `Left: {out: ${msg.left.out}, cen: ${msg.left.cen}, in: ${msg.left.in}, status: ${msg.left.status}} - ` +
        `Right: {out: ${msg.right.out}, cen: ${msg.right.cen}, in: ${msg.right.in}, status: ${msg.right.status}}`);
}
async function processPiezoDualMessage(msg) {
    const timestamp = new Date(msg.ts * 1000).toISOString();
    console.info(`[${timestamp}][PiezoDual] ` +
        `Frequency: ${msg.freq} Hz, ` +
        `ADC: ${msg.adc}, ` +
        `Gain: ${msg.gain}, ` +
        `Left1: ${msg.left1.length} bytes, ` +
        `Left2: ${msg.left2.length} bytes, ` +
        `Right1: ${msg.right1.length} bytes, ` +
        `Right2: ${msg.right2.length} bytes`);
}
async function processBedTempMessage(msg) {
    const timestamp = new Date(msg.ts * 1000).toISOString();
    console.info(`[${timestamp}][BedTemp] ` +
        `Ambient: ${msg.amb / 100}°C, ` +
        `MCU: ${msg.mcu / 100}°C, ` +
        `HU: ${msg.hu / 100}%, ` +
        `Left: ${msg.left.out / 100}°C, ` +
        `Right: ${msg.right.out / 100}°C`);
}
async function processMessage(msg) {
    if (!msg || typeof msg !== 'object' || !('type' in msg)) {
        console.warn('Invalid message format:', msg);
        return;
    }
    switch (msg.type) {
        case 'log':
            await processLogMessage(msg);
            break;
        case 'frzTemp':
            await processTemperatureMessage(msg);
            break;
        case 'capSense':
            await processCapSenseMessage(msg);
            break;
        case 'piezo-dual':
            await processPiezoDualMessage(msg);
            break;
        case 'bedTemp':
            await processBedTempMessage(msg);
            break;
        default:
            console.debug('Unknown message type:', msg.type, msg);
    }
}
async function handleBatchItem(batchItem) {
    try {
        // First decode the BatchItem's data field
        const messages = await cbor.decodeAll(batchItem.data);
        // Process each message in the batch item
        for (const msg of messages) {
            await processMessage(msg);
        }
    }
    catch (error) {
        console.warn("Failed to process batch item data. Sequence:", batchItem.seq, "Error:", error, "Raw data:", batchItem.data.toString('hex'));
    }
}
async function handleBatch(item, socket) {
    if (!item.id) {
        console.warn("no id was present for batch");
        return;
    }
    console.info(`Received batch ${item.id}`);
    const response = new StreamItemClass("batch", "raw", item.id, null, null, null);
    const encoded = cbor.encode(response);
    await socket.write(encoded);
    if (!item.stream) {
        console.warn("no stream in batch");
        return;
    }
    try {
        const streamBuffer = Buffer.from(item.stream);
        const reader = Readable.from(streamBuffer);
        const decoder = new cbor.Decoder();
        decoder.on('data', async (batchItem) => {
            await handleBatchItem(batchItem);
        });
        decoder.on('error', (error) => {
            if (error.message === 'unexpected end of input') {
                console.debug('Reached end of CBOR stream');
                // this appears to be normal.
            }
            else {
                console.warn('Decoder error:', error);
            }
        });
        reader.on('error', (error) => {
            console.warn('Reader error:', error);
        });
        reader.on('end', () => {
            console.debug('Reader finished');
        });
        reader.pipe(decoder);
    }
    catch (error) {
        console.warn("Failed to process batch:", error);
    }
}
async function handleDataStream(socket) {
    console.info("Incoming TCP connection");
    socket.setTimeout(60000);
    const decoder = new cbor.Decoder();
    decoder.on('data', async (item) => {
        switch (item.part) {
            case "session":
                await handleSession(item, socket);
                break;
            case "batch":
                await handleBatch(item, socket);
                break;
            default:
                console.warn("Unrecognized part:", item.part);
        }
    });
    decoder.on('error', (error) => {
        console.warn('Decoder error:', error);
    });
    socket.pipe(decoder);
    socket.on('error', (error) => {
        console.error('Socket error:', error);
        socket.destroy();
    });
    socket.on('timeout', () => {
        console.warn('Socket timeout');
        socket.destroy();
    });
}
export function startMetricsServer(options = {}) {
    const { port = 1337, host = '0.0.0.0' } = options;
    return new Promise((resolve, reject) => {
        const server = net.createServer((socket) => {
            handleDataStream(socket);
        });
        server.on('error', (error) => {
            reject(error);
        });
        server.listen(port, host, () => {
            console.log(`Metrics server listening on ${host}:${port}`);
            resolve(server);
        });
    });
}
