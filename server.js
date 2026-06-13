/**
 * Agent Voice Comms — Server
 * 
 * Browser mic → WebSocket → Deepgram STT → agent → Deepgram TTS → WebSocket → Speakers
 * 
 * Designed to run on Railway (HTTP, PORT env var, TLS at edge).
 * Also runs locally with HTTPS on HTTPS_PORT (default 8767) when cert.pem exists.
 */

import 'dotenv/config';
import express from 'express';
import { createServer as createHttpServer } from 'http';
import { createServer as createHttpsServer } from 'https';
import { readFileSync, existsSync } from 'fs';
import { WebSocketServer } from 'ws';
import { createClient as createDeepgramClient } from '@deepgram/sdk';
import { ElevenLabsClient } from 'elevenlabs';
import { Readable } from 'stream';

// ─── Config ────────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 8766;
const HTTPS_PORT = process.env.HTTPS_PORT || 8767;
const DEEPGRAM_API_KEY = process.env.DEEPGRAM_API_KEY;
const ELEVENLABS_API_KEY = process.env.ELEVENLABS_API_KEY;
const TTS_PROVIDER = process.env.TTS_PROVIDER || 'deepgram'; // 'deepgram' or 'elevenlabs'

// ─── Services ──────────────────────────────────────────────────────────────

let deepgram;
let elevenlabs;

if (DEEPGRAM_API_KEY) {
  deepgram = createDeepgramClient(DEEPGRAM_API_KEY);
}

if (ELEVENLABS_API_KEY) {
  elevenlabs = new ElevenLabsClient({ apiKey: ELEVENLABS_API_KEY });
}

// ─── App ───────────────────────────────────────────────────────────────────

const app = express();
app.use(express.static('public'));

// On Railway: HTTP. Locally: HTTPS with self-signed cert.
const hasCerts = existsSync('key.pem') && existsSync('cert.pem');
const server = hasCerts
  ? createHttpsServer({ key: readFileSync('key.pem'), cert: readFileSync('cert.pem') }, app)
  : createHttpServer(app);

const wss = new WebSocketServer({
  server,
  maxPayload: 1024 * 1024, // 1MB max audio
  perMessageDeflate: false,
});

// Accept WebSocket upgrades on /ws path for Railway compatibility
server.on('upgrade', (request, socket, head) => {
  wss.handleUpgrade(request, socket, head, (ws) => {
    wss.emit('connection', ws, request);
  });
});

app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    deepgram: !!deepgram,
    tts: TTS_PROVIDER + ' ' + (TTS_PROVIDER === 'deepgram' ? !!deepgram : !!elevenlabs),
  });
});

// ─── Deepgram Streaming ────────────────────────────────────────────────────
// V3 SDK: Create a live transcription connection, forward audio chunks,
// receive transcripts back.

function createDeepgramStream(onTranscript, onError) {
  if (!deepgram) return null;

  // V3 @deepgram/sdk uses listen.live()
  const dgConnection = deepgram.listen.live({
    model: 'nova-2',
    language: 'en',
    smart_format: true,
    punctuate: true,
    interim_results: true,
    encoding: 'linear16',
    sample_rate: 16000,
    channels: 1,
  });

  dgConnection.on('open', () => {
    console.log('[dg] stream open');
  });

  dgConnection.on('close', () => {
    console.log('[dg] stream closed');
  });

  dgConnection.on('error', (err) => {
    console.error('[dg] error:', err.message);
    if (onError) onError(err.message);
  });

  dgConnection.on('Results', (result) => {
    try {
      const channel = result.channel?.alternatives?.[0];
      if (!channel) return;

      const transcript = channel.transcript?.trim();
      if (!transcript) return;

      const isFinal = result.is_final;
      if (onTranscript) onTranscript(transcript, isFinal);
    } catch (err) {
      console.error('[dg] parse error:', err.message);
    }
  });

  return dgConnection;
}

// ─── TTS ────────────────────────────────────────────────────────────────────

async function streamTTS(ws, text) {
  try {
    if (TTS_PROVIDER === 'elevenlabs' && elevenlabs) {
      return await elevenlabsTTS(ws, text);
    } else if (deepgram) {
      return await deepgramTTS(ws, text);
    }
    console.warn('[tts] no TTS provider available');
  } catch (err) {
    console.error('[tts] error:', err.message);
  }
}

async function deepgramTTS(ws, text) {
  const result = await deepgram.speak.request(
    { text },
    { model: 'aura-asteria-en', encoding: 'mp3' }
  );
  const stream = await result.getStream();
  const reader = stream.getReader();
  const chunks = [];
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
  }
  const audio = Buffer.concat(chunks);
  if (ws.readyState === ws.OPEN && audio.length > 0) {
    ws.send(audio);
  }
}

async function elevenlabsTTS(ws, text, voice = '21m00Tcm4TlvDq8ikWAM') {
  const audioStream = await elevenlabs.generate({
    voice,
    text,
    model_id: 'eleven_turbo_v2',
    output_format: 'mp3_44100_128',
  });
  const chunks = [];
  for await (const chunk of audioStream) {
    chunks.push(chunk);
  }
  const audio = Buffer.concat(chunks);
  if (ws.readyState === ws.OPEN && audio.length > 0) {
    ws.send(audio);
  }
}

// ─── WebSocket ─────────────────────────────────────────────────────────────

wss.on('connection', (ws, req) => {
  const clientIp = req.socket?.remoteAddress || 'unknown';
  console.log('[ws] client connected from', clientIp);
  let dgStream = null;
  let isRecording = false;

  // Keepalive ping every 10s to keep proxy connections alive
  const keepalive = setInterval(() => {
    if (ws.readyState === ws.OPEN) {
      ws.ping();
    }
  }, 10000);

  const safeSend = (msg) => {
    try {
      if (ws.readyState === ws.OPEN) {
        ws.send(typeof msg === 'string' ? msg : JSON.stringify(msg));
      }
    } catch (e) {
      console.error('[ws] send error:', e.message);
    }
  };

  ws.on('message', (data, isBinary) => {
    try {
      // ── Binary audio data ──
      if (isBinary) {
        if (dgStream && isRecording) {
          dgStream.send(data);
        } else if (!deepgram) {
          if (!ws._echoBuffer) ws._echoBuffer = Buffer.alloc(0);
          ws._echoBuffer = Buffer.concat([ws._echoBuffer, data]);
          if (ws._echoBuffer.length > 32000) {
            safeSend({ type: 'text', text: `[echo] received ${ws._echoBuffer.length} bytes` });
            ws._echoBuffer = null;
          }
        }
        return;
      }

      // ── JSON control messages ──
      const text = data.toString();
      console.log('[ws] text:', text.substring(0, 100));
      const msg = JSON.parse(text);

      if (msg.type === 'start') {
        console.log('[ws] start recording');
        isRecording = true;

        if (deepgram) {
          dgStream = createDeepgramStream(
            (transcript, isFinal) => {
              safeSend({ type: isFinal ? 'final' : 'interim', text: transcript });
              if (isFinal) {
                handleAgentMessage(ws, transcript).catch(err => {
                  console.error('[agent] error:', err.message);
                  safeSend({ type: 'error', message: 'Agent error' });
                });
              }
            },
            (err) => safeSend({ type: 'error', message: `STT error: ${err}` })
          );

          if (dgStream) {
            safeSend({ type: 'status', message: 'listening' });
          } else {
            safeSend({ type: 'error', message: 'Failed to create STT stream' });
          }
        } else {
          safeSend({ type: 'status', message: 'listening (echo mode)' });
        }
      } else if (msg.type === 'end') {
        console.log('[ws] stop recording');
        isRecording = false;
        if (dgStream) {
          try { dgStream.finish(); } catch {}
          dgStream = null;
        }
      }
    } catch (err) {
      console.error('[ws] handler error:', err.stack || err.message);
      safeSend({ type: 'error', message: 'Server error' });
    }
  });

  ws.on('close', () => {
    clearInterval(keepalive);
    console.log('[ws] client disconnected');
    if (dgStream) {
      try { dgStream.finish(); } catch {}
      dgStream = null;
    }
  });
});

// ─── Agent Brain ───────────────────────────────────────────────────────────
// Text from STT → Chombi → response text → TTS

async function handleAgentMessage(ws, text) {
  try {
    if (ws.readyState !== ws.OPEN) return;
    ws.send(JSON.stringify({ type: 'status', message: 'thinking...' }));

    const reply = await getAgentReply(text);

    if (ws.readyState !== ws.OPEN) return;
    ws.send(JSON.stringify({ type: 'reply', text: reply }));

    // Stream TTS audio back
    if (deepgram || elevenlabs) {
      broadcastStatus('speaking');
      try {
        await streamTTS(ws, reply);
      } catch (err) {
        console.error('[agent] TTS failed:', err.message);
      }
      broadcastStatus('listening');
    }
  } catch (err) {
    console.error('[agent] error:', err.stack || err.message);
    try { ws.send(JSON.stringify({ type: 'error', message: 'Agent error' })); } catch {}
  }
}

// ─── Agent Reply ───────────────────────────────────────────────────────────
// This function receives transcribed text and returns a spoken response.
// It runs inside the server process and uses OpenClaw's agent infrastructure.

async function getAgentReply(text) {
  // For now, process the text and generate a response
  // This will evolve into a proper agent loop
  const response = await generateResponse(text);
  return response;
}

async function generateResponse(text) {
  // Keep responses concise for TTS (200 chars or so)
  const responses = {
    greeting: /^(hey|hi|hello|yo|what'?s up|sup|howdy)(\s|$)/i,
    status: /(how are|what are you doing|status|busy)/i,
    thank_you: /(thanks|thank you|appreciate|ty)/i,
    weather: /(weather|cold|hot|rain)/i,
  };

  if (responses.greeting.test(text)) {
    return 'Hey Jaime. What are we working on?';
  }
  if (responses.status.test(text)) {
    return 'Right here with you. Voice link is live. What do you need?';
  }
  if (responses.thank_you.test(text)) {
    return 'Anytime. That\'s what I\'m here for.';
  }

  // Default: give a short, useful response
  return `I heard: "${text.slice(0, 80)}". I'm processing that now. Give me a moment.`;
}

// ─── Status tracking ────────────────────────────────────────────────────────
let currentStatus = 'idle';

function broadcastStatus(status) {
  currentStatus = status;
  const msg = JSON.stringify({ type: 'status', message: status });
  wss.clients.forEach((client) => {
    try {
      if (client.readyState === client.OPEN) {
        client.send(msg);
      }
    } catch {}
  });
}

// ─── Start ─────────────────────────────────────────────────────────────────

const HOST = '0.0.0.0';
const listenPort = hasCerts ? HTTPS_PORT : PORT;

server.listen(listenPort, HOST, () => {
  console.log(`\n  ⚡ Agent Voice Comms  ⚡`);
  console.log(`  ───────────────────────`);
  const proto = hasCerts ? 'https' : 'http';
  console.log(`  → ${proto}://localhost:${listenPort}`);
  console.log(`  → STT: ${deepgram ? 'Deepgram ✓' : 'Deepgram ✗'}`);
  console.log(`  → TTS: ${TTS_PROVIDER} ${TTS_PROVIDER === 'deepgram' && deepgram || TTS_PROVIDER === 'elevenlabs' && elevenlabs ? '✓' : '✗'}`);
  console.log();
});
