# Agent Voice Comms ⚡

Real-time voice interface for Volt Labs agents. Mic → you/Chombi → speakers.

## Quick Start

```bash
# Install deps
npm install

# Set your API keys
cp .env.example .env
# Edit .env with your Deepgram + ElevenLabs keys

# Start
npm start

# Open
open http://localhost:8766
```

## Architecture

```
Browser mic → WebSocket → Deepgram STT → Chombi → ElevenLabs TTS → WebSocket → Speakers
                          (streaming)             (agent brain)   (streaming)
```

## Controls

- Click 🎤 or press **Space** to toggle mic
- Speak naturally
- Agent responds through speakers

## Status

🟢 MVP — June 12, 2026
