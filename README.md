# ZakShoot Duel — online relay server

One-file, zero-dependency WebSocket relay for the ZAKSHOOT DUEL online quick-match mode.
Routes only tiny NDJSON game messages between paired players. No secrets, no database.

## Deploy (Render, free)

[![Deploy to Render](https://render.com/images/deploy-button.svg)](https://render.com/deploy?repo=https://github.com/ZiryaNoov/zakshoot-duel-server)

1. Open the link above (or render.com → New → Blueprint → pick this repo)
2. Log in with GitHub → **Apply**
3. Service goes live at `https://zakshoot-duel-server.onrender.com` (~2 min)

Free tier sleeps after ~15 min idle and wakes in ~30-60s on the next connection
(the game auto-retries and shows "waking server").

## Run anywhere else

`node server.js [port]` (uses $PORT on hosts like Render/Railway/Fly).
