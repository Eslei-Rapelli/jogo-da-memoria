const express=require('express'),http=require('http'),{WebSocketServer}=require('ws');
const app=express();app.use(express.static('public'));
const srv=http.createServer(app),wss=new WebSocketServer({server:srv,maxPayload:50000});
const rooms=new Map(); // codigo -> {state, subs, t}
function room(c){let r=rooms.get(c);if(!r){r={state:null,subs:new Set(),t:Date.now()};rooms.set(c,r)}r.t=Date.now();return r}
wss.on('connection',ws=>{ws.alive=true;ws.on('pong',()=>ws.alive=true);ws.rooms=new Set();
 ws.on('message',d=>{let m;try{m=JSON.parse(d)}catch{return}
  const c=String(m.code||'').toUpperCase();if(!/^[A-Z0-9]{4}$/.test(c))return;
  if(m.t==='get'){const r=rooms.get(c);ws.send(JSON.stringify({t:'got',code:c,state:r?r.state:null}))}
  else if(m.t==='sub'){const r=room(c);r.subs.add(ws);ws.rooms.add(c);if(r.state)ws.send(JSON.stringify({t:'state',code:c,state:r.state}))}
  else if(m.t==='set'&&m.state&&typeof m.state==='object'){const r=room(c);r.state=m.state;const out=JSON.stringify({t:'state',code:c,state:m.state});r.subs.forEach(w=>w.readyState===1&&w.send(out))}});
 ws.on('close',()=>ws.rooms.forEach(c=>{const r=rooms.get(c);if(r)r.subs.delete(ws)}))});
setInterval(()=>{wss.clients.forEach(w=>{if(!w.alive)return w.terminate();w.alive=false;w.ping()});
 const n=Date.now();rooms.forEach((r,c)=>{if(!r.subs.size&&n-r.t>6*3600e3)rooms.delete(c)})},30000);
srv.listen(process.env.PORT||3000,()=>console.log('Servidor no ar'));
