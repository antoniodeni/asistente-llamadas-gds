/**
 * Asistente de llamadas de Gestión del Sur
 * ----------------------------------------
 * Cuando Juanjo no coge el 621, la llamada se desvía al número virtual (Telnyx), que la manda a OpenAI.
 * OpenAI avisa a este programa ("realtime.call.incoming"), el programa la acepta con las instrucciones y la voz,
 * escucha la conversación por un canal aparte (WebSocket), recoge los datos que el asistente guarda,
 * cuelga después de la despedida y manda los datos a n8n (hoja + Telegram).
 *
 * Variables (se ponen en Easypanel, NUNCA en el código):
 *   OPENAI_API_KEY         clave del proyecto "Gestión del Sur - llamadas"
 *   OPENAI_WEBHOOK_SECRET  "whsec_..." del webhook creado en OpenAI
 *   N8N_WEBHOOK_URL        https://engranaje-sur-n8n.ul39e5.easypanel.host/webhook/gds-llamadas
 *   N8N_TOKEN              token compartido con el flujo de n8n (cabecera x-gds-token)
 *   MODEL                  (opcional) por defecto gpt-realtime-2.1-mini
 *   VOICE                  (opcional) por defecto marin
 *   MAX_SECONDS            (opcional) duración máxima de una llamada, por defecto 240
 *   PORT                   (opcional) por defecto 3000
 */
'use strict';

const http = require('http');
const crypto = require('crypto');
const WebSocket = require('ws');

const CFG = {
  apiKey: process.env.OPENAI_API_KEY || '',
  webhookSecret: process.env.OPENAI_WEBHOOK_SECRET || '',
  n8nUrl: process.env.N8N_WEBHOOK_URL || '',
  n8nToken: process.env.N8N_TOKEN || '',
  model: process.env.MODEL || 'gpt-realtime-2.1-mini',
  voice: process.env.VOICE || 'marin',
  maxSeconds: Number(process.env.MAX_SECONDS) || 240,
  port: Number(process.env.PORT) || 3000
};

const OPENAI = 'https://api.openai.com/v1';

/* ------------------------------------------------------------------ */
/* Instrucciones del asistente                                          */
/* ------------------------------------------------------------------ */
const DESPEDIDA = 'Estupendo, en breve un compañero se pondrá en contacto con usted. Un saludo y gracias por contactar.';

const INSTRUCCIONES = `
Eres la recepcionista virtual de Gestión del Sur, una inmobiliaria y financiera de Lepe (Huelva).
Atiendes las llamadas que el equipo no puede coger.

CÓMO HABLAS:
- Español de España, con acento peninsular natural. Nunca acento extranjero (ni inglés, ni ruso, ni latinoamericano).
- Cercana y agradable, como una buena recepcionista. Usa expresiones como "Perfecto", "Muy bien", "Claro que sí" y agradece lo que te cuentan.
- Frases cortas y naturales, sin sonar seca ni robótica.
- Haz UNA sola pregunta cada vez y espera a que la persona termine de hablar.
- Si la persona habla mientras tú hablas, cállate y escúchala.
- Si no entiendes algo, pide que lo repita: "Perdone, no le he entendido bien, ¿me lo repite?". Nunca te inventes lo que ha dicho.
- Si el nombre de un pueblo o zona no te suena, pide que lo repita.
- Trata siempre de usted.

SALUDO (solo una vez, al principio), di exactamente:
"Hola, soy el asistente virtual de Gestión del Sur, dígame su nombre y en qué puedo ayudarle."

QUÉ TIENES QUE APUNTAR (si ya lo ha dicho, no lo vuelvas a preguntar):
1. Su nombre.
2. Qué necesita: comprar, vender, alquilar (busca), alquilar su vivienda (propietario), hipoteca o préstamo, u otra cosa.
3. Haz SIEMPRE una o dos preguntas más según el caso, para que el compañero sepa de qué va:
   - Comprar: qué busca (piso, casa...), en qué zona y presupuesto aproximado.
   - Vender o alquilar la suya: qué tipo de vivienda es (piso, casa, terreno...) y dónde está.
   - Buscar alquiler: qué busca, dónde y cuánto puede pagar al mes.
   - Hipoteca o préstamo: si ya tiene la vivienda vista o la está buscando.
   - Otra cosa: que le cuente brevemente qué necesita.
4. Si le llamamos a este mismo número o prefiere otro.
Si dice que es urgente, apúntalo, pero no lo preguntes.

CÓMO TERMINAS (en este orden, sin saltarte ningún paso):
1. Confirma en una sola frase lo que has apuntado y espera su respuesta. Por ejemplo:
   "Muy bien, Antonio. Entonces quiere vender una casa en Lepe y le llamamos a este mismo número, ¿es correcto?"
2. Si corrige algo, apúntalo bien y vuelve a confirmarlo.
3. Cuando diga que sí, llama a la función guardar_datos. En "motivo" usa siempre una de las opciones de la lista.
4. Después despídete SIEMPRE con esta frase exacta y nada más: "${DESPEDIDA}"

NORMAS:
- No des precios, ni condiciones de hipoteca, ni plazos, ni prometas nada. Si pregunta, di que un compañero se lo explicará.
- No inventes viviendas ni datos. No pidas DNI, cuentas ni datos bancarios.
- Si no quiere dar algún dato, no insistas.
- Si es una llamada comercial, de publicidad o se ha equivocado, despídete con educación.
- La llamada no debe pasar de unos dos o tres minutos.
`.trim();

const HERRAMIENTAS = [{
  type: 'function',
  name: 'guardar_datos',
  description: 'Guarda los datos del cliente para que un compañero le devuelva la llamada. Llamar una sola vez y SOLO después de que el cliente haya confirmado que los datos son correctos.',
  parameters: {
    type: 'object',
    properties: {
      nombre: { type: 'string', description: 'Nombre del cliente, si lo ha dicho.' },
      motivo: { type: 'string', enum: ['Comprar', 'Vender', 'Alquilar (busca)', 'Alquilar (propietario)', 'Hipoteca o préstamo', 'Otro'], description: 'Qué necesita.' },
      zona: { type: 'string', description: 'Pueblo o zona que le interesa o donde está su vivienda.' },
      detalles: { type: 'string', description: 'Resumen breve en una o dos frases: qué busca, presupuesto, plazos, etc.' },
      telefono_contacto: { type: 'string', description: 'Solo si quiere que le llamen a un número distinto del que llama. Si no, vacío.' },
      urgente: { type: 'boolean', description: 'true si el cliente dice que es urgente.' }
    },
    required: ['motivo', 'detalles']
  }
}];

/* ------------------------------------------------------------------ */
/* Utilidades                                                           */
/* ------------------------------------------------------------------ */
function log(...a) { console.log(new Date().toISOString(), ...a); }

/** Verifica la firma del webhook de OpenAI (formato "Standard Webhooks"). */
function verificarFirma(headers, body) {
  if (!CFG.webhookSecret) return false;
  const id = headers['webhook-id'], ts = headers['webhook-timestamp'], firma = headers['webhook-signature'];
  if (!id || !ts || !firma) return false;
  if (Math.abs(Date.now() / 1000 - Number(ts)) > 300) return false;           // más de 5 minutos: se rechaza
  const secreto = Buffer.from(CFG.webhookSecret.replace(/^whsec_/, ''), 'base64');
  const esperado = crypto.createHmac('sha256', secreto).update(`${id}.${ts}.${body}`).digest('base64');
  return String(firma).split(' ').some(p => {
    const v = p.split(',')[1] || '';
    const a = Buffer.from(v), b = Buffer.from(esperado);
    return a.length === b.length && crypto.timingSafeEqual(a, b);
  });
}

/** Saca el número de teléfono de las cabeceras SIP (From, P-Asserted-Identity, Diversion). */
function telefonoLlamante(sipHeaders) {
  const lista = Array.isArray(sipHeaders) ? sipHeaders : [];
  const buscar = (n) => (lista.find(h => String(h.name).toLowerCase() === n) || {}).value || '';
  for (const n of ['from', 'p-asserted-identity', 'remote-party-id']) {
    const m = String(buscar(n)).match(/(?:sip|tel):\+?(\d{6,15})/i);
    if (m) return '+' + m[1].replace(/^00/, '');
  }
  return '';
}

async function openai(path, body) {
  const r = await fetch(OPENAI + path, {
    method: 'POST',
    headers: { Authorization: `Bearer ${CFG.apiKey}`, 'Content-Type': 'application/json' },
    body: body ? JSON.stringify(body) : undefined
  });
  if (!r.ok) throw new Error(`${path} -> ${r.status} ${await r.text()}`);
  return r;
}

async function enviarAn8n(datos) {
  if (!CFG.n8nUrl) return log('Sin N8N_WEBHOOK_URL: no se envía', datos);
  try {
    const r = await fetch(CFG.n8nUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-gds-token': CFG.n8nToken },
      body: JSON.stringify(datos)
    });
    log('n8n', r.status, datos.call_id);
  } catch (e) { log('ERROR n8n', e.message); }
}

/* ------------------------------------------------------------------ */
/* Una llamada                                                          */
/* ------------------------------------------------------------------ */
const llamadas = new Map();   // call_id -> estado (evita aceptar dos veces la misma)

async function atenderLlamada(callId, sipHeaders) {
  if (llamadas.has(callId)) return;
  const estado = { inicio: Date.now(), telefono: telefonoLlamante(sipHeaders), datos: null, enviado: false, transcripcion: [], colgando: false };
  llamadas.set(callId, estado);
  log('Llamada entrante', callId, estado.telefono || '(sin número)');

  // 1) Aceptar la llamada con las instrucciones, la voz y la herramienta
  try {
    await openai(`/realtime/calls/${encodeURIComponent(callId)}/accept`, {
      type: 'realtime',
      model: CFG.model,
      instructions: INSTRUCCIONES,
      audio: {
        input: {
          noise_reduction: { type: 'near_field' },   // filtro de ruido para teléfono pegado a la oreja
          transcription: { model: 'gpt-4o-mini-transcribe', language: 'es', prompt: 'Llamada a una inmobiliaria de Lepe (Huelva). Zonas: Lepe, La Antilla, Islantilla, Urbasur, El Terrón, Cartaya, El Rompido, Isla Cristina, Ayamonte, La Redondela, Huelva.' },
          turn_detection: { type: 'semantic_vad', eagerness: 'low' }   // espera a que la persona termine de hablar
        },
        output: { voice: CFG.voice }
      },
      tools: HERRAMIENTAS,
      tool_choice: 'auto'
    });
  } catch (e) {
    log('ERROR al aceptar', e.message);
    llamadas.delete(callId);
    return;
  }

  const colgar = async (motivo) => {
    if (estado.colgando) return;
    estado.colgando = true;
    log('Colgando', callId, motivo);
    try { await openai(`/realtime/calls/${encodeURIComponent(callId)}/hangup`); } catch (e) { log('Aviso al colgar', e.message); }
  };

  const terminar = () => {
    if (estado.enviado) return;
    estado.enviado = true;
    clearTimeout(estado.tope);
    const d = estado.datos || {};
    const texto = estado.transcripcion.join(' | ');
    enviarAn8n({
      call_id: callId,
      fecha: new Date(estado.inicio).toISOString(),
      telefono: d.telefono_contacto || estado.telefono,
      nombre: d.nombre || '',
      motivo: d.motivo || (estado.datos ? '' : 'Colgó antes de terminar'),
      zona: d.zona || '',
      detalles: (d.detalles || '') + (d.telefono_contacto ? ` (llama desde ${estado.telefono || 'número oculto'})` : '') +
        (!estado.datos && texto ? `Lo que dijo: ${texto.slice(0, 700)}` : ''),
      urgente: !!d.urgente,
      duracion_seg: Math.round((Date.now() - estado.inicio) / 1000)
    });
    setTimeout(() => llamadas.delete(callId), 60000);
  };

  // 2) Canal para "escuchar" la conversación
  const ws = new WebSocket(`wss://api.openai.com/v1/realtime?call_id=${encodeURIComponent(callId)}`, {
    headers: { Authorization: `Bearer ${CFG.apiKey}` }
  });

  // Tope de duración: pide despedirse y cuelga
  estado.tope = setTimeout(() => {
    log('Tope de duración', callId);
    try {
      ws.send(JSON.stringify({ type: 'response.create', response: { instructions: 'Se ha acabado el tiempo de la llamada. Llama ahora a la función guardar_datos con todo lo que sepas del cliente, sin decir nada.' } }));
    } catch (e) {}
    setTimeout(() => colgar('tope'), 25000);
  }, CFG.maxSeconds * 1000);

  ws.on('open', () => {
    // Que el asistente hable primero (saludo)
    ws.send(JSON.stringify({ type: 'response.create' }));
  });

  ws.on('message', (raw) => {
    let ev; try { ev = JSON.parse(raw); } catch (e) { return; }
    switch (ev.type) {
      case 'conversation.item.input_audio_transcription.completed':
        if (ev.transcript) estado.transcripcion.push('Cliente: ' + ev.transcript.trim());
        break;
      case 'response.output_audio_transcript.done':
        if (ev.transcript) estado.transcripcion.push('Asistente: ' + ev.transcript.trim());
        break;
      case 'response.done': {
        const salida = (ev.response && ev.response.output) || [];
        const llamadaFn = salida.find(o => o.type === 'function_call' && o.name === 'guardar_datos');
        if (llamadaFn) {
          try { estado.datos = JSON.parse(llamadaFn.arguments || '{}'); } catch (e) { estado.datos = { detalles: llamadaFn.arguments }; }
          log('Datos guardados', callId, estado.datos.motivo);
          terminar();   // los datos se mandan ya, aunque luego cuelgue a mitad de la despedida
          ws.send(JSON.stringify({ type: 'conversation.item.create', item: { type: 'function_call_output', call_id: llamadaFn.call_id, output: '{"ok":true}' } }));
          ws.send(JSON.stringify({ type: 'response.create', response: { instructions: `Di exactamente esta frase, con tono amable, y nada más: "${DESPEDIDA}"` } }));
          estado.esperandoDespedida = true;
        } else if (estado.esperandoDespedida) {
          // Ha terminado de generar la despedida: se deja tiempo a que suene y se cuelga
          const texto = salida.map(o => (o.content || []).map(c => c.transcript || '').join(' ')).join(' ');
          const espera = Math.min(12000, 2500 + texto.length * 70);
          setTimeout(() => colgar('despedida'), espera);
        }
        break;
      }
      case 'error':
        log('Error de OpenAI', callId, JSON.stringify(ev.error || ev));
        break;
    }
  });

  ws.on('close', () => { log('Fin de llamada', callId); terminar(); });
  ws.on('error', (e) => log('ERROR WebSocket', callId, e.message));
}

/* ------------------------------------------------------------------ */
/* Servidor web                                                         */
/* ------------------------------------------------------------------ */
const server = http.createServer((req, res) => {
  if (req.method === 'GET' && (req.url === '/' || req.url === '/salud')) {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ ok: true, servicio: 'asistente-llamadas', clave: !!CFG.apiKey, webhook: !!CFG.webhookSecret, n8n: !!CFG.n8nUrl, llamadasActivas: llamadas.size }));
  }
  if (req.method === 'POST' && req.url === '/openai-webhook') {
    let body = '';
    req.on('data', (c) => { body += c; if (body.length > 1e6) req.destroy(); });
    req.on('end', () => {
      if (!verificarFirma(req.headers, body)) {
        log('Webhook con firma no válida: rechazado');
        res.writeHead(401); return res.end('firma no válida');
      }
      res.writeHead(200); res.end('ok');   // OpenAI necesita respuesta rápida
      let ev; try { ev = JSON.parse(body); } catch (e) { return; }
      log('Evento', ev.type);
      if (ev.type === 'realtime.call.incoming' && ev.data && ev.data.call_id) {
        atenderLlamada(ev.data.call_id, ev.data.sip_headers).catch(e => log('ERROR', e.message));
      }
    });
    return;
  }
  res.writeHead(404); res.end('no encontrado');
});

server.listen(CFG.port, () => {
  log(`Asistente de llamadas escuchando en el puerto ${CFG.port} (modelo ${CFG.model}, voz ${CFG.voice})`);
  if (!CFG.apiKey) log('AVISO: falta OPENAI_API_KEY');
  if (!CFG.webhookSecret) log('AVISO: falta OPENAI_WEBHOOK_SECRET');
  if (!CFG.n8nUrl) log('AVISO: falta N8N_WEBHOOK_URL');
});
