# Asistente de llamadas · Gestión del Sur

Contesta las llamadas que el equipo no coge, toma nota (nombre, qué necesita, zona, detalles) y avisa por Telegram y en la hoja «Llamadas asistente».

**Recorrido:** cliente llama al 621 → no se coge → Vodafone desvía al número virtual (Telnyx) → OpenAI → este programa acepta la llamada y la escucha → al terminar manda los datos a n8n → hoja + Telegram.

## Variables (en Easypanel → Entorno)

| Variable | Qué es |
|---|---|
| `OPENAI_API_KEY` | Clave del proyecto «Gestión del Sur - llamadas» de OpenAI |
| `OPENAI_WEBHOOK_SECRET` | El secreto `whsec_...` del webhook de OpenAI |
| `N8N_WEBHOOK_URL` | `https://engranaje-sur-n8n.ul39e5.easypanel.host/webhook/gds-llamadas` |
| `N8N_TOKEN` | El token compartido con el flujo de n8n |
| `MODEL` | Opcional. Por defecto `gpt-realtime-2.1-mini` |
| `VOICE` | Opcional. Por defecto `marin` |
| `MAX_SECONDS` | Opcional. Duración máxima por llamada. Por defecto `240` |

Las claves **nunca** van en el código ni en GitHub.

## Direcciones

- `GET /salud` → comprueba que está vivo y qué variables tiene puestas (no enseña las claves).
- `POST /openai-webhook` → aquí avisa OpenAI de cada llamada (firma verificada).

## En OpenAI

- Proyecto «Gestión del Sur - llamadas», límite de 15 $/mes.
- Webhook → `https://<dominio de este servicio>/openai-webhook`, evento `realtime.call.incoming`.

## En Telnyx

- Conexión SIP de tipo FQDN a `sip.api.openai.com`, puerto 5061, TLS y SRTP obligatorio.
- Número +34 959 74 02 20 asignado a esa conexión, con «Translated Number» = ID del proyecto (`proj_...`).
